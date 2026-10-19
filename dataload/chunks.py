"""Work out the chunks of each chunked table from Oracle and write them to the table's chunk file.

    python3 -m dataload.chunks [--config dataload.toml] check [--table NAME ...]
    python3 -m dataload.chunks [--config dataload.toml] make [--chunks 100] [--table NAME ...] [--yes]

A table is cut the way the DBA's PL/SQL cuts it: each chunk ends at the highest of the next N IDs, so
chunks hold the same number of rows however the IDs are spread. N is the row count divided by the
number of chunks wanted, rounded up, so there are never more chunks than asked for (at most 120).
Lines are written as the DBA's block and the old files have them, "ID BETWEEN low AND high": the first
starts at the table's lowest ID, each next one at the previous end plus one (IDs must be whole numbers),
and the last ends at the largest value the column can hold, so every row is in exactly one chunk.

A DATE column is cut at whole days instead, from one count of rows per day: each chunk takes days until
it holds at least N rows (one busy day can make a chunk larger). DATE values carry a time of day, so
BETWEEN would leave out the last day's rows after midnight; these lines are half-open instead,
"COL >= DATE 'first day' AND COL < DATE 'day after'", which Oracle and PostgreSQL both read the same way.
The first starts at the earliest day, each next one where the previous stops, and the last stops at
9999-12-31.
The old chunk file is kept beside the new one as <name>.bak-<date>-<time>.
"""
import argparse
import math
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime

from . import config, plan
from .cli import run_is_active
from .conf import OraConf
from .state import State

MAX_CHUNKS = 120        # the DBA's limit
DEFAULT_CHUNKS = 100
BIGINT_MAX = 2 ** 63 - 1  # last chunk's end when the column has no precision
DATE_CEILING = "9999-12-31"  # last chunk's end for a DATE column (the line says "< DATE '9999-12-31'")

# Reads "id<TAB>sql" lines and answers each with "id<TAB>ROW<TAB>value..." per row, then "id<TAB>END",
# or "id<TAB>ERROR<TAB>why". One session serves every query of a table.
# It uses the same Perl driver ora2pg uses, so it works wherever ora2pg can reach Oracle.
ORACLE_QUERY_PL = r"""
use strict;
use DBI;
$| = 1;
sub clean { my $why = shift || "unknown error"; $why =~ s/\s+/ /g; return $why; }
my $dbh = DBI->connect($ENV{DL_ORA_DSN}, $ENV{DL_ORA_USER}, $ENV{DL_ORA_PWD},
                       {RaiseError => 0, PrintError => 0, AutoCommit => 1});
if (!$dbh) {
    print "FATAL\t" . clean($DBI::errstr) . "\n";
    exit 2;
}
while (my $line = <STDIN>) {
    chomp $line;
    my ($id, $sql) = split /\t/, $line, 2;
    next unless defined $sql;
    my $rows = $dbh->selectall_arrayref($sql);
    if (!$rows) {
        print "$id\tERROR\t" . clean($dbh->errstr) . "\n";
        next;
    }
    for my $row (@$rows) {
        print join("\t", $id, "ROW", map { defined $_ ? $_ : "" } @$row), "\n";
    }
    print "$id\tEND\n";
}
$dbh->disconnect;
"""

# The column a chunk condition is about: "FTS_ITEM_NO BETWEEN 1 AND 2" -> FTS_ITEM_NO
CONDITION_COLUMN_RE = re.compile(r'^\s*"?([A-Za-z][A-Za-z0-9_$#]*)"?\s*(?:BETWEEN\b|IS\b|IN\b|[<>=!])', re.I)


class ChunkError(Exception):
    pass


def number(text):
    return f"{int(text):,}" if text and text.lstrip("-").isdigit() else (text or "?")


def human(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{seconds % 3600 // 60:02d}m"


def conditions(column, lowest, highs, ceiling):
    """Chunk conditions from the table's lowest ID and the highest ID of each chunk. Each chunk starts
    one above the previous end, and the last ends at the ceiling instead of today's highest ID."""
    starts = [lowest] + [high + 1 for high in highs[:-1]]
    ends = highs[:-1] + [ceiling]
    return [f"{column} BETWEEN {a} AND {b}" for a, b in zip(starts, ends)]


def date_conditions(column, first_day, starts, ceiling=DATE_CEILING):
    """Chunk conditions for a DATE column from the earliest day and the first day of every later chunk
    (YYYY-MM-DD). Each chunk stops where the next starts, and the last stops at the ceiling."""
    begins = [first_day] + list(starts)
    ends = list(starts) + [ceiling]
    return [f"{column} >= DATE '{a}' AND {column} < DATE '{b}'" for a, b in zip(begins, ends)]


def day_starts(days, size):
    """Where chunks begin after the first, from [(day, rows)] in day order: a chunk closes as soon as it
    holds at least size rows, so there are never more than ceil(total / size) chunks."""
    starts, held = [], 0
    for day, rows in days:
        if held >= size:
            starts.append(day)
            held = 0
        held += rows
    return starts


def ceiling_for(precision):
    """The largest whole number a NUMBER(precision) column can hold."""
    return 10 ** int(precision) - 1 if precision else BIGINT_MAX


@dataclass
class Table:
    step: plan.Step
    owner: str              # Oracle owner, upper case
    name: str               # upper case
    column: str = ""
    column_from: str = ""
    current: list = field(default_factory=list)
    problems: list = field(default_factory=list)
    info: dict = field(default_factory=dict)

    @property
    def label(self):
        return f"{self.owner}.{self.name}"

    @property
    def sql_name(self):
        return f'"{self.owner}"."{self.name}"'

    @property
    def sql_column(self):
        return f'"{self.column.upper()}"'


class Oracle:
    """One Oracle session for one table, kept open between queries."""

    def __init__(self, cmd, conf):
        env = {**os.environ, "DL_ORA_DSN": conf.get("ORACLE_DSN"), "DL_ORA_USER": conf.get("ORACLE_USER"),
               "DL_ORA_PWD": conf.get("ORACLE_PWD")}
        if conf.get("ORACLE_HOME"):
            env.setdefault("ORACLE_HOME", conf.get("ORACLE_HOME"))
        self.child = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                      text=True, env=env)
        self.asked = 0

    def rows(self, sql):
        self.asked += 1
        ident = str(self.asked)
        noise = []
        try:
            self.child.stdin.write(f"{ident}\t{' '.join(sql.split())}\n")
            self.child.stdin.flush()
        except (BrokenPipeError, OSError):
            pass
        found = []
        for line in self.child.stdout:
            parts = line.rstrip("\n").split("\t")
            if parts[0] == "FATAL":
                raise ChunkError("cannot connect to Oracle: " + (parts[1] if len(parts) > 1 else "unknown error"))
            if parts[0] != ident or len(parts) < 2:
                noise.append(line.strip())
                continue
            if parts[1] == "ROW":
                found.append(parts[2:])
            elif parts[1] == "ERROR":
                raise ChunkError(parts[2] if len(parts) > 2 else "the query failed")
            elif parts[1] == "END":
                return found
        raise ChunkError("the Oracle helper stopped" + (": " + " ".join(noise)[:300] if noise else ""))

    def close(self):
        try:
            self.child.stdin.close()
            self.child.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            self.child.kill()
            self.child.wait()
        self.child.stdout.close()


class Chunker:
    def __init__(self, cfg, out=print):
        self.cfg = cfg
        self.print_lock = threading.Lock()
        self.stopping = False
        self.sessions = []
        cfg.logs.mkdir(parents=True, exist_ok=True)
        self.log_file = cfg.logs / "chunks.log"
        self._out = out
        if cfg.oracle_query:
            self.cmd = [cfg.oracle_query]
        else:
            cfg.work.mkdir(parents=True, exist_ok=True)
            script = cfg.work / "oracle_query.pl"
            script.write_text(ORACLE_QUERY_PL)
            self.cmd = ["perl", str(script)]

    def out(self, text=""):
        with self.print_lock:
            self._out(text)
            with open(self.log_file, "a") as f:
                f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} {text}\n" if text else "\n")

    # ---------- finding the tables ----------

    def tables(self, names, columns):
        steps = [s for s in plan.load_steps(self.cfg) if s.kind == "chunk"]
        if not steps:
            raise ChunkError(f"no chunked tables (folders with a run_*.sh) under {self.cfg.content}")
        wanted = {n.upper() for n in names}
        if wanted:
            known = {s.table.upper() for s in steps} | {s.id.upper() for s in steps}
            unknown = sorted(wanted - known)
            if unknown:
                raise ChunkError(f"not a chunked table: {', '.join(unknown)} "
                                 f"(chunked tables: {', '.join(s.table for s in steps)})")
            steps = [s for s in steps if s.table.upper() in wanted or s.id.upper() in wanted]
        found = []
        for s in steps:
            t = Table(s, OraConf(s.conf_path).get("SCHEMA").upper(), s.table.upper(), current=list(s.units))
            if not t.owner:
                t.problems.append(f"{s.conf_path.name} has no SCHEMA, so the Oracle owner is unknown")
            if t.name in columns:
                t.column, t.column_from = columns[t.name], "--column"
            else:
                named = {m.group(1).upper() for m in map(CONDITION_COLUMN_RE.match, t.current) if m}
                if len(named) == 1:
                    t.column, t.column_from = named.pop(), s.chunks_file.name
                elif len(named) > 1:
                    t.problems.append(f"{s.chunks_file.name} names more than one column ({', '.join(sorted(named))}); "
                                      f"say which with --column {t.name}=COLUMN")
            found.append(t)
        return found

    # ---------- reading Oracle ----------

    def session(self, t):
        ora = Oracle(self.cmd, OraConf(t.step.conf_path))
        self.sessions.append(ora)
        return ora

    def inspect(self, t, ora):
        """Fills t.info and t.problems from Oracle's dictionary. Cheap: no table data is read."""
        if not t.column:
            rows = ora.rows(f"""
                SELECT MIN(cc.column_name), TO_CHAR(COUNT(1), 'TM9')
                FROM all_constraints k
                JOIN all_cons_columns cc ON cc.owner = k.owner AND cc.constraint_name = k.constraint_name
                WHERE k.owner = '{t.owner}' AND k.table_name = '{t.name}' AND k.constraint_type = 'P'""")
            if rows and rows[0][1] == "1":
                t.column, t.column_from = rows[0][0], "primary key"
            else:
                t.problems.append(f"no chunk file to take the ID column from and no one-column primary key; "
                                  f"say which with --column {t.name}=COLUMN")
                return
        rows = ora.rows(f"""
            SELECT c.data_type, c.nullable,
                   (SELECT MIN(ic.index_name) FROM all_ind_columns ic
                     WHERE ic.table_owner = c.owner AND ic.table_name = c.table_name
                       AND ic.column_name = c.column_name AND ic.column_position = 1),
                   TO_CHAR(t.num_rows, 'TM9'), TO_CHAR(t.last_analyzed, 'YYYY-MM-DD'),
                   TO_CHAR(c.data_precision), TO_CHAR(c.data_scale)
            FROM all_tab_columns c
            JOIN all_tables t ON t.owner = c.owner AND t.table_name = c.table_name
            WHERE c.owner = '{t.owner}' AND c.table_name = '{t.name}' AND c.column_name = '{t.column.upper()}'""")
        if not rows:
            t.problems.append(f"Oracle has no column {t.column} in {t.label} (or this login cannot see it)")
            return
        data_type, nullable, index, num_rows, analyzed, precision, scale = (rows[0] + [""] * 7)[:7]
        t.info = {"type": data_type, "nullable": nullable, "index": index, "num_rows": num_rows,
                  "analyzed": analyzed, "precision": precision, "scale": scale}
        if data_type == "DATE":
            pass
        elif data_type not in ("NUMBER", "INTEGER"):
            t.problems.append(f"{t.column} is {data_type}, neither a whole number nor a DATE")
        elif scale not in ("", "0"):
            t.problems.append(f"{t.column} is NUMBER({precision},{scale}) and can hold decimals; "
                              "BETWEEN chunks need whole numbers")
        if not index:
            t.problems.append(f"no index starts with {t.column}: every chunk would read the whole table. "
                              "Ask the DBA before going on")

    def cut(self, t, ora, want, started):
        """(rows, rows per chunk, lowest ID, highest ID of each chunk)."""
        self.out(f"{t.label}: counting rows")
        # A NUMBER with no scale can hold decimals; then the rows are checked for any.
        fractions = (f"TO_CHAR(COUNT(CASE WHEN {t.sql_column} <> TRUNC({t.sql_column}) THEN 1 END), 'TM9')"
                     if t.info.get("scale") != "0" else "'0'")
        total, nulls, low, high, decimals = (ora.rows(f"""
            SELECT TO_CHAR(COUNT(1), 'TM9'), TO_CHAR(COUNT(1) - COUNT({t.sql_column}), 'TM9'),
                   TO_CHAR(MIN({t.sql_column}), 'TM9'), TO_CHAR(MAX({t.sql_column}), 'TM9'), {fractions}
            FROM {t.sql_name}""")[0] + [""] * 5)[:5]
        total, nulls, decimals = int(total), int(nulls), int(decimals or 0)
        if total == 0:
            raise ChunkError("the table is empty")
        if nulls:
            raise ChunkError(f"{nulls:,} rows have no {t.column} and would be in no chunk")
        if decimals:
            raise ChunkError(f"{decimals:,} rows have a {t.column} with decimals; BETWEEN chunks need whole numbers")
        low, high = int(low), int(high)
        ceiling = ceiling_for(t.info.get("precision"))
        if ceiling < high:
            raise ChunkError(f"highest {t.column} {high} is above {ceiling}")
        t.info.update(lowest=low, highest=high, ceiling=ceiling)
        size = math.ceil(total / want)
        self.out(f"{t.label}: {total:,} rows, {size:,} per chunk, about {math.ceil(total / size)} chunks "
                 f"({human(time.time() - started)})")
        highs, after, op = [], low, ">="
        while not self.stopping:
            hi = ora.rows(f"""
                SELECT TO_CHAR(MAX({t.sql_column}), 'TM9') FROM (
                    SELECT {t.sql_column} FROM {t.sql_name} WHERE {t.sql_column} {op} {after}
                    ORDER BY {t.sql_column}
                ) WHERE ROWNUM <= {size}""")[0][0]
            if not hi:
                break
            highs.append(int(hi))
            if len(highs) > MAX_CHUNKS:
                raise ChunkError(f"more than {MAX_CHUNKS} chunks; nothing written")
            self.out(f"{t.label}: chunk {len(highs)} ends at {t.column} {hi} ({human(time.time() - started)})")
            if int(hi) >= high:
                break
            after, op = hi, ">"
        self.out(f"{t.label}: lowest {t.column} {low:,}, highest {high:,}; the last chunk ends at {ceiling}")
        return total, size, low, highs

    def cut_by_day(self, t, ora, want, started):
        """(rows, rows per chunk, chunk lines) for a DATE column, from one count of rows per day.
        The query can be answered from the index alone, since the column leads it."""
        self.out(f"{t.label}: counting rows per day of {t.column}")
        hint = f'/*+ INDEX_FFS(x "{t.info["index"]}") */ ' if t.info.get("index") else ""
        rows = ora.rows(f"""
            SELECT {hint}TO_CHAR(TRUNC(x.{t.sql_column}), 'YYYY-MM-DD'), TO_CHAR(COUNT(1), 'TM9')
            FROM {t.sql_name} x GROUP BY TRUNC(x.{t.sql_column}) ORDER BY 1""")
        days = [(day, int(n)) for day, n in rows if day]
        nulls = sum(int(n) for day, n in rows if not day)
        total = sum(n for _, n in days) + nulls
        if total == 0:
            raise ChunkError("the table is empty")
        if nulls:
            raise ChunkError(f"{nulls:,} rows have no {t.column} and would be in no chunk")
        first, last = days[0][0], days[-1][0]
        if last >= DATE_CEILING:
            raise ChunkError(f"latest {t.column} is {last}, on or after {DATE_CEILING} where the last chunk stops")
        t.info.update(lowest=first, highest=last, ceiling=DATE_CEILING)
        size = math.ceil(total / want)
        starts = day_starts(days, size)
        if len(starts) + 1 > MAX_CHUNKS:
            raise ChunkError(f"more than {MAX_CHUNKS} chunks; nothing written")
        lines = date_conditions(t.column, first, starts)
        sizes, n = [], 0
        bounds = set(starts)
        for day, count in days:
            if day in bounds:
                sizes.append(n)
                n = 0
            n += count
        sizes.append(n)
        self.out(f"{t.label}: {total:,} rows over {len(days):,} days, {size:,} per chunk wanted, {len(lines)} chunks, "
                 f"largest {max(sizes):,} rows ({human(time.time() - started)})")
        self.out(f"{t.label}: earliest {t.column} {first}, latest {last}; the last chunk stops at {DATE_CEILING}")
        return total, size, lines

    # ---------- writing ----------

    def write(self, t, lines, stamp):
        path = t.step.chunks_file
        new = path.with_name(path.name + ".new")
        new.write_text("\n".join(lines) + "\n")
        backup = None
        if path.exists():
            backup = path.with_name(f"{path.name}.bak-{stamp}")
            shutil.copy2(path, backup)
        os.replace(new, path)
        return backup

    # ---------- commands ----------

    def describe(self, t, want=None):
        self.out(f"{t.step.id}")
        self.out(f"  table {t.label}, ID column {t.column or '?'}"
                 + (f" (from {t.column_from})" if t.column_from else ""))
        if t.info:
            i = t.info
            rows = (f"about {number(i['num_rows'])} rows (statistics of {i['analyzed']})"
                    if i["num_rows"] else "no statistics")
            kind = i["type"] + (f"({i['precision']}" + (f",{i['scale']}" if i["scale"] not in ("", "0") else "") + ")"
                                if i["precision"] else "")
            self.out(f"  Oracle: {kind}, {'not null' if i['nullable'] == 'N' else 'can be null'}, "
                     f"index {i['index'] or 'NONE'}, {rows}")
            if i["type"] in ("NUMBER", "INTEGER") and i["scale"] in ("", "0"):
                self.out(f"  the last chunk will end at {ceiling_for(i['precision'])}")
            elif i["type"] == "DATE":
                self.out(f"  chunks are whole days, the last one stops at {DATE_CEILING}")
            if want and i["num_rows"].isdigit() and int(i["num_rows"]):
                per = math.ceil(int(i["num_rows"]) / want)
                self.out(f"  with --chunks {want}: about {per:,} rows per chunk")
        self.out(f"  {t.step.chunks_file.name} now: "
                 + (f"{len(t.current)} chunks" if t.current else "missing or empty"))
        for p in t.problems:
            self.out(f"  PROBLEM: {p}")

    def check(self, names=(), columns=None, want=DEFAULT_CHUNKS):
        tables = self.tables(names, columns or {})
        for t in tables:
            if not t.problems:
                ora = self.session(t)
                try:
                    self.inspect(t, ora)
                except ChunkError as e:
                    t.problems.append(str(e))
                finally:
                    ora.close()
            self.describe(t, want)
            self.out()
        bad = [t for t in tables if t.problems]
        if bad:
            self.out(f"{len(bad)} of {len(tables)} table(s) need attention before 'make'.")
            return 1
        self.out(f"All {len(tables)} table(s) ready. 'make' reads every ID of each table once, so run it when "
                 "the DBA says (after applications are stopped and Oracle users are locked).")
        return 0

    def make(self, want, names=(), columns=None, yes=False, confirm=input, workers=0):
        if not 1 <= want <= MAX_CHUNKS:
            raise ChunkError(f"--chunks must be between 1 and {MAX_CHUNKS}")
        if run_is_active(self.cfg):
            raise ChunkError("a dataload run is in progress; chunk files cannot change under it")
        tables = self.tables(names, columns or {})
        for t in tables:
            if not t.problems:
                ora = self.session(t)
                try:
                    self.inspect(t, ora)
                except ChunkError as e:
                    t.problems.append(str(e))
                finally:
                    ora.close()
        bad = [t for t in tables if t.problems]
        if bad:
            for t in bad:
                self.describe(t)
            raise ChunkError(f"{len(bad)} table(s) cannot be chunked; nothing was read or written")

        self.out(f"Tables: {', '.join(t.label for t in tables)}; up to {want} chunks each.")
        if not yes:
            answer = confirm("Applications stopped and Oracle users locked, and the DBA agrees to read every ID "
                             "of these tables now? [y/N] ")
            if answer.strip().lower() not in ("y", "yes"):
                self.out("Nothing done.")
                return 1

        stamp = f"{datetime.now():%Y%m%d-%H%M%S}"
        results, failures = {}, {}

        def one(t):
            started = time.time()
            ora = self.session(t)
            try:
                if t.info.get("type") == "DATE":
                    total, size, lines = self.cut_by_day(t, ora, want, started)
                else:
                    total, size, low, highs = self.cut(t, ora, want, started)
                    lines = conditions(t.column, low, highs, t.info["ceiling"])
            finally:
                ora.close()
            if self.stopping:
                raise ChunkError("stopped")
            backup = self.write(t, lines, stamp)
            self.out(f"{t.label}: {len(lines)} chunks written to {t.step.chunks_file}"
                     + (f", old file kept as {backup.name}" if backup else "") + f" ({human(time.time() - started)})")
            return total, size, len(lines), backup

        pool = ThreadPoolExecutor(max_workers=workers or len(tables))
        futures = {pool.submit(one, t): t for t in tables}
        try:
            for f in as_completed(futures):
                t = futures[f]
                try:
                    results[t.label] = f.result()
                except Exception as e:   # one table's trouble must not stop the others
                    failures[t.label] = str(e) or type(e).__name__
                    self.out(f"{t.label}: FAILED: {e}; its chunk file is left as it was")
        except KeyboardInterrupt:
            self.stopping = True
            for ora in self.sessions:
                ora.child.kill()
            pool.shutdown(wait=True, cancel_futures=True)
            self.out("Stopped. Tables finished before the stop have their new chunk files; the rest are unchanged.")
            return 130
        pool.shutdown()

        self.out()
        self.out(f"{'table':32} {'rows':>15} {'rows/chunk':>12} {'chunks':>6}")
        for t in tables:
            if t.label in results:
                total, size, count, _ = results[t.label]
                self.out(f"{t.label:32} {total:>15,} {size:>12,} {count:>6}")
            else:
                self.out(f"{t.label:32} FAILED: {failures.get(t.label, '')}")
        if results and self.cfg.state.exists() and State(self.cfg.state).run_info() is not None:
            self.out("\nA run is recorded in the state file. Start the load with 'run --new': "
                     "'resume' refuses chunk files that changed since its run started.")
        return 1 if failures else 0


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python3 -m dataload.chunks",
                                     description="Work out the chunks of the chunked tables from Oracle "
                                                 "and write their chunk files")
    parser.add_argument("--config", default="dataload.toml")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, text in (("check", "show each chunked table, its ID column and index; reads no table data"),
                       ("make", "work out the chunks and write the chunk files (the old ones are kept)")):
        p = sub.add_parser(name, help=text)
        p.add_argument("--table", action="append", default=[], metavar="NAME",
                       help="only this table (or step name, as 'plan' shows it); can be given more than once")
        p.add_argument("--column", action="append", default=[], metavar="TABLE=COLUMN",
                       help="the column to cut a table by (a whole-number ID or a DATE), when its chunk file does not say")
        p.add_argument("--chunks", type=int, default=DEFAULT_CHUNKS,
                       help=f"chunks wanted per table, at most {MAX_CHUNKS} (default {DEFAULT_CHUNKS})")
        if name == "make":
            p.add_argument("--yes", action="store_true", help="do not ask before reading Oracle")
            p.add_argument("--workers", type=int, default=0,
                           help="tables worked on at the same time, one Oracle session each (default: all)")
    args = parser.parse_args(argv)

    try:
        os.getcwd()
    except FileNotFoundError:
        print("The folder this shell is in no longer exists (it was deleted or rebuilt). "
              "Change into it again, for example: cd \"$PWD\"")
        return 2
    if not os.path.isfile(args.config):
        print(f"Settings file not found: {os.path.abspath(args.config)}\n"
              "Run this from the folder that holds dataload.toml, or pass --config PATH before the command.")
        return 2
    columns = {}
    for item in args.column:
        table, sep, column = item.partition("=")
        if not sep or not table.strip() or not column.strip():
            print(f"--column must look like TABLE=COLUMN, not {item!r}")
            return 2
        columns[table.strip().upper()] = column.strip().upper()
    if not 1 <= args.chunks <= MAX_CHUNKS:
        print(f"--chunks must be between 1 and {MAX_CHUNKS}")
        return 2
    cfg = config.load(args.config)
    try:
        chunker = Chunker(cfg)
        if args.command == "check":
            return chunker.check(args.table, columns, args.chunks)
        return chunker.make(args.chunks, args.table, columns, yes=args.yes, workers=args.workers)
    except (ChunkError, plan.PlanError) as e:
        print(f"error: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
