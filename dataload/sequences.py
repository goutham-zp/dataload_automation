"""Set PostgreSQL's sequences from Oracle's after the data load.

    python3 -m dataload.sequences [--config dataload.toml] plan | apply [--yes] | check | undo CSV [--yes]

Named sequences are matched by schema and name; identity columns by table and column, because their
sequence names never match (ISEQ$$_645901 on Oracle, company_offer_company_offer_id_seq on PostgreSQL).
Oracle's value is copied. A sequence only on PostgreSQL is set to the highest value in the columns it feeds
plus one. A sequence already at or past its value is left alone, so apply never moves anything back and
running again is always safe. Sequences only on Oracle are reported and not touched. undo puts the
sequences an apply set back where they were, unless they were used since.
"""
import argparse
import csv
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime

from . import config, plan
from .conf import OraConf

# Runs the SQL read from stdin and prints "ROW<TAB>value<TAB>value..." per row, or "FATAL<TAB>why".
# It uses the same Perl driver ora2pg uses, so it works wherever ora2pg can reach Oracle.
ORACLE_ROWS_PL = r"""
use strict;
use DBI;
$| = 1;
sub fatal { my $why = shift || "unknown error"; $why =~ s/\s+/ /g; print "FATAL\t$why\n"; exit 2; }
my $dbh = DBI->connect($ENV{DL_ORA_DSN}, $ENV{DL_ORA_USER}, $ENV{DL_ORA_PWD},
                       {RaiseError => 0, PrintError => 0, AutoCommit => 1}) or fatal($DBI::errstr);
my $sql = do { local $/; <STDIN> };
my $rows = $dbh->selectall_arrayref($sql) or fatal($dbh->errstr);
for my $row (@$rows) {
    print join("\t", "ROW", map { defined $_ ? $_ : "" } @$row), "\n";
}
$dbh->disconnect;
"""

ORACLE_SQL = """
SELECT s.sequence_owner, s.sequence_name, TO_CHAR(s.last_number, 'TM9'), TO_CHAR(s.increment_by, 'TM9'),
       c.table_name, c.column_name
FROM dba_sequences s
LEFT JOIN dba_tab_identity_cols c
  ON c.owner = s.sequence_owner AND c.sequence_name = s.sequence_name AND c.table_name NOT LIKE 'BIN$%%'
WHERE s.sequence_owner IN (%s)
"""

# pg_get_sequence_data (PostgreSQL 18) gives is_called, which tells whether last_value was handed out already.
PG_SEQUENCES_SQL = """
SELECT n.nspname, c.relname, s.seqincrement, s.seqmin, s.seqmax, s.seqstart, d.last_value, d.is_called,
       has_sequence_privilege(c.oid, 'UPDATE')
FROM pg_sequence s
JOIN pg_class c ON c.oid = s.seqrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL pg_get_sequence_data(c.oid) d
WHERE n.nspname IN (%s)
"""

# Which column each sequence feeds: identity, OWNED BY, or a column default calling nextval.
# Partitions are left out; their parent table is checked instead.
PG_LINKS_SQL = """
SELECT tn.nspname, t.relname, a.attname, sn.nspname, s.relname,
       CASE d.deptype WHEN 'i' THEN 'identity' ELSE 'owned by' END, format_type(a.atttypid, NULL)
FROM pg_depend d
JOIN pg_class s ON s.oid = d.objid AND s.relkind = 'S'
JOIN pg_namespace sn ON sn.oid = s.relnamespace
JOIN pg_class t ON t.oid = d.refobjid AND NOT t.relispartition
JOIN pg_namespace tn ON tn.oid = t.relnamespace
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
WHERE d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i')
  AND tn.nspname IN (%(schemas)s)
UNION
SELECT tn.nspname, t.relname, a.attname, sn.nspname, s.relname, 'default', format_type(a.atttypid, NULL)
FROM pg_attrdef ad
JOIN pg_depend d ON d.classid = 'pg_attrdef'::regclass AND d.objid = ad.oid AND d.refclassid = 'pg_class'::regclass
JOIN pg_class s ON s.oid = d.refobjid AND s.relkind = 'S'
JOIN pg_namespace sn ON sn.oid = s.relnamespace
JOIN pg_class t ON t.oid = ad.adrelid AND NOT t.relispartition
JOIN pg_namespace tn ON tn.oid = t.relnamespace
JOIN pg_attribute a ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
WHERE tn.nspname IN (%(schemas)s)
"""

NUMERIC_TYPES = {"smallint", "integer", "bigint", "numeric"}
PLACEHOLDER_RE = re.compile(r"^-?9{5,}\d?$")   # 999999, 999999999, 9999999991: marker rows typed in by hand
SEP = "\x1f"                                     # psql field separator; never part of a name


class SequenceError(Exception):
    pass


@dataclass
class OracleSeq:
    owner: str          # upper case
    name: str           # upper case
    next_value: int     # LAST_NUMBER: the next value Oracle would write to disk, at or past anything handed out
    increment: int
    table: str = ""     # identity sequences: the column they feed
    column: str = ""

    @property
    def label(self):
        return f"{self.owner}.{self.name}"

    @property
    def identity(self):
        return bool(self.table)


@dataclass
class PgSeq:
    schema: str
    name: str
    increment: int
    min_value: int
    max_value: int
    start: int
    last_value: int
    is_called: bool
    can_update: bool = True

    @property
    def label(self):
        return f"{self.schema}.{self.name}"

    @property
    def next_value(self):
        if self.last_value is None:
            return self.start
        return self.last_value + self.increment if self.is_called else self.last_value


@dataclass
class Link:
    schema: str
    table: str
    column: str
    seq_schema: str
    seq_name: str
    source: str         # identity | owned by | default | links file
    data_type: str = ""

    @property
    def seq_label(self):
        return f"{self.seq_schema}.{self.seq_name}"

    @property
    def column_label(self):
        return f"{self.schema}.{self.table}.{self.column}"


@dataclass
class Row:
    kind: str                       # named | identity | postgresql only
    oracle: str = ""
    feeds: str = ""                 # identity: schema.table.column on PostgreSQL
    pg: str = ""
    oracle_next: int = None
    pg_next: int = None
    pg_after: int = None
    action: str = ""                # set | ahead | refused | oracle only | postgresql only | failed
    reason: str = ""
    moved: str = ""                 # Oracle's value when read again after apply, if it changed
    data_highest: int = None        # postgresql only: highest value in the columns the sequence feeds
    data_next: int = None           # postgresql only: the value it is set to, data_highest + increment
    target: PgSeq = field(default=None, repr=False)

    @property
    def goal(self):
        return self.oracle_next if self.oracle_next is not None else self.data_next


def quote_ident(name):
    return '"' + name.replace('"', '""') + '"'


def quote_literal(text):
    return "'" + text.replace("'", "''") + "'"


def to_int(text):
    return int(text) if text not in ("", None) else None


def ahead(value, other, increment):
    """True when `value` is at or past `other` in the direction the sequence counts."""
    return value >= other if increment > 0 else value <= other


def match(oracle, pg, links, schema_map):
    """One Row per Oracle sequence, then one per PostgreSQL sequence nothing on Oracle matched."""
    pg_by_name = {(s.schema, s.name): s for s in pg}
    by_column = {}
    for link in links:
        if link.source != "links file":
            by_column.setdefault((link.schema, link.table, link.column), []).append(link)
    rows, matched = [], set()
    for o in sorted(oracle, key=lambda s: (s.owner, s.name)):
        schema = schema_map[o.owner]
        if o.identity:
            row = Row("identity", oracle=o.label, oracle_next=o.next_value,
                      feeds=f"{schema}.{o.table.lower()}.{o.column.lower()}")
            found = by_column.get((schema, o.table.lower(), o.column.lower()), [])
            # The column's own sequence first; a column default that calls nextval only when there is none.
            found.sort(key=lambda l: l.source == "default")
            target = pg_by_name.get((found[0].seq_schema, found[0].seq_name)) if found else None
            if target is None:
                row.action, row.reason = "oracle only", "no sequence feeds this column on PostgreSQL"
                usual = f"{o.table.lower()}_{o.column.lower()}_seq"
                if (schema, usual) in pg_by_name:
                    row.reason += f"; {schema}.{usual} exists but is not linked to it"
            elif found[0].source == "default":
                row.reason = "matched through the column default"
        elif o.name.startswith("ISEQ$$_"):
            rows.append(Row("identity", oracle=o.label, oracle_next=o.next_value, action="oracle only",
                            reason="identity sequence whose table was not found (dropped?)"))
            continue
        else:
            row = Row("named", oracle=o.label, oracle_next=o.next_value)
            target = pg_by_name.get((schema, o.name.lower()))
            if target is None:
                row.action, row.reason = "oracle only", "no sequence with this name on PostgreSQL"
        if target is not None:
            row.pg, row.pg_next, row.target = target.label, target.next_value, target
            matched.add(target.label)
            decide(row, o, target)
        rows.append(row)
    shared = {}
    for row in rows:
        if row.target is not None:
            shared.setdefault(row.pg, []).append(row)
    for group in shared.values():
        if len(group) > 1:
            for row in group:
                row.action, row.reason = "refused", "matched by more than one Oracle sequence: " + ", ".join(
                    r.oracle for r in group)
    for s in sorted(pg, key=lambda s: (s.schema, s.name)):
        if s.label not in matched:
            rows.append(Row("postgresql only", pg=s.label, pg_next=s.next_value, action="postgresql only",
                            reason="left alone", target=s))
    return rows


def decide(row, o, target):
    if o.increment != target.increment:
        row.action, row.reason = "refused", f"increment differs: Oracle {o.increment}, PostgreSQL {target.increment}"
    elif not target.min_value <= o.next_value <= target.max_value:
        row.action, row.reason = "refused", (f"Oracle's value is outside PostgreSQL's range "
                                             f"{target.min_value}..{target.max_value}")
    elif ahead(target.next_value, o.next_value, o.increment):
        row.action = "ahead"
        row.reason = "level with Oracle" if target.next_value == o.next_value else "already past Oracle, left as it is"
    elif not target.can_update:
        row.action, row.reason = "refused", "this login may not change the sequence (UPDATE privilege)"
    else:
        row.action = "set"


def decide_from_data(row, highest):
    """A sequence only on PostgreSQL: the next value is the highest value in its columns plus one."""
    seq = row.target
    row.data_highest = highest
    if highest is None:
        row.reason = "its columns are empty; left alone"
        return
    if PLACEHOLDER_RE.match(str(highest)):
        row.reason = f"highest value {highest} is an all-nines marker row; left alone, set it by hand if needed"
        return
    want = highest + seq.increment
    if ahead(seq.next_value, want, seq.increment):
        row.reason = f"already past its data (highest {highest}); left as it is"
    elif not seq.min_value <= want <= seq.max_value:
        row.action, row.reason = "refused", f"highest value + 1 is outside the range {seq.min_value}..{seq.max_value}"
    elif not seq.can_update:
        row.action, row.reason = "refused", "this login may not change the sequence (UPDATE privilege)"
    else:
        row.action, row.data_next = "set", want
        row.reason = f"only on PostgreSQL: set from its data (highest {highest})"


def read_links_file(path, schemas):
    """Extra sequence -> column links for the check, one per line: schema.sequence schema.table.column"""
    links, problems = [], []
    if not path or not path.exists():
        return links, problems
    for number, line in enumerate(path.read_text().splitlines(), 1):
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.lower().split()
        seq, col = (parts[0].split("."), parts[1].split(".")) if len(parts) == 2 else ([], [])
        if len(seq) != 2 or len(col) != 3:
            problems.append(f"{path.name} line {number}: expected 'schema.sequence schema.table.column'")
            continue
        if col[0] in schemas:
            links.append(Link(col[0], col[1], col[2], seq[0], seq[1], "links file"))
    return links, problems


def check_label(highest, next_value, increment):
    if highest is None:
        return "ok", "table is empty"
    if not ahead(highest, next_value, increment):
        return "ok", ""
    if PLACEHOLDER_RE.match(str(highest)):
        return "placeholder", "highest value is an all-nines marker row; expected, same as on Oracle"
    return "needs a look", "a row already holds the sequence's next value or one past it"


class Databases:
    def __init__(self, cfg):
        self.cfg = cfg
        loads = plan.load_steps(cfg)
        if not loads:
            raise SequenceError(f"no load steps found under {cfg.content}")
        confs = [OraConf(s.conf_path) for s in loads if s.conf_path and s.conf_path.exists()]
        self.schema_map = {}
        for conf in confs:
            owner = conf.get("SCHEMA").upper()
            if owner:
                self.schema_map[owner] = conf.pg_schema() or owner.lower()
        targets = sorted({c.target() for c in confs})
        if len(targets) != 1:
            raise SequenceError(f"confs point at different PostgreSQL targets: {', '.join(targets)}")
        self.target = targets[0]
        self.pg_env = confs[0].pg_env()
        first = confs[0]
        self.ora_env = {"DL_ORA_DSN": first.get("ORACLE_DSN"), "DL_ORA_USER": first.get("ORACLE_USER"),
                        "DL_ORA_PWD": first.get("ORACLE_PWD")}
        if first.get("ORACLE_HOME"):
            self.ora_env["ORACLE_HOME"] = first.get("ORACLE_HOME")
        self.source = first.get("ORACLE_DSN").split(":", 2)[-1]
        if cfg.oracle_rows:
            self.oracle_cmd = [cfg.oracle_rows]
        else:
            cfg.work.mkdir(parents=True, exist_ok=True)
            script = cfg.work / "oracle_rows.pl"
            script.write_text(ORACLE_ROWS_PL)
            self.oracle_cmd = ["perl", str(script)]

    @property
    def pg_schemas(self):
        return sorted(set(self.schema_map.values()))

    def oracle(self):
        owners = ", ".join(quote_literal(o) for o in sorted(self.schema_map))
        env = {**os.environ, **self.ora_env}
        r = subprocess.run(self.oracle_cmd, input=ORACLE_SQL % owners, capture_output=True, text=True, env=env)
        found, problem = [], ""
        for line in r.stdout.splitlines():
            parts = line.split("\t")
            if parts[0] == "FATAL":
                problem = parts[1] if len(parts) > 1 else "unknown error"
            elif parts[0] == "ROW":
                owner, name, last, inc, table, column = (parts[1:] + [""] * 6)[:6]
                found.append(OracleSeq(owner, name, int(last), int(inc), table, column))
        if problem or r.returncode != 0:
            raise SequenceError(f"cannot read Oracle's sequences: {problem or r.stderr.strip() or r.returncode}")
        return found

    def psql(self, sql, timeout_seconds=0):
        env = {**os.environ, **self.pg_env}
        if timeout_seconds:
            env["PGOPTIONS"] = f"-c statement_timeout={int(timeout_seconds * 1000)}"
        r = subprocess.run([self.cfg.psql, "-X", "-q", "-A", "-t", "-F", SEP, "-v", "ON_ERROR_STOP=1", "-c", sql],
                           capture_output=True, text=True, env=env)
        return r.returncode, r.stdout.strip("\n"), (r.stderr.strip().splitlines() or [""])[0]

    def rows(self, sql):
        rc, out, err = self.psql(sql)
        if rc != 0:
            raise SequenceError(f"PostgreSQL {self.target}: {err}")
        return [line.split(SEP) for line in out.splitlines() if line]

    def pg_sequences(self):
        schemas = ", ".join(quote_literal(s) for s in self.pg_schemas)
        return [PgSeq(r[0], r[1], int(r[2]), int(r[3]), int(r[4]), int(r[5]), to_int(r[6]), r[7] == "t", r[8] == "t")
                for r in self.rows(PG_SEQUENCES_SQL % schemas)]

    def pg_links(self):
        schemas = ", ".join(quote_literal(s) for s in self.pg_schemas)
        return [Link(*r[:6], data_type=r[6]) for r in self.rows(PG_LINKS_SQL % {"schemas": schemas})]

    @staticmethod
    def current_next(seq):
        name = quote_ident(seq.schema) + "." + quote_ident(seq.name)
        return name, f"(SELECT CASE WHEN is_called THEN last_value + {seq.increment} ELSE last_value END FROM {name})"

    def set_value(self, row):
        """Moves the sequence to its goal, or leaves it where it is if it got there first meanwhile."""
        seq = row.target
        name, current = self.current_next(seq)
        pick = "GREATEST" if seq.increment > 0 else "LEAST"
        rc, out, err = self.psql(f"SELECT setval({quote_literal(name)}, {pick}({row.goal}, {current}), false)")
        if rc != 0:
            return None, err
        return int(out.strip()), ""

    def restore_value(self, seq, after, before):
        """Moves the sequence back to `before`, only if it still stands where the apply left it."""
        name, current = self.current_next(seq)
        rc, out, err = self.psql(f"SELECT CASE WHEN {current} = {after} "
                                 f"THEN setval({quote_literal(name)}, {before}, false) END")
        if rc != 0:
            return None, err
        return (int(out.strip()) if out.strip() else None), ""

    def column_extreme(self, link, increment, timeout_seconds):
        name = quote_ident(link.schema) + "." + quote_ident(link.table)
        func = "max" if increment > 0 else "min"
        rc, out, err = self.psql(f"SELECT {func}({quote_ident(link.column)})::text FROM {name}", timeout_seconds)
        if rc != 0:
            return None, err or f"psql exit code {rc}"
        return (out.strip() or None), ""


class Sync:
    def __init__(self, cfg, out=print):
        self.cfg, self.out = cfg, out
        self.db = Databases(cfg)
        self.stamp = f"{datetime.now():%Y%m%d-%H%M%S}"

    def target_problem(self, changing):
        expected = self.cfg.expected_target
        if expected and expected != self.db.target:
            return f"confs point at {self.db.target} but dataload.toml expects {expected}; nothing was done"
        if changing and not expected:
            return f"[target] expected is not set in dataload.toml (the confs point at {self.db.target})"
        return ""

    def compare(self):
        oracle = self.db.oracle()
        missing = sorted({o.owner for o in oracle} - set(self.db.schema_map))
        if missing:
            raise SequenceError(f"Oracle returned owners not in the confs: {', '.join(missing)}")
        pg = self.db.pg_sequences()
        links = self.db.pg_links()
        rows = match(oracle, pg, links, self.db.schema_map)
        named = sum(1 for o in oracle if not o.identity and not o.name.startswith("ISEQ$$_"))
        self.out(f"Oracle {self.db.source}: {len(oracle)} sequences ({named} named, {len(oracle) - named} identity)")
        self.out(f"PostgreSQL {self.db.target}: {len(pg)} sequences in {len(self.db.pg_schemas)} schemas")
        self.from_data([r for r in rows if r.kind == "postgresql only"], self.all_links(links))
        return oracle, rows

    def all_links(self, links):
        """The catalogue's links plus the links file's, each pair once."""
        extra, problems = read_links_file(self.cfg.sequence_links, set(self.db.pg_schemas))
        known = {(l.seq_label, l.column_label) for l in links}
        return links + [l for l in extra if (l.seq_label, l.column_label) not in known], problems

    def from_data(self, rows, links_and_problems):
        """Sequences only on PostgreSQL: the highest value in the columns each one feeds decides its value."""
        links, _ = links_and_problems
        types = {l.column_label: l.data_type for l in links if l.data_type}
        fed = {}
        for link in links:
            fed.setdefault(link.seq_label, []).append(link)

        def one(row):
            columns = [l for l in fed.get(row.pg, []) if types.get(l.column_label, "") in NUMERIC_TYPES | {""}]
            if not columns:
                row.reason = "no number column is linked to it; left alone"
                return
            values = []
            for link in columns:
                value, err = self.db.column_extreme(link, row.target.increment, self.cfg.sequence_check_timeout)
                if err:
                    row.reason = f"highest value of {link.column_label} not read ({err}); left alone"
                    return
                if value is not None:
                    try:
                        values.append(int(value))
                    except ValueError:
                        row.reason = f"{link.column_label} does not hold whole numbers; left alone"
                        return
            pick = max if row.target.increment > 0 else min
            decide_from_data(row, pick(values) if values else None)

        with ThreadPoolExecutor(max_workers=max(1, self.cfg.count_workers)) as pool:
            list(pool.map(one, rows))

    def summary(self, rows, verb):
        names = {"set": verb, "ahead": "already level or past Oracle", "refused": "refused",
                 "failed": "failed", "oracle only": "only on Oracle", "postgresql only": "only on PostgreSQL"}
        self.out("")
        for action, label in names.items():
            count = sum(1 for r in rows if r.action == action)
            if count:
                self.out(f"  {label:32} {count:5}")
            from_data = sum(1 for r in rows if r.action == action and r.kind == "postgresql only")
            if action in ("set", "failed") and from_data:
                self.out(f"    {'of which only on PostgreSQL':30} {from_data:5}")
        for action in ("refused", "failed", "oracle only"):
            listed = [r for r in rows if r.action == action]
            if listed:
                self.out(f"\n{names[action][0].upper()}{names[action][1:]}:")
                for r in listed:
                    what = r.pg or r.oracle
                    extra = f" (feeds {r.feeds})" if r.feeds and r.action == "oracle only" else ""
                    self.out(f"  {what}{extra}: {r.reason}")

    def write_csv(self, rows, name):
        self.cfg.logs.mkdir(parents=True, exist_ok=True)
        path = self.cfg.logs / f"sequences-{name}-{self.stamp}.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["kind", "oracle_sequence", "identity_column", "postgresql_sequence", "oracle_next_value",
                        "postgresql_next_before", "postgresql_next_after", "action", "reason", "oracle_value_moved_to",
                        "column_highest", "postgresql_target"])
            for r in rows:
                w.writerow([r.kind, r.oracle, r.feeds, r.pg, r.oracle_next, r.pg_next, r.pg_after, r.action, r.reason,
                            r.moved, r.data_highest, self.db.target])
        return path

    def plan(self):
        problem = self.target_problem(changing=False)
        if problem.startswith("confs point"):
            self.out(problem)
            return 2
        if problem:
            self.out(f"warning: {problem}")
        _, rows = self.compare()
        self.summary(rows, "would be set")
        self.out(f"\nNothing was changed. Details: {self.write_csv(rows, 'plan')}")
        return 0

    def apply(self, yes=False, confirm=input):
        problem = self.target_problem(changing=True)
        if problem:
            self.out(problem)
            return 2
        oracle, rows = self.compare()
        todo = [r for r in rows if r.action == "set"]
        self.summary(rows, "to be set")
        if not todo:
            self.out("\nNothing to set.")
        else:
            if not self.agreed(yes, confirm, f"Set {len(todo)} sequences on {self.db.target}?"):
                return 2
            with ThreadPoolExecutor(max_workers=max(1, self.cfg.count_workers)) as pool:
                results = list(pool.map(self.db.set_value, todo))
            for row, (value, err) in zip(todo, results):
                if value is None:
                    row.action, row.reason = "failed", err
                else:
                    row.pg_after = value
                    if value != row.goal:
                        row.reason = "PostgreSQL got past this value meanwhile; left there"
            done = sum(1 for r in todo if r.action == "set")
            self.out(f"\n{done} of {len(todo)} sequences set.")
        moved = self.oracle_moved(oracle, rows)
        failed = [r for r in rows if r.action == "failed"]
        for r in failed:
            self.out(f"FAILED {r.pg}: {r.reason}")
        self.out(f"Details: {self.write_csv(rows, 'apply')}")
        checked = self.check()
        return 1 if failed or moved or checked else 0

    def agreed(self, yes, confirm, question):
        if yes:
            return True
        if not sys.stdin.isatty():
            self.out("\nnot asking without a terminal; pass --yes to go on")
            return False
        if confirm(f"\n{question} Type yes to go on: ").strip().lower() != "yes":
            self.out("nothing was changed")
            return False
        return True

    def undo(self, path, yes=False, confirm=input):
        """Puts back the values an apply's CSV recorded, for each sequence that still stands where apply left it."""
        problem = self.target_problem(changing=True)
        if problem:
            self.out(problem)
            return 2
        try:
            with open(path, newline="") as f:
                recorded = list(csv.DictReader(f))
        except OSError as e:
            self.out(f"cannot read {path}: {e.strerror}")
            return 2
        needed = {"postgresql_sequence", "postgresql_next_before", "postgresql_next_after", "action"}
        if not recorded or not needed <= set(recorded[0]):
            self.out(f"{path} is not a CSV written by 'dataload.sequences apply'")
            return 2
        targets = {r.get("postgresql_target") for r in recorded} - {None, ""}
        if targets and targets != {self.db.target}:
            self.out(f"{path} was written for {', '.join(sorted(targets))}, not {self.db.target}; nothing was done")
            return 2
        sequences = {s.label: s for s in self.db.pg_sequences()}
        todo, results = [], []
        for r in recorded:
            if r["action"] != "set" or not r["postgresql_next_after"] or not r["postgresql_next_before"]:
                continue
            label, after, before = r["postgresql_sequence"], int(r["postgresql_next_after"]), int(r["postgresql_next_before"])
            seq = sequences.get(label)
            if seq is None:
                results.append([label, "", before, "", "skipped", "sequence not found on PostgreSQL"])
            elif seq.next_value != after:
                results.append([label, seq.next_value, before, "", "skipped",
                                f"used or moved since the apply (next was {after}, is now {seq.next_value}); "
                                "moving it back could hand out numbers already used"])
            else:
                todo.append((label, seq, after, before))
        if not todo and not results:
            self.out(f"{path} records no sequence that apply set; nothing to undo")
            return 0
        self.out(f"{len(todo)} sequences to put back on {self.db.target}, {len(results)} skipped")
        for label, _, after, before in todo[:20]:
            self.out(f"  {label}: {after} -> {before}")
        if len(todo) > 20:
            self.out(f"  ... and {len(todo) - 20} more")
        if todo and not self.agreed(yes, confirm, f"Put {len(todo)} sequences back?"):
            return 2
        with ThreadPoolExecutor(max_workers=max(1, self.cfg.count_workers)) as pool:
            done = list(pool.map(lambda t: self.db.restore_value(t[1], t[2], t[3]), todo))
        for (label, _, after, before), (value, err) in zip(todo, done):
            if err:
                results.append([label, after, before, "", "failed", err])
            elif value is None:
                results.append([label, after, before, "", "skipped", "used or moved meanwhile; left as it is"])
            else:
                results.append([label, after, before, value, "restored", ""])
        self.cfg.logs.mkdir(parents=True, exist_ok=True)
        out = self.cfg.logs / f"sequences-undo-{self.stamp}.csv"
        with open(out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["postgresql_sequence", "next_before_undo", "next_wanted", "next_after_undo", "result", "reason"])
            w.writerows(sorted(results))
        restored = sum(1 for r in results if r[4] == "restored")
        self.out(f"\n{restored} of {len(todo)} sequences put back.")
        for r in sorted(results):
            if r[4] != "restored":
                self.out(f"  {r[4].upper()} {r[0]}: {r[5]}")
        self.out(f"Details: {out}")
        return 0 if restored == len(results) else 1

    def oracle_moved(self, before, rows):
        """Reads Oracle again: a value that moved means Oracle is still being written to."""
        now = {o.label: o.next_value for o in self.db.oracle()}
        moved = []
        for row in rows:
            if row.oracle and row.oracle in now and now[row.oracle] != row.oracle_next:
                row.moved = now[row.oracle]
                moved.append(row)
        if moved:
            self.out(f"\nWARNING: {len(moved)} Oracle sequences moved while this ran, so Oracle is still being "
                     "written to. Run apply again once writes have stopped; it only moves what is behind:")
            for r in moved[:20]:
                self.out(f"  {r.oracle}: {r.oracle_next} -> {r.moved}")
            if len(moved) > 20:
                self.out(f"  ... and {len(moved) - 20} more, listed in the CSV")
        return moved

    def check(self):
        """Every column a sequence feeds against that sequence's next value. Changes nothing."""
        problem = self.target_problem(changing=False)
        if problem.startswith("confs point"):
            self.out(problem)
            return 2
        sequences = {s.label: s for s in self.db.pg_sequences()}
        links, problems = self.all_links(self.db.pg_links())
        types = {l.column_label: l.data_type for l in links if l.data_type}
        results = []

        def one(link):
            seq = sequences.get(link.seq_label)
            if seq is None:
                return link, None, "not checked", "sequence not found on PostgreSQL"
            data_type = types.get(link.column_label, "")
            if data_type and data_type not in NUMERIC_TYPES:
                return link, seq, "not checked", f"column type is {data_type}"
            value, err = self.db.column_extreme(link, seq.increment, self.cfg.sequence_check_timeout)
            if err:
                return link, seq, "not checked", err
            try:
                highest = int(value) if value is not None else None
            except ValueError:
                return link, seq, "not checked", "values are not whole numbers"
            return (link, seq) + check_label(highest, seq.next_value, seq.increment) + (highest,)

        with ThreadPoolExecutor(max_workers=max(1, self.cfg.count_workers)) as pool:
            results = list(pool.map(one, sorted(links, key=lambda l: (l.seq_label, l.column_label))))

        self.cfg.logs.mkdir(parents=True, exist_ok=True)
        path = self.cfg.logs / f"sequences-check-{self.stamp}.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["postgresql_sequence", "column", "link", "sequence_next_value", "column_highest", "result",
                        "reason"])
            for item in results:
                link, seq, result, reason = item[:4]
                highest = item[4] if len(item) > 4 else ""
                w.writerow([link.seq_label, link.column_label, link.source, seq.next_value if seq else "", highest,
                            result, reason])

        counts = {}
        for item in results:
            counts[item[2]] = counts.get(item[2], 0) + 1
        self.out(f"\nCheck: {len(results)} columns fed by a sequence")
        for result in ("ok", "placeholder", "needs a look", "not checked"):
            if counts.get(result):
                self.out(f"  {result:32} {counts[result]:5}")
        for p in problems:
            self.out(f"warning: {p}")
        for result in ("needs a look", "not checked"):
            listed = [i for i in results if i[2] == result]
            if listed:
                self.out(f"\n{result[0].upper()}{result[1:]}:")
                for item in listed:
                    link, seq, _, reason = item[:4]
                    numbers = f" highest {item[4]}, sequence next {seq.next_value}" if len(item) > 4 else ""
                    self.out(f"  {link.column_label} ({link.seq_label}):{numbers} {reason}".rstrip())
        self.out(f"Details: {path}")
        return 1 if counts.get("needs a look") or counts.get("not checked") else 0


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python3 -m dataload.sequences",
                                     description="Set PostgreSQL's sequences from Oracle's after the data load")
    parser.add_argument("--config", default="dataload.toml")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("plan", help="dry run: show what would change, change nothing")
    apply = sub.add_parser("apply", help="set the sequences that are behind Oracle, then run the check")
    apply.add_argument("--yes", action="store_true", help="do not ask before changing anything")
    sub.add_parser("check", help="compare each sequence with the highest value in the columns it feeds")
    undo = sub.add_parser("undo", help="put back the sequences an apply set, from its logs/sequences-apply-*.csv")
    undo.add_argument("csv", help="the CSV the apply wrote")
    undo.add_argument("--yes", action="store_true", help="do not ask before changing anything")
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
    cfg = config.load(args.config)
    try:
        sync = Sync(cfg)
        if args.command == "plan":
            return sync.plan()
        if args.command == "apply":
            return sync.apply(yes=args.yes)
        if args.command == "undo":
            return sync.undo(args.csv, yes=args.yes)
        return sync.check()
    except (SequenceError, plan.PlanError) as e:
        print(f"error: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
