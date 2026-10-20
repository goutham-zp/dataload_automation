"""Set PostgreSQL's sequences from Oracle's after the data load.

    python3 -m dataload.sequences [--config dataload.toml] plan | apply [--yes] | check | undo CSV [--yes]

Named sequences are matched by schema and name; identity columns by table and column, because their
sequence names never match (ISEQ$$_645901 on Oracle, company_offer_company_offer_id_seq on PostgreSQL).
Oracle's value is copied, or the highest value in the columns a sequence feeds plus one when that is further and
those rows look like its own run; a sequence only on PostgreSQL goes by its data alone. A sequence already at or
past its value is left alone, so apply never moves anything back and running again is always safe. Whatever it
will not decide on its own is listed for a person with the reason and a suggested fix (logs/sequences-manual-*.csv).
undo puts the sequences an apply set back where they were, unless they were used since.
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

# Sequences in schemas no conf names: this script never reads or sets them, so they are listed for a person.
OTHER_SCHEMAS_SQL = """
SELECT n.nspname, count(*)
FROM pg_sequence s
JOIN pg_class c ON c.oid = s.seqrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN (%s) AND n.nspname <> 'information_schema' AND n.nspname NOT LIKE 'pg\\_%%'
GROUP BY n.nspname
ORDER BY n.nspname
"""

NUMERIC_TYPES = {"smallint", "integer", "bigint", "numeric"}
PLACEHOLDER_RE = re.compile(r"^-?9{5,}\d?$")   # 999999, 999999999, 9999999991: marker rows typed in by hand
SEP = "\x1f"                                     # psql field separator; never part of a name

# The manual list, most urgent first; at most this many of each kind on screen, all of them in the CSV.
MANUAL_ORDER = ["failed", "refused", "needs a look", "rows above the sequence", "marker rows", "outside the range",
                "column not read", "not a number column", "not whole numbers", "sequence not found",
                "more than one sequence", "no column linked", "only on Oracle", "schema not in the confs"]
MANUAL_SHOWN = 25


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
    data_highest: int = None        # highest value in the columns the sequence feeds (marker rows stepped over)
    data_next: int = None           # set when its data is past Oracle's value: data_highest + increment
    fix: str = ""                   # refused or only on one side: what a person can do about it
    columns: int = 0                # how many columns it feeds that PostgreSQL knows of
    target: PgSeq = field(default=None, repr=False)
    manual: list = field(default_factory=list, repr=False)     # Manual items found for its columns

    @property
    def goal(self):
        return self.data_next if self.data_next is not None else self.oracle_next


@dataclass
class Manual:
    """Something the script would not do on its own: what, why, and what a person can do."""
    sequence: str
    column: str
    problem: str
    reason: str
    fix: str


def quote_ident(name):
    return '"' + name.replace('"', '""') + '"'


def quote_literal(text):
    return "'" + text.replace("'", "''") + "'"


def to_int(text):
    return int(text) if text not in ("", None) else None


def ahead(value, other, increment):
    """True when `value` is at or past `other` in the direction the sequence counts."""
    return value >= other if increment > 0 else value <= other


def own_run(goal, highest, distinct, first, increment):
    """True when the values from the sequence's next value up to the highest look like its own run of numbers:
    they start in the lower half of that range and fill at least 1 in 100 of its numbers. A sparse block far
    above (reserved IDs, IDs typed in by hand) is left to a person, since raising the sequence would jump it."""
    span = (highest - goal) // increment + 1
    gap = (first - goal) // increment
    return gap * 2 < span and distinct * 100 >= span


def setval_sql(seq, value):
    return f"SELECT setval({quote_literal(seq)}, {value}, false);"


def link_fix(link):
    if link.source == "links file":
        return f"the link is probably wrong: fix or remove the {link.seq_label} line in sequence_links.txt"
    return f"the column default or identity on {link.column_label} is probably wrong; check it by hand"


def block_item(seq, link, base, highest, distinct, first):
    """A sequence with rows above it that do not look like its own run: say how soon it runs into them."""
    inc = seq.increment
    after = (first - base) // inc
    when = "on the next insert" if after == 0 else f"after {after} more inserts"
    rows = f"one value, {first}, is" if distinct == 1 else f"{distinct} different values from {first} to {highest} are"
    reason = (f"{rows} at or past its next value {base}, too sparse or too far up to be its own run (reserved IDs, "
              f"or IDs typed in by hand?); it runs into {first} {when}")
    fix = f"if those IDs are the sequence's own: {setval_sql(seq.label, highest + inc)}"
    if inc > 0:
        column, table = quote_ident(link.column), f"{link.schema}.{link.table}"
        fix += (f" If they are a reserved block, move it to the first free number instead: SELECT min(a.{column}) + 1 "
                f"FROM {table} a WHERE a.{column} >= {base} AND NOT EXISTS (SELECT 1 FROM {table} b WHERE "
                f"b.{column} = a.{column} + 1)")
    return Manual(seq.label, link.column_label, "rows above the sequence", reason, fix)


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
                table, column = f"{schema}.{o.table.lower()}", o.column.lower()
                usual = f"{o.table.lower()}_{column}_seq"
                if (schema, usual) in pg_by_name:
                    row.reason += f"; {schema}.{usual} exists but is not linked to it"
                    row.fix = (f"ALTER SEQUENCE {schema}.{usual} OWNED BY {row.feeds}; ALTER TABLE {table} ALTER "
                               f"COLUMN {column} SET DEFAULT nextval('{schema}.{usual}'); then run apply again")
                else:
                    row.fix = (f"ALTER TABLE {table} ALTER COLUMN {column} ADD GENERATED BY DEFAULT AS IDENTITY "
                               f"(START WITH {o.next_value}); then run apply again")
            elif found[0].source == "default":
                row.reason = "matched through the column default"
        elif o.name.startswith("ISEQ$$_"):
            rows.append(Row("identity", oracle=o.label, oracle_next=o.next_value, action="oracle only",
                            reason="identity sequence whose table was not found (dropped?)",
                            fix="nothing, if the table was dropped on Oracle (a recycle bin leftover)"))
            continue
        else:
            row = Row("named", oracle=o.label, oracle_next=o.next_value)
            target = pg_by_name.get((schema, o.name.lower()))
            if target is None:
                row.action, row.reason = "oracle only", "no sequence with this name on PostgreSQL"
                row.fix = (f"if anything on PostgreSQL uses it: CREATE SEQUENCE {schema}.{o.name.lower()} "
                           f"START WITH {o.next_value} INCREMENT BY {o.increment}; otherwise nothing")
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
            pick = max if group[0].target.increment > 0 else min
            for row in group:
                row.action, row.reason = "refused", "matched by more than one Oracle sequence: " + ", ".join(
                    r.oracle for r in group)
                row.fix = (f"find which of them really feeds it; their highest value is safe: "
                           f"{setval_sql(row.pg, pick(r.oracle_next for r in group))}")
    for s in sorted(pg, key=lambda s: (s.schema, s.name)):
        if s.label not in matched:
            rows.append(Row("postgresql only", pg=s.label, pg_next=s.next_value, action="postgresql only",
                            reason="left alone", target=s))
    return rows


def decide(row, o, target):
    if o.increment != target.increment:
        row.action, row.reason = "refused", f"increment differs: Oracle {o.increment}, PostgreSQL {target.increment}"
        row.fix = f"ALTER SEQUENCE {target.label} INCREMENT BY {o.increment}; then run apply again"
    elif not target.min_value <= o.next_value <= target.max_value:
        row.action, row.reason = "refused", (f"Oracle's value is outside PostgreSQL's range "
                                             f"{target.min_value}..{target.max_value}")
        row.fix = widen_fix(target)
    elif ahead(target.next_value, o.next_value, o.increment):
        row.action = "ahead"
        row.reason = "level with Oracle" if target.next_value == o.next_value else "already past Oracle, left as it is"
    elif not target.can_update:
        row.action, row.reason = "refused", "this login may not change the sequence (UPDATE privilege)"
        row.fix = grant_fix(target)
    else:
        row.action = "set"


def widen_fix(seq):
    return (f"ALTER SEQUENCE {seq.label} AS bigint; check the columns it feeds are bigint too; "
            "then run apply again")


def grant_fix(seq):
    return f"run apply as the sequence's owner, or GRANT UPDATE ON SEQUENCE {seq.label} TO <this login>"


def decide_from_data(row, highest, column=""):
    """The highest value in the columns a sequence feeds, plus one, when that is past where it goes now: Oracle's
    value for a matched sequence, its own next value otherwise. Never moves anything back."""
    seq = row.target
    only_pg = row.kind == "postgresql only"
    row.data_highest = highest
    if highest is None:
        if only_pg:
            row.reason = "its columns are empty; left alone"
        return
    if PLACEHOLDER_RE.match(str(highest)):
        row.reason = f"highest value {highest} is an all-nines marker row; left alone, set it by hand if needed"
        return
    want = highest + seq.increment
    current = row.goal if row.action == "set" else seq.next_value
    if ahead(current, want, seq.increment):
        if only_pg:
            row.reason = f"already past its data (highest {highest}); left as it is"
        return
    where = f"{column} holds up to {highest}" if column else f"highest {highest}"
    if not seq.min_value <= want <= seq.max_value:
        if only_pg:
            row.action = "refused"
        row.reason = (("" if only_pg else "Oracle's value is used, but its data's ") +
                      f"highest value + 1 is outside the range {seq.min_value}..{seq.max_value}")
        row.fix = widen_fix(seq)
        if not only_pg:
            row.manual.append(Manual(seq.label, column, "outside the range", row.reason, row.fix))
        return
    if not seq.can_update:
        row.action, row.reason, row.fix = "refused", "this login may not change the sequence (UPDATE privilege)", \
            grant_fix(seq)
        return
    if only_pg:
        row.reason = f"only on PostgreSQL: set from its data (highest {highest})"
    elif row.action == "set":
        row.reason = f"set past its data: {where}, Oracle's value is {row.oracle_next}"
    else:
        row.reason = f"was level with or past Oracle but behind its data: {where}"
    row.action, row.data_next = "set", want


def link_problems(links, known):
    """Links-file lines naming a missing sequence, and columns more than one sequence is linked to."""
    items, by_column = [], {}
    for link in links:
        if link.seq_label not in known:
            items.append(Manual(link.seq_label, link.column_label, "sequence not found",
                                "sequence_links.txt names a sequence that does not exist on PostgreSQL",
                                "fix or remove its line in sequence_links.txt"))
        else:
            by_column.setdefault(link.column_label, {}).setdefault(link.seq_label, []).append(link.source)
    for column, seqs in sorted(by_column.items()):
        if len(seqs) > 1:
            items.append(Manual(", ".join(sorted(seqs)), column, "more than one sequence",
                                "linked to " + "; ".join(f"{s} ({', '.join(src)})" for s, src in sorted(seqs.items())),
                                "keep the one its identity or column default calls; for an extra OWNED BY: "
                                "ALTER SEQUENCE <other> OWNED BY NONE, for a links-file line: fix "
                                "sequence_links.txt; drop the other if nothing uses it"))
    return items


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

    def column_extreme(self, link, increment, timeout_seconds, beyond=None):
        """The highest value (lowest for a descending sequence), or the highest below `beyond`."""
        name = quote_ident(link.schema) + "." + quote_ident(link.table)
        column = quote_ident(link.column)
        func = "max" if increment > 0 else "min"
        where = f" WHERE {column} {'<' if increment > 0 else '>'} {beyond}" if beyond is not None else ""
        rc, out, err = self.psql(f"SELECT {func}({column})::text FROM {name}{where}", timeout_seconds)
        if rc != 0:
            return None, err or f"psql exit code {rc}"
        return (out.strip() or None), ""

    def real_extreme(self, link, increment, timeout_seconds):
        """The highest value under any all-nines marker rows, as (value, marker, error); values are text."""
        marker, beyond = "", None
        for _ in range(5):
            value, err = self.column_extreme(link, increment, timeout_seconds, beyond)
            if err or value is None or not PLACEHOLDER_RE.match(value):
                return value, marker, err
            marker, beyond = marker or value, value
        return None, marker, "more than 5 different all-nines marker values"

    def column_above(self, link, goal, highest, increment, timeout_seconds):
        """From goal to highest: how many different values, and the first one the sequence would run into."""
        name = quote_ident(link.schema) + "." + quote_ident(link.table)
        column = quote_ident(link.column)
        low, high, first = (goal, highest, "min") if increment > 0 else (highest, goal, "max")
        rc, out, err = self.psql(f"SELECT count(DISTINCT {column}), {first}({column})::text FROM {name} "
                                 f"WHERE {column} BETWEEN {low} AND {high}", timeout_seconds)
        if rc != 0:
            return None, None, err or f"psql exit code {rc}"
        distinct, value = out.strip().split(SEP)
        if not value:
            return None, None, "the rows above the sequence were gone when read again; run it again"
        return int(distinct), int(value), ""

    def other_schemas(self):
        """Schemas on the target holding sequences that no conf names, with how many."""
        schemas = ", ".join(quote_literal(s) for s in self.pg_schemas)
        return [(r[0], int(r[1])) for r in self.rows(OTHER_SCHEMAS_SQL % schemas)]


class Sync:
    def __init__(self, cfg, out=print):
        self.cfg, self.out = cfg, out
        self.db = Databases(cfg)
        self.stamp = f"{datetime.now():%Y%m%d-%H%M%S}"
        self.extremes = {}      # column -> (highest, marker, error): the data does not change while this runs
        self.extra = []         # Manual items not tied to one sequence row: links, other schemas

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
        links_and_problems = self.all_links(links)
        known = {s.label for s in pg}
        self.extra = link_problems(links_and_problems[0], known) + self.other_schema_items()
        self.settle(rows, links_and_problems, known)
        return oracle, rows

    def all_links(self, links):
        """The catalogue's links plus the links file's, each pair once."""
        extra, problems = read_links_file(self.cfg.sequence_links, set(self.db.pg_schemas))
        known = {(l.seq_label, l.column_label) for l in links}
        return links + [l for l in extra if (l.seq_label, l.column_label) not in known], problems

    def extreme(self, link, increment):
        key = (link.column_label, increment > 0)
        if key not in self.extremes:
            self.extremes[key] = self.db.real_extreme(link, increment, self.cfg.sequence_check_timeout)
        return self.extremes[key]

    def settle(self, rows, links_and_problems, known):
        """Each sequence against the data in the columns it feeds. It is raised past its data when the rows above
        it look like its own run of numbers; what cannot be decided that way is listed for a person."""
        links, _ = links_and_problems
        types = {l.column_label: l.data_type for l in links if l.data_type}
        fed = {}
        for link in links:
            fed.setdefault(link.seq_label, {}).setdefault(link.column_label, link)     # each column once
        todo = [r for r in rows if r.target is not None and r.action in ("set", "ahead", "postgresql only")]
        with ThreadPoolExecutor(max_workers=max(1, self.cfg.count_workers)) as pool:
            list(pool.map(lambda r: self.settle_one(r, list(fed.get(r.pg, {}).values()), types), todo))

    def settle_one(self, row, links, types):
        seq, inc = row.target, row.target.increment
        only_pg = row.kind == "postgresql only"
        row.columns = len(links)
        if not links:
            if only_pg:
                row.reason = "no number column is linked to it; left alone"
                row.manual.append(Manual(seq.label, "", "no column linked", (
                    "only on PostgreSQL, and nothing shows which column it feeds (no identity, OWNED BY, column "
                    "default or sequence_links.txt line)"), (
                    f"find what calls nextval('{seq.label}'): the application, or SELECT proname FROM pg_proc WHERE "
                    f"prosrc ILIKE '%{seq.name}%'; add its column to sequence_links.txt and run apply again, or drop "
                    "it if nothing uses it")))
            return
        found, marked, unread = [], False, False
        for link in links:
            data_type = types.get(link.column_label, "")
            if data_type and data_type not in NUMERIC_TYPES:
                row.manual.append(self.not_number(link, data_type))
                unread = True
                continue
            value, marker, err = self.extreme(link, inc)
            if err:
                row.manual.append(self.not_read(link, err))
                unread = True
                continue
            try:
                highest = int(value) if value is not None else None
            except ValueError:
                row.manual.append(Manual(link.seq_label, link.column_label, "not whole numbers",
                                         "the column holds values that are not whole numbers", link_fix(link)))
                unread = True
                continue
            if marker and only_pg:
                # With no Oracle sequence to go by, marker rows are not stepped over without a person.
                marked = True
                if highest is not None and ahead(highest, seq.next_value, inc):
                    row.manual.append(Manual(seq.label, link.column_label, "marker rows", (
                        f"all-nines marker rows ({marker}) and real rows up to {highest}, at or past the sequence's "
                        f"next value {seq.next_value}"), (
                        f"if the marker rows are meant to stay: {setval_sql(seq.label, highest + inc)}")))
                continue
            found.append((link, highest))

        base = row.goal if row.action == "set" else seq.next_value
        best, blocks = None, []
        for link, highest in sorted((f for f in found if f[1] is not None), key=lambda f: f[1] * inc):
            if not ahead(highest, base, inc):
                continue
            distinct, first, err = self.db.column_above(link, base, highest, inc, self.cfg.sequence_check_timeout)
            if err:
                row.manual.append(self.not_read(link, err))
                unread = True
            elif own_run(base, highest, distinct, first, inc):
                base, best = highest + inc, (link, highest)
            else:
                blocks.append((highest, block_item(seq, link, base, highest, distinct, first)))
        # A block below a column that raised the sequence past it no longer matters.
        row.manual += [item for highest, item in blocks if ahead(highest, base, inc)]
        highs = [h for _, h in found if h is not None]
        if best:
            decide_from_data(row, best[1], best[0].column_label)
        elif only_pg:
            if blocks:
                row.reason = "the rows above it look like a separate block; left alone"
            elif unread and not highs:
                row.reason = "its columns could not be read; left alone"
            elif marked and not highs:
                row.reason = "all-nines marker rows in its columns; left alone"
            else:
                decide_from_data(row, (max if inc > 0 else min)(highs) if highs else None)
        if row.data_highest is None and highs:
            row.data_highest = (max if inc > 0 else min)(highs)

    def not_read(self, link, err):
        fix = (f"read it by hand: SELECT max({quote_ident(link.column)}) FROM {link.schema}.{link.table}; or raise "
               "check_timeout_seconds under [sequences] in dataload.toml and run again")
        if link.source == "links file":
            fix += "; if the column does not exist, fix its line in sequence_links.txt"
        return Manual(link.seq_label, link.column_label, "column not read", err, fix)

    def not_number(self, link, data_type):
        fix = link_fix(link) if link.source == "links file" else (
            f"check the column default is meant to call {link.seq_label}, and compare by hand: "
            f"SELECT max({quote_ident(link.column)}) FROM {link.schema}.{link.table}")
        return Manual(link.seq_label, link.column_label, "not a number column", f"the column is {data_type}", fix)

    def other_schema_items(self):
        return [Manual(f"{count} sequence" + ("s" if count != 1 else ""), schema, "schema not in the confs",
                       "no conf loads this schema, so its sequences are not read or set",
                       "set them by hand, or add the schema's conf to the content folder and run again")
                for schema, count in self.db.other_schemas()]

    def manual_items(self, rows):
        items = []
        for r in rows:
            if r.action in ("refused", "failed", "oracle only"):
                problem = {"oracle only": "only on Oracle"}.get(r.action, r.action)
                fix = r.fix or ("fix the error and run apply again" if r.action == "failed" else "")
                items.append(Manual(r.pg or r.oracle, r.feeds if r.action == "oracle only" else "", problem,
                                    r.reason, fix))
            items += r.manual
        return items + self.extra

    def report_manual(self, items):
        """Prints what needs a person, grouped by problem, and writes them all to a CSV. Returns the items."""
        seen, unique = set(), []
        for m in items:
            if (m.sequence, m.column) not in seen:
                seen.add((m.sequence, m.column))
                unique.append(m)
        if not unique:
            self.out("\nNothing needs a person.")
            return unique
        self.cfg.logs.mkdir(parents=True, exist_ok=True)
        path = self.cfg.logs / f"sequences-manual-{self.stamp}.csv"
        unique.sort(key=lambda m: (MANUAL_ORDER.index(m.problem) if m.problem in MANUAL_ORDER else len(MANUAL_ORDER),
                                   m.sequence, m.column))
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["problem", "sequence", "column", "reason", "suggested_fix", "postgresql_target"])
            for m in unique:
                w.writerow([m.problem, m.sequence, m.column, m.reason, m.fix, self.db.target])
        self.out(f"\nNeeds a person: {len(unique)}, each with the reason and a suggested fix")
        groups = {}
        for m in unique:
            groups.setdefault(m.problem, []).append(m)
        for problem, group in groups.items():
            self.out(f"\n  {problem} ({len(group)}):")
            for m in group[:MANUAL_SHOWN]:
                self.out(f"    {m.sequence}" + (f"  ->  {m.column}" if m.column else ""))
                self.out(f"        why: {m.reason}")
                if m.fix:
                    self.out(f"        fix: {m.fix}")
            if len(group) > MANUAL_SHOWN:
                self.out(f"    ... and {len(group) - MANUAL_SHOWN} more in the CSV")
        self.out(f"\nList: {path}")
        return unique

    def summary(self, rows, verb):
        names = {"set": verb, "ahead": "already level or past Oracle", "refused": "refused",
                 "failed": "failed", "oracle only": "only on Oracle", "postgresql only": "only on PostgreSQL"}
        self.out("")
        for action, label in names.items():
            count = sum(1 for r in rows if r.action == action)
            if count:
                self.out(f"  {label:32} {count:5}")
            from_data = sum(1 for r in rows if r.action == action and r.kind == "postgresql only")
            past_oracle = sum(1 for r in rows if r.action == action and r.kind != "postgresql only"
                              and r.data_next is not None)
            if action in ("set", "failed") and from_data:
                self.out(f"    {'of which only on PostgreSQL':30} {from_data:5}")
            if action in ("set", "failed") and past_oracle:
                self.out(f"    {'of which past Oracle (data)':30} {past_oracle:5}")
        blind = sum(1 for r in rows if r.kind != "postgresql only" and r.action in ("set", "ahead") and not r.columns)
        if blind:
            self.out(f"\n{blind} sequences matched to Oracle feed no column PostgreSQL knows of, so only Oracle's "
                     "value is used for them (a sequence_links.txt line lets their data count too).")

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
        self.report_manual(self.manual_items(rows))
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
        self.out(f"Details: {self.write_csv(rows, 'apply')}")
        checked = self.check(report=False)
        manual = self.report_manual(self.manual_items(rows) + self.check_items)
        return 1 if failed or moved or checked or manual else 0

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

    def check(self, report=True):
        """Every column a sequence feeds against that sequence's next value. Changes nothing. What needs a person
        is kept in self.check_items, and listed when `report` is set."""
        self.check_items = []
        problem = self.target_problem(changing=False)
        if problem.startswith("confs point"):
            self.out(problem)
            return 2
        sequences = {s.label: s for s in self.db.pg_sequences()}
        links, problems = self.all_links(self.db.pg_links())
        types = {l.column_label: l.data_type for l in links if l.data_type}
        results = []
        if report:
            self.check_items += link_problems(links, set(sequences)) + self.other_schema_items()

        def one(link):
            seq = sequences.get(link.seq_label)
            if seq is None:
                return link, None, "not checked", "sequence not found on PostgreSQL"
            data_type = types.get(link.column_label, "")
            if data_type and data_type not in NUMERIC_TYPES:
                return link, seq, "not checked", f"column type is {data_type}", self.not_number(link, data_type)
            value, marker, err = self.extreme(link, seq.increment)
            if err:
                return link, seq, "not checked", err, self.not_read(link, err)
            try:
                highest = int(value) if value is not None else None
            except ValueError:
                return link, seq, "not checked", "values are not whole numbers", Manual(
                    link.seq_label, link.column_label, "not whole numbers", "the column holds values that are not "
                    "whole numbers", link_fix(link))
            result, reason = check_label(highest, seq.next_value, seq.increment)
            if marker and result == "ok":
                result, reason = "placeholder", (f"all-nines marker rows ({marker}) above the real rows; expected, "
                                                 "same as on Oracle")
            item = None
            if result == "needs a look":
                distinct, first, err = self.db.column_above(link, seq.next_value, highest, seq.increment,
                                                            self.cfg.sequence_check_timeout)
                if err:
                    item = self.not_read(link, err)
                elif own_run(seq.next_value, highest, distinct, first, seq.increment):
                    item = Manual(seq.label, link.column_label, "needs a look", (
                        f"rows from {first} to {highest} already hold numbers the sequence will hand out (next "
                        f"{seq.next_value})"), f"run apply, which raises it, or {setval_sql(seq.label, highest + seq.increment)}")
                else:
                    item = block_item(seq, link, seq.next_value, highest, distinct, first)
            return link, seq, result, reason, item, highest

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
                highest = item[5] if len(item) > 5 else ""
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
        for item in results:
            if len(item) > 4 and item[4] is not None:
                self.check_items.append(item[4])
            elif item[2] == "not checked":
                link, _, _, reason = item[:4]
                self.check_items.append(Manual(link.seq_label, link.column_label, "sequence not found", reason,
                                               "fix or remove its line in sequence_links.txt"))
        self.out(f"Details: {path}")
        if report:
            self.report_manual(self.check_items)
        return 1 if counts.get("needs a look") or counts.get("not checked") or self.check_items else 0


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
