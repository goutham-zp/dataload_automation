"""What to ask Oracle and PostgreSQL to learn a table's row count and size."""
import re
from dataclasses import dataclass
from pathlib import Path

# Reads "id<TAB>sql" lines on stdin and answers "id<TAB>value" or "id<TAB>ERROR message".
# It uses the same Perl driver ora2pg uses, so it works wherever ora2pg can reach Oracle.
ORACLE_COUNT_PL = r"""
use strict;
use DBI;
$| = 1;
my $dbh = DBI->connect($ENV{DL_ORA_DSN}, $ENV{DL_ORA_USER}, $ENV{DL_ORA_PWD},
                       {RaiseError => 0, PrintError => 0, AutoCommit => 1});
if (!$dbh) {
    my $why = $DBI::errstr || "connection failed";
    $why =~ s/\s+/ /g;
    print "FATAL\t$why\n";
    exit 2;
}
while (my $line = <STDIN>) {
    chomp $line;
    my ($id, $sql) = split /\t/, $line, 2;
    next unless defined $sql;
    my @row = $dbh->selectrow_array($sql);
    if (@row && defined $row[0]) {
        print "$id\t$row[0]\n";
    } else {
        my $why = $dbh->errstr || "the query returned nothing";
        $why =~ s/\s+/ /g;
        print "$id\tERROR $why\n";
    }
}
$dbh->disconnect;
"""


@dataclass
class Spec:
    """One table of one load step, and the filter its load applies (None = the whole table)."""
    step_id: str
    conf_path: Path
    schema: str            # Oracle owner, upper case
    table: str             # upper case
    pg: str                # "schema"."table" as PostgreSQL names it
    filter: str = None
    chunked: bool = False  # chunked tables are not truncated, so PostgreSQL is counted with the filter too

    @property
    def key(self):
        return (self.step_id, self.table)

    @property
    def name(self):
        return f"{self.schema}.{self.table}"

    def oracle_count_sql(self):
        return f'SELECT COUNT(1) FROM "{self.schema}"."{self.table}"' + (f" WHERE {self.filter}" if self.filter else "")

    def oracle_size_sql(self):
        # The table's own segments (every partition) plus its LOB segments; indexes are not included.
        return ("SELECT NVL(SUM(bytes), 0) FROM dba_segments "
                f"WHERE owner = '{self.schema}' AND (segment_name = '{self.table}' OR segment_name IN "
                f"(SELECT segment_name FROM dba_lobs WHERE owner = '{self.schema}' AND table_name = '{self.table}'))")

    def pg_count_sql(self):
        return f"SELECT count(1) FROM {self.pg}" + (f" WHERE {self.filter}" if self.filter and self.chunked else "")

    def pg_size_sql(self):
        # A partitioned table has no storage of its own, so its partitions are added up;
        # pg_partition_tree is empty for an ordinary table, which is then measured directly.
        literal = self.pg.replace("'", "''")
        return (f"SELECT COALESCE((SELECT sum(pg_total_relation_size(relid)) FROM pg_partition_tree('{literal}')), "
                f"pg_total_relation_size('{literal}'))::bigint")


def where_filters(value):
    """ora2pg's WHERE directive: 'TABLE[cond] OTHER[cond]', or a bare condition for every table ("*")."""
    value = (value or "").strip()
    if not value:
        return {}
    per_table = re.findall(r"(\w+)\s*\[(.*?)\](?=\s+\w+\s*\[|\s*$)", value)
    if per_table:
        return {name.upper(): cond.strip() for name, cond in per_table}
    return {"*": value}


def human_bytes(n):
    if n is None:
        return ""
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{int(size)} B" if unit == "B" else f"{size:,.1f} {unit}"
        size /= 1024
