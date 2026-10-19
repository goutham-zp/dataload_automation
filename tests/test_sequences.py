"""Tests for dataload.sequences. The first group needs no database; the second uses the local PostgreSQL 18
the end-to-end tests use, with tests/fake/oracle_rows standing in for Oracle."""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from dataload.sequences import Link, OracleSeq, PgSeq, Row, check_label, decide_from_data, match, read_links_file

ROOT = Path(__file__).resolve().parents[1]
PGBIN = Path(os.environ.get("DATALOAD_TEST_PGBIN", "/opt/homebrew/opt/postgresql@18/bin"))
PGPORT = os.environ.get("DATALOAD_TEST_PGPORT", "54318")
PSQL = str(PGBIN / "psql")
DB = "dataload_test"
SCHEMAS = {"SEQA": "seqa", "SEQB": "seqb"}


def pg_seq(name, next_value=1, increment=1, schema="seqa", called=True, max_value=2 ** 63 - 1, min_value=1):
    last = next_value - increment if called else next_value
    return PgSeq(schema, name, increment, min_value, max_value, 1, last, called)


def by_oracle(rows):
    return {r.oracle or r.pg: r for r in rows}


class Matching(unittest.TestCase):
    def test_named_sequence_behind_oracle_is_set(self):
        rows = match([OracleSeq("SEQA", "ORDER_SEQ", 51, 1)], [pg_seq("order_seq", 1)], [], SCHEMAS)
        self.assertEqual((rows[0].action, rows[0].pg, rows[0].oracle_next, rows[0].pg_next),
                         ("set", "seqa.order_seq", 51, 1))

    def test_never_moves_a_sequence_back(self):
        rows = by_oracle(match([OracleSeq("SEQA", "A", 100, 1), OracleSeq("SEQA", "B", 100, 1)],
                               [pg_seq("a", 500), pg_seq("b", 100)], [], SCHEMAS))
        self.assertEqual(rows["SEQA.A"].action, "ahead")
        self.assertIn("past Oracle", rows["SEQA.A"].reason)
        self.assertEqual((rows["SEQA.B"].action, rows["SEQA.B"].reason), ("ahead", "level with Oracle"))

    def test_descending_sequence_counts_down(self):
        rows = by_oracle(match([OracleSeq("SEQA", "DOWN", -21, -1), OracleSeq("SEQA", "PAST", -21, -1)],
                               [pg_seq("down", -1, -1, min_value=-2 ** 63, max_value=-1),
                                pg_seq("past", -40, -1, min_value=-2 ** 63, max_value=-1)], [], SCHEMAS))
        self.assertEqual(rows["SEQA.DOWN"].action, "set")
        self.assertEqual(rows["SEQA.PAST"].action, "ahead")

    def test_next_value_respects_is_called(self):
        self.assertEqual(pg_seq("x", 51, called=False).next_value, 51)
        self.assertEqual(PgSeq("s", "x", 1, 1, 10, 1, 50, True).next_value, 51)
        self.assertEqual(PgSeq("s", "x", 1, 1, 10, 7, None, False).next_value, 7)

    def test_refuses_values_postgresql_cannot_hold_and_different_increments(self):
        rows = by_oracle(match([OracleSeq("SEQA", "SMALL", 3_000_000_000, 1), OracleSeq("SEQA", "STEP", 10, 2)],
                               [pg_seq("small", max_value=2 ** 31 - 1), pg_seq("step")], [], SCHEMAS))
        self.assertEqual(rows["SEQA.SMALL"].action, "refused")
        self.assertIn("range", rows["SEQA.SMALL"].reason)
        self.assertEqual(rows["SEQA.STEP"].action, "refused")
        self.assertIn("increment", rows["SEQA.STEP"].reason)

    def test_refuses_when_login_may_not_change_it(self):
        seq = pg_seq("locked")
        seq.can_update = False
        rows = match([OracleSeq("SEQA", "LOCKED", 9, 1)], [seq], [], SCHEMAS)
        self.assertEqual(rows[0].action, "refused")

    def test_identity_is_matched_by_table_and_column_not_name(self):
        links = [Link("seqa", "hist", "history_id", "seqa", "hist_history_id_seq", "identity")]
        rows = match([OracleSeq("SEQA", "ISEQ$$_645901", 30, 1, "HIST", "HISTORY_ID")],
                     [pg_seq("hist_history_id_seq", 1)], links, SCHEMAS)
        self.assertEqual((rows[0].kind, rows[0].pg, rows[0].feeds, rows[0].action),
                         ("identity", "seqa.hist_history_id_seq", "seqa.hist.history_id", "set"))

    def test_identity_falls_back_to_the_column_default(self):
        links = [Link("seqa", "hist", "history_id", "seqa", "named_seq", "default")]
        rows = match([OracleSeq("SEQA", "ISEQ$$_1", 30, 1, "HIST", "HISTORY_ID")], [pg_seq("named_seq")], links, SCHEMAS)
        self.assertEqual((rows[0].pg, rows[0].action), ("seqa.named_seq", "set"))
        self.assertIn("column default", rows[0].reason)

    def test_one_side_only_is_reported_and_left_alone(self):
        oracle = [OracleSeq("SEQA", "GONE", 7, 1), OracleSeq("SEQA", "ISEQ$$_2", 5, 1),
                  OracleSeq("SEQA", "ISEQ$$_3", 5, 1, "NO_TABLE", "ID")]
        rows = by_oracle(match(oracle, [pg_seq("extra")], [], SCHEMAS))
        self.assertEqual(rows["SEQA.GONE"].action, "oracle only")
        self.assertIn("dropped", rows["SEQA.ISEQ$$_2"].reason)
        self.assertEqual(rows["SEQA.ISEQ$$_3"].action, "oracle only")
        self.assertEqual(rows["seqa.extra"].action, "postgresql only")

    def test_unlinked_sequence_with_the_usual_name_is_pointed_out(self):
        rows = by_oracle(match([OracleSeq("SEQA", "ISEQ$$_5", 5, 1, "HIST", "HISTORY_ID")],
                               [pg_seq("hist_history_id_seq")], [], SCHEMAS))
        self.assertEqual(rows["SEQA.ISEQ$$_5"].action, "oracle only")
        self.assertIn("seqa.hist_history_id_seq exists but is not linked", rows["SEQA.ISEQ$$_5"].reason)

    def test_iseq_named_postgresql_leftover_is_not_matched_by_name(self):
        rows = by_oracle(match([OracleSeq("SEQA", "ISEQ$$_9", 5, 1, "T", "ID")], [pg_seq("iseq$$_9")], [], SCHEMAS))
        self.assertEqual(rows["SEQA.ISEQ$$_9"].action, "oracle only")
        self.assertEqual(rows["seqa.iseq$$_9"].action, "postgresql only")

    def test_two_oracle_sequences_on_one_postgresql_sequence_are_refused(self):
        links = [Link("seqa", "t", "id", "seqa", "shared", "default")]
        rows = match([OracleSeq("SEQA", "SHARED", 10, 1), OracleSeq("SEQA", "ISEQ$$_4", 99, 1, "T", "ID")],
                     [pg_seq("shared")], links, SCHEMAS)
        self.assertEqual({r.action for r in rows}, {"refused"})


class FromData(unittest.TestCase):
    def row(self, seq):
        return Row("postgresql only", pg=seq.label, pg_next=seq.next_value, action="postgresql only", target=seq)

    def test_behind_its_data_is_set_to_highest_plus_one(self):
        row = self.row(pg_seq("own_seq", 5))
        decide_from_data(row, 40)
        self.assertEqual((row.action, row.data_next, row.goal), ("set", 41, 41))

    def test_left_alone_when_empty_past_or_a_marker_row(self):
        for seq, highest, words in ((pg_seq("a", 5), None, "empty"), (pg_seq("b", 500), 40, "already past"),
                                    (pg_seq("c", 41), 40, "already past"), (pg_seq("d", 5), 999999999, "marker")):
            row = self.row(seq)
            decide_from_data(row, highest)
            self.assertEqual(row.action, "postgresql only", words)
            self.assertIn(words, row.reason)

    def test_refused_outside_range_or_without_privilege(self):
        row = self.row(pg_seq("small", 1, max_value=2 ** 31 - 1))
        decide_from_data(row, 2 ** 31 - 1)
        self.assertEqual(row.action, "refused")
        locked = pg_seq("locked", 1)
        locked.can_update = False
        row = self.row(locked)
        decide_from_data(row, 40)
        self.assertEqual(row.action, "refused")

    def test_descending_counts_down(self):
        row = self.row(pg_seq("down", -1, -1, min_value=-2 ** 63, max_value=-1))
        decide_from_data(row, -30)
        self.assertEqual((row.action, row.data_next), ("set", -31))


class CheckRules(unittest.TestCase):
    def test_labels(self):
        self.assertEqual(check_label(None, 5, 1)[0], "ok")
        self.assertEqual(check_label(50, 51, 1)[0], "ok")
        self.assertEqual(check_label(51, 51, 1)[0], "needs a look")
        self.assertEqual(check_label(6621, 1585, 1)[0], "needs a look")
        for marker in (999999, 999999999, 9999999991, 9999999999):
            self.assertEqual(check_label(marker, 133047, 1)[0], "placeholder", marker)
        self.assertEqual(check_label(-30, -21, -1)[0], "needs a look")
        self.assertEqual(check_label(-5, -21, -1)[0], "ok")

    def test_links_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "links.txt"
            path.write_text("# comment\nseqa.order_seq seqb.second.x  # shared\nbad line\nother.s other.t.c\n")
            links, problems = read_links_file(path, {"seqa", "seqb"})
        self.assertEqual([(l.seq_label, l.column_label) for l in links], [("seqa.order_seq", "seqb.second.x")])
        self.assertEqual(len(problems), 1)
        self.assertEqual(read_links_file(Path("/nonexistent"), set()), ([], []))


def sql(statement):
    r = subprocess.run([PSQL, "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-h", "localhost", "-p", PGPORT,
                        "-d", DB, "-c", statement], capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(r.stderr)
    return r.stdout.strip()


def database_available():
    try:
        return sql("SHOW server_version_num") >= "180000"
    except (RuntimeError, OSError):
        return False


DDL = """
DROP SCHEMA IF EXISTS seqa CASCADE; DROP SCHEMA IF EXISTS seqb CASCADE;
CREATE SCHEMA seqa; CREATE SCHEMA seqb;
CREATE SEQUENCE seqa.order_seq;
CREATE TABLE seqa.orders (order_id bigint DEFAULT nextval('seqa.order_seq') PRIMARY KEY);
INSERT INTO seqa.orders VALUES (1), (2), (50);
CREATE SEQUENCE seqa.ahead_seq START 500;
CREATE TABLE seqa.hist (history_id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY);
INSERT INTO seqa.hist VALUES (1), (29);
CREATE SEQUENCE seqa.small_seq AS integer;
CREATE SEQUENCE seqa.down_seq INCREMENT -1;
CREATE SEQUENCE seqa.pg_only_seq;
CREATE SEQUENCE seqb.tok_seq;
CREATE TABLE seqb.tokens (id bigint DEFAULT nextval('seqb.tok_seq'), note text);
INSERT INTO seqb.tokens (id) VALUES (10), (999999999);
CREATE SEQUENCE seqb.bank_seq;
CREATE TABLE seqb.bank (bank_id int DEFAULT nextval('seqb.bank_seq'));
INSERT INTO seqb.bank VALUES (5), (6621);
CREATE TABLE seqb.second (x bigint);
INSERT INTO seqb.second VALUES (70);
CREATE TABLE seqb.own (own_id bigint GENERATED BY DEFAULT AS IDENTITY, note text);
INSERT INTO seqb.own (own_id) VALUES (3), (40);
"""
ORACLE = [
    ["SEQA", "ORDER_SEQ", "51", "1", None, None],
    ["SEQA", "AHEAD_SEQ", "100", "1", None, None],
    ["SEQA", "ISEQ$$_1", "30", "1", "HIST", "HISTORY_ID"],
    ["SEQA", "ISEQ$$_2", "5", "1", None, None],
    ["SEQA", "SMALL_SEQ", "3000000000", "1", None, None],
    ["SEQA", "DOWN_SEQ", "-21", "-1", None, None],
    ["SEQA", "MISSING_SEQ", "7", "1", None, None],
    ["SEQB", "TOK_SEQ", "11", "1", None, None],
    ["SEQB", "BANK_SEQ", "1585", "1", None, None],
]


def conf_text(schema):
    return "\n".join(["ORACLE_DSN  dbi:Oracle:host=fake", f"SCHEMA  {schema}", "TYPE  COPY", "ALLOW  T1",
                      f"PG_DSN  dbi:Pg:dbname={DB};host=localhost;port={PGPORT}", f"PG_USER  {os.environ.get('USER')}",
                      "PG_PWD  unused", f"PG_SCHEMA  {schema.lower()}"]) + "\n"


@unittest.skipUnless(database_available(), f"no PostgreSQL 18 with database {DB} on localhost:{PGPORT}")
class EndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="dataload_seq_"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.addCleanup(sql, "DROP SCHEMA IF EXISTS seqa CASCADE; DROP SCHEMA IF EXISTS seqb CASCADE;")
        sql(DDL)
        for schema in SCHEMAS:
            (self.tmp / "content" / schema).mkdir(parents=True)
            (self.tmp / "content" / schema / "ora2pg_set_1.conf").write_text(conf_text(schema))
        (self.tmp / "sequence_links.txt").write_text("seqa.order_seq seqb.second.x\n")
        self.oracle = self.tmp / "oracle.json"
        self.oracle.write_text(json.dumps(ORACLE))
        self.write_settings(f"localhost:{PGPORT}/{DB}")
        self.env = {**os.environ, "PYTHONPATH": str(ROOT), "FAKE_ORA_SEQUENCES": str(self.oracle),
                    "FAKE_ORA_SEQUENCES_LATER": str(self.tmp / "oracle_later.json")}

    def write_settings(self, expected):
        (self.tmp / "dataload.toml").write_text(f"""
[paths]
content = "content"
[tools]
psql = "{PSQL}"
oracle_rows = "{ROOT / 'tests' / 'fake' / 'oracle_rows'}"
[target]
expected = "{expected}"
""")

    def sequences(self, *args):
        r = subprocess.run([sys.executable, "-m", "dataload.sequences", "--config", str(self.tmp / "dataload.toml"),
                            *args], env=self.env, capture_output=True, text=True, cwd=self.tmp, stdin=subprocess.DEVNULL)
        self.output = r.stdout + r.stderr
        return r.returncode

    def next_value(self, name):
        return int(sql(f"SELECT CASE WHEN is_called THEN last_value + {-1 if 'down' in name else 1} "
                       f"ELSE last_value END FROM pg_get_sequence_data('{name}')"))

    def values(self):
        return {name: self.next_value(name) for name in (
            "seqa.order_seq", "seqa.ahead_seq", "seqa.hist_history_id_seq", "seqa.small_seq", "seqa.down_seq",
            "seqa.pg_only_seq", "seqb.tok_seq", "seqb.bank_seq", "seqb.own_own_id_seq")}

    def test_plan_changes_nothing(self):
        before = self.values()
        self.assertEqual(self.sequences("plan"), 0, self.output)
        self.assertEqual(self.values(), before)
        for expected in ("9 sequences (7 named, 2 identity)", "would be set", "only on Oracle",
                         "seqa.small_seq: Oracle's value is outside PostgreSQL's range", "Nothing was changed"):
            self.assertIn(expected, self.output)
        csv_files = list((self.tmp / "logs").glob("sequences-plan-*.csv"))
        self.assertEqual(len(csv_files), 1)
        text = csv_files[0].read_text()
        self.assertIn("seqa.pg_only_seq", text)
        self.assertIn("no number column is linked to it", text)
        self.assertIn("set from its data (highest 40)", text)

    def test_apply_needs_yes_without_a_terminal(self):
        before = self.values()
        self.assertEqual(self.sequences("apply"), 2, self.output)
        self.assertIn("pass --yes", self.output)
        self.assertEqual(self.values(), before)

    def test_apply_sets_what_is_behind_and_never_moves_back(self):
        self.assertEqual(self.sequences("apply", "--yes"), 1, self.output)   # 1: the check found rows to look at
        self.assertEqual(self.values(), {
            "seqa.order_seq": 51, "seqa.ahead_seq": 500, "seqa.hist_history_id_seq": 30, "seqa.small_seq": 1,
            "seqa.down_seq": -21, "seqa.pg_only_seq": 1, "seqb.tok_seq": 11, "seqb.bank_seq": 1585,
            "seqb.own_own_id_seq": 41})
        self.assertIn("6 of 6 sequences set", self.output)
        self.assertRegex(self.output, r"of which only on PostgreSQL\s+1")
        self.assertIn("seqb.bank.bank_id (seqb.bank_seq): highest 6621, sequence next 1585", self.output)
        self.assertIn("seqb.second.x (seqa.order_seq): highest 70, sequence next 51", self.output)
        self.assertRegex(self.output, r"placeholder\s+1")
        self.assertNotIn("WARNING", self.output)
        sql("SELECT nextval('seqa.order_seq')")
        self.assertEqual(self.sequences("apply", "--yes"), 1, self.output)
        self.assertIn("Nothing to set", self.output)
        self.assertEqual(self.next_value("seqa.order_seq"), 52)

    def test_oracle_still_written_to_is_reported_and_a_second_run_catches_up(self):
        later = [row[:] for row in ORACLE]
        later[0][2] = "60"
        (self.tmp / "oracle_later.json").write_text(json.dumps(later))
        self.assertEqual(self.sequences("apply", "--yes"), 1, self.output)
        self.assertIn("1 Oracle sequences moved while this ran", self.output)
        self.assertIn("SEQA.ORDER_SEQ: 51 -> 60", self.output)
        self.assertEqual(self.next_value("seqa.order_seq"), 51)
        self.sequences("apply", "--yes")
        self.assertEqual(self.next_value("seqa.order_seq"), 60)

    def test_wrong_target_does_nothing(self):
        self.write_settings("elsewhere:5432/other")
        before = self.values()
        self.assertEqual(self.sequences("apply", "--yes"), 2, self.output)
        self.assertIn("dataload.toml expects elsewhere:5432/other", self.output)
        self.assertEqual(self.values(), before)

    def apply_csv(self):
        return str(sorted((self.tmp / "logs").glob("sequences-apply-*.csv"))[-1])

    def test_undo_puts_back_what_apply_set_unless_used_since(self):
        before = self.values()
        self.sequences("apply", "--yes")
        sql("SELECT nextval('seqa.order_seq')")                     # used since the apply: must stay
        self.assertEqual(self.sequences("undo", self.apply_csv()), 2, self.output)
        self.assertIn("pass --yes", self.output)
        self.assertEqual(self.sequences("undo", self.apply_csv(), "--yes"), 1, self.output)
        self.assertIn("5 of 5 sequences put back", self.output)
        self.assertIn("SKIPPED seqa.order_seq: used or moved since the apply (next was 51, is now 52)", self.output)
        expected = dict(before, **{"seqa.order_seq": 52})
        self.assertEqual(self.values(), expected)
        self.assertTrue(list((self.tmp / "logs").glob("sequences-undo-*.csv")))

    def test_undo_refuses_another_targets_csv(self):
        self.sequences("apply", "--yes")
        path = Path(self.apply_csv())
        path.write_text(path.read_text().replace(f"localhost:{PGPORT}/{DB}", "elsewhere:5432/other"))
        before = self.values()
        self.assertEqual(self.sequences("undo", str(path), "--yes"), 2, self.output)
        self.assertIn("was written for elsewhere:5432/other", self.output)
        self.assertEqual(self.values(), before)

    def test_check_alone(self):
        self.assertEqual(self.sequences("check"), 1, self.output)
        self.assertIn("seqa.orders.order_id (seqa.order_seq): highest 50, sequence next 1", self.output)
        self.assertTrue(list((self.tmp / "logs").glob("sequences-check-*.csv")))


if __name__ == "__main__":
    unittest.main()
