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

from dataload.sequences import (Link, Manual, OracleSeq, PgSeq, Row, SequenceError, check_label, decide_from_data,
                                ignored_by, link_problems, match, oracle_from_url, own_run, pg_from_url,
                                read_ignore_file, read_links_file, shared_default_groups)

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

    def test_matched_sequence_goes_past_its_data_not_back_to_oracle(self):
        row = Row("named", oracle="SEQA.TASK_SEQ", oracle_next=7, pg="seqa.task_seq", target=pg_seq("task_seq", 1),
                  action="set")
        decide_from_data(row, 14, "seqa.task.task_id")
        self.assertEqual((row.action, row.goal), ("set", 15))
        self.assertIn("set past its data: seqa.task.task_id holds up to 14, Oracle's value is 7", row.reason)
        row = Row("named", oracle="SEQA.A", oracle_next=50, pg="seqa.a", target=pg_seq("a", 50), action="set")
        decide_from_data(row, 20)
        self.assertEqual((row.action, row.goal), ("set", 50))

    def test_matched_sequence_past_oracle_but_behind_its_data_is_set(self):
        row = Row("named", oracle="SEQA.A", oracle_next=5, pg="seqa.a", target=pg_seq("a", 9), action="ahead")
        decide_from_data(row, 20)
        self.assertEqual((row.action, row.goal), ("set", 21))

    def test_matched_sequence_whose_data_is_out_of_range_keeps_oracles_value(self):
        row = Row("named", oracle="SEQA.A", oracle_next=5, pg="seqa.a",
                  target=pg_seq("a", 1, max_value=2 ** 31 - 1), action="set")
        decide_from_data(row, 2 ** 31 - 1, "seqa.t.id")
        self.assertEqual((row.action, row.goal), ("set", 5))
        self.assertEqual([m.problem for m in row.manual], ["outside the range"])


class OwnRun(unittest.TestCase):
    def test_rows_just_above_the_sequence_are_its_own(self):
        self.assertTrue(own_run(7, 14, 8, 7, 1))           # every number taken: it is simply behind
        self.assertTrue(own_run(1, 40, 2, 3, 1))           # starts low, fills 1 in 20
        self.assertTrue(own_run(70, 70, 1, 70, 1))         # the very next number is taken

    def test_a_far_or_sparse_block_is_left_to_a_person(self):
        self.assertFalse(own_run(1585, 6621, 1, 6621, 1))            # one value far above
        self.assertFalse(own_run(344427, 10020044, 45, 10020000, 1))  # a reserved block
        self.assertFalse(own_run(1, 10000000, 50, 1, 1))              # starts low but fills 1 in 200000

    def test_descending(self):
        self.assertTrue(own_run(-21, -30, 10, -21, -1))
        self.assertFalse(own_run(-21, -9000, 1, -9000, -1))


class Urls(unittest.TestCase):
    def test_postgresql_url(self):
        env, target = pg_from_url("postgresql://loader:p%40ss%3Aw%2Fd@pg-host:5900/ftxe01")
        self.assertEqual(env, {"PGHOST": "pg-host", "PGPORT": "5900", "PGDATABASE": "ftxe01", "PGUSER": "loader",
                               "PGPASSWORD": "p@ss:w/d"})
        self.assertEqual(target, "pg-host:5900/ftxe01")
        env, target = pg_from_url("postgres://loader@pg-host/ftxe01")
        self.assertEqual((env["PGPORT"], env["PGPASSWORD"], target), ("5432", "", "pg-host:5432/ftxe01"))

    def test_oracle_url(self):
        env, source = oracle_from_url("oracle://loader:p%40ss@ora-host:1522/FTXUAT")
        self.assertEqual(env, {"DL_ORA_DSN": "dbi:Oracle:host=ora-host;SERVICE_NAME=FTXUAT;port=1522",
                               "DL_ORA_USER": "loader", "DL_ORA_PWD": "p@ss"})
        self.assertEqual(source, "host=ora-host;SERVICE_NAME=FTXUAT;port=1522")
        self.assertEqual(oracle_from_url("oracle://u:p@ora-host/S")[0]["DL_ORA_DSN"],
                         "dbi:Oracle:host=ora-host;SERVICE_NAME=S;port=1521")

    def test_bad_url_is_refused_without_showing_the_password(self):
        for parse, url in ((pg_from_url, "mysql://u:secret1@h/db"), (pg_from_url, "postgresql://u:secret1@h/"),
                           (pg_from_url, "postgresql://u:secret1@/db"), (oracle_from_url, "oracle://u:secret1@h"),
                           (oracle_from_url, "oracle://u:secret1@h:port/S"), (oracle_from_url, "oracle://h:1521/S")):
            with self.assertRaises(SequenceError, msg=url) as caught:
                parse(url)
            self.assertNotIn("secret1", str(caught.exception))


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

    def test_link_problems(self):
        links = [Link("seqa", "t", "id", "seqa", "t_id_seq", "identity"),
                 Link("seqa", "v", "id", "seqa", "gone_seq", "links file")]
        items = link_problems(links, {"seqa.t_id_seq"})
        self.assertEqual([(m.problem, m.sequence, m.column) for m in items],
                         [("sequence not found", "seqa.gone_seq", "seqa.v.id")])

    def test_one_sequence_as_the_default_of_two_columns_of_a_table(self):
        links = [Link("seqa", "rev", "id", "seqa", "rev_seq", "default"),
                 Link("seqa", "rev", "fts_id", "seqa", "rev_seq", "default"),
                 Link("seqa", "rev", "id", "seqa", "rev_seq", "owned by"),
                 Link("seqa", "other", "id", "seqa", "rev_seq", "default"),
                 Link("seqa", "solo", "id", "seqa", "solo_seq", "default")]
        groups = shared_default_groups(links)
        self.assertEqual(list(groups), [("seqa.rev_seq", "seqa.rev")])
        self.assertEqual(sorted(l.column for l in groups[("seqa.rev_seq", "seqa.rev")]), ["fts_id", "id"])

    def test_ignore_file_needs_a_reason_for_every_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ignore.txt"
            path.write_text("# comment\nflyway_m.*  # Flyway's own\nfts.iseq$$_*  # leftovers\nfast.bank_seq\n")
            patterns, problems = read_ignore_file(path)
        self.assertEqual(patterns, [("flyway_m.*", "", "Flyway's own"), ("fts.iseq$$_*", "", "leftovers")])
        self.assertEqual(problems, ["ignore.txt line 4: no reason after #, so it is not used"])
        schema = Manual("1 sequence", "flyway_m", "schema not in the confs", "", "")
        leftover = Manual("fts.iseq$$_633070", "", "no column linked", "", "")
        stray = Manual("fast.bank_seq", "", "no column linked", "", "")
        self.assertEqual(ignored_by(schema, patterns), "Flyway's own")
        self.assertEqual(ignored_by(leftover, patterns), "leftovers")
        self.assertEqual(ignored_by(stray, patterns), "")
        self.assertEqual(read_ignore_file(Path("/nonexistent")), ([], []))

    def test_ignore_line_can_name_the_one_problem_it_covers(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ignore.txt"
            path.write_text("fast.email_to_send_seq  far ahead  # gap left by an earlier setval\n")
            patterns, problems = read_ignore_file(path)
        self.assertEqual((patterns, problems), ([("fast.email_to_send_seq", "far ahead", "gap left by an earlier setval")],
                                                []))
        ahead_item = Manual("fast.email_to_send_seq", "", "far ahead", "", "")
        collision = Manual("fast.email_to_send_seq", "fast.email_to_send.id", "needs a look", "", "")
        self.assertEqual(ignored_by(ahead_item, patterns), "gap left by an earlier setval")
        self.assertEqual(ignored_by(collision, patterns), "")          # a new problem still shows


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
DROP SCHEMA IF EXISTS seqa CASCADE; DROP SCHEMA IF EXISTS seqb CASCADE; DROP SCHEMA IF EXISTS seqc CASCADE;
CREATE SCHEMA seqa; CREATE SCHEMA seqb; CREATE SCHEMA seqc;
CREATE SEQUENCE seqc.not_in_any_conf_seq;
CREATE SEQUENCE seqa.task_seq;
CREATE TABLE seqa.task (task_id bigint DEFAULT nextval('seqa.task_seq'));
INSERT INTO seqa.task SELECT generate_series(1, 14);
CREATE TABLE seqb.mark (mark_id bigint GENERATED BY DEFAULT AS IDENTITY);
INSERT INTO seqb.mark VALUES (1), (2), (999999);
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
INSERT INTO seqb.second VALUES (5000);
CREATE SEQUENCE seqa.rev_seq;
CREATE TABLE seqa.reversal (silver_bullet_id bigint PRIMARY KEY DEFAULT nextval('seqa.rev_seq'),
                            fts_id bigint DEFAULT nextval('seqa.rev_seq'));
INSERT INTO seqa.reversal VALUES (1, 7238), (2, 339993);
CREATE SEQUENCE seqb.filter_seq;
CREATE TABLE seqb.r_filter (filter_id bigint PRIMARY KEY DEFAULT nextval('seqb.filter_seq'));
INSERT INTO seqb.r_filter SELECT generate_series(1, 10) UNION ALL SELECT 28;
CREATE SEQUENCE seqb.ref_seq;
CREATE TABLE seqb.ledger (id bigint PRIMARY KEY, ref_id bigint DEFAULT nextval('seqb.ref_seq'));
INSERT INTO seqb.ledger VALUES (1, 500), (2, 900);
CREATE SEQUENCE seqb.ftype_seq START 5;
CREATE SEQUENCE seqb.wrong_seq START 22;
CREATE TABLE seqb.ftype (ftype_id bigint DEFAULT nextval('seqb.wrong_seq'));
INSERT INTO seqb.ftype VALUES (1), (2), (3);
CREATE TABLE seqb.own (own_id bigint GENERATED BY DEFAULT AS IDENTITY, note text);
INSERT INTO seqb.own (own_id) VALUES (3), (40);
CREATE SEQUENCE seqa.company_seq START 10000050;
CREATE TABLE seqa.company (fts_id bigint PRIMARY KEY DEFAULT nextval('seqa.company_seq'));
INSERT INTO seqa.company SELECT generate_series(1, 40) UNION ALL SELECT generate_series(10000000, 10000014);
CREATE SEQUENCE seqa.dist_seq START 3;
CREATE TABLE seqa.dist (fts_id bigint PRIMARY KEY DEFAULT nextval('seqa.dist_seq') REFERENCES seqa.company,
                        silver_bullet_id bigint DEFAULT nextval('seqa.dist_seq'));
INSERT INTO seqa.dist VALUES (1, 1), (40, 2);
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
    ["SEQA", "TASK_SEQ", "7", "1", None, None],
    ["SEQA", "REV_SEQ", "3", "1", None, None],
    ["SEQB", "FILTER_SEQ", "20", "1", None, None],
    ["SEQA", "COMPANY_SEQ", "36", "1", None, None],
    ["SEQA", "DIST_SEQ", "3", "1", None, None],
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
        self.addCleanup(sql, "DROP SCHEMA IF EXISTS seqa CASCADE; DROP SCHEMA IF EXISTS seqb CASCADE; "
                             "DROP SCHEMA IF EXISTS seqc CASCADE;")
        sql(DDL)
        for schema in SCHEMAS:
            (self.tmp / "content" / schema).mkdir(parents=True)
            (self.tmp / "content" / schema / "ora2pg_set_1.conf").write_text(conf_text(schema))
        (self.tmp / "sequence_links.txt").write_text("seqa.order_seq seqb.second.x\nseqb.ftype_seq seqb.ftype.ftype_id\n")
        (self.tmp / "sequence_ignore.txt").write_text("seqc.*  # not migrated from Oracle\nseqa.pg_only_seq\n")
        self.oracle = self.tmp / "oracle.json"
        self.oracle.write_text(json.dumps(ORACLE))
        self.write_settings(f"localhost:{PGPORT}/{DB}")
        self.env = {**os.environ, "PYTHONPATH": str(ROOT), "FAKE_ORA_SEQUENCES": str(self.oracle),
                    "FAKE_ORA_SEQUENCES_LATER": str(self.tmp / "oracle_later.json")}

    def write_settings(self, expected, extra="", psql=PSQL):
        (self.tmp / "dataload.toml").write_text(f"""
[paths]
content = "content"
[tools]
psql = "{psql}"
oracle_rows = "{ROOT / 'tests' / 'fake' / 'oracle_rows'}"
[target]
expected = "{expected}"
{extra}""")

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
            "seqa.pg_only_seq", "seqb.tok_seq", "seqb.bank_seq", "seqb.own_own_id_seq", "seqa.task_seq",
            "seqb.mark_mark_id_seq", "seqa.rev_seq", "seqb.filter_seq", "seqb.ref_seq", "seqb.ftype_seq",
            "seqb.wrong_seq", "seqa.company_seq")}

    def test_plan_changes_nothing(self):
        before = self.values()
        self.assertEqual(self.sequences("plan"), 0, self.output)
        self.assertEqual(self.values(), before)
        for expected in ("14 sequences (12 named, 2 identity)", "would be set", "only on Oracle (2):",
                         "refused (1):\n    seqa.small_seq\n        why: Oracle's value is outside PostgreSQL's range",
                         "fix: ALTER SEQUENCE seqa.small_seq AS bigint", "far ahead (1):", "Nothing was changed"):
            self.assertIn(expected, self.output)
        self.assertTrue(list((self.tmp / "logs").glob("sequences-manual-*.csv")))
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
        self.assertEqual(self.sequences("apply", "--yes"), 1, self.output)   # 1: some need a person
        self.assertEqual(self.values(), {
            "seqa.order_seq": 51, "seqa.ahead_seq": 500, "seqa.hist_history_id_seq": 30, "seqa.small_seq": 1,
            "seqa.down_seq": -21, "seqa.pg_only_seq": 1, "seqb.tok_seq": 11, "seqb.bank_seq": 1585,
            "seqb.own_own_id_seq": 41, "seqa.task_seq": 15, "seqb.mark_mark_id_seq": 1, "seqa.rev_seq": 3,
            "seqb.filter_seq": 29, "seqb.ref_seq": 1, "seqb.ftype_seq": 5, "seqb.wrong_seq": 22,
            "seqa.company_seq": 10000050})
        self.assertIn("9 of 9 sequences set", self.output)
        self.assertRegex(self.output, r"of which only on PostgreSQL\s+1")
        self.assertRegex(self.output, r"of which past Oracle \(data\)\s+2")      # task_seq; filter_seq, a 9-number jump
        self.assertRegex(self.output, r"placeholder\s+1")
        self.assertNotIn("WARNING", self.output)
        for expected in (
                "seqb.bank_seq  ->  seqb.bank.bank_id\n        why: one value, 6621, is at or past its next value 1585",
                "it runs into 6621 after 5036 more inserts; if the column is meant to be unique that insert fails",
                "fix: if those IDs are the sequence's own: SELECT setval('seqb.bank_seq', 6622, false);",
                "seqa.order_seq  ->  seqb.second.x",
                "marker rows (1):\n    seqb.mark_mark_id_seq  ->  seqb.mark.mark_id",
                "SELECT setval('seqb.mark_mark_id_seq', 3, false);",
                "no column linked (1):\n    seqa.pg_only_seq",
                # one sequence as the default of the key and of a foreign key: only the latter is wrong
                "seqa.rev_seq  ->  seqa.reversal.fts_id, seqa.reversal.silver_bullet_id",
                "its next value is 3; fts_id highest 339993; silver_bullet_id highest 2 (key). Probably wrong on fts_id",
                "fix: ALTER TABLE seqa.reversal ALTER COLUMN fts_id DROP DEFAULT;",
                # the key is also a foreign key: its values come from the table it points at, so it is the wrong one
                "seqa.dist_seq  ->  seqa.dist.fts_id, seqa.dist.silver_bullet_id",
                "fts_id highest 40 (key); silver_bullet_id highest 2. Probably wrong on fts_id: a foreign key to "
                "seqa.company, so its values come from there",
                "fix: ALTER TABLE seqa.dist ALTER COLUMN fts_id DROP DEFAULT;",
                # a column that is not a key: no insert fails, the default is suspect
                "seqb.ref_seq  ->  seqb.ledger.ref_id\n        why: its values reach 900",
                "fix: ALTER TABLE seqb.ledger ALTER COLUMN ref_id DROP DEFAULT;",
                # two sequences on one column: the one just above the highest value
                "seqb.wrong_seq (default) next 22; the column's highest value is 3; closest above it: seqb.ftype_seq",
                "ALTER TABLE seqb.ftype ALTER COLUMN ftype_id SET DEFAULT nextval('seqb.ftype_seq');",
                # left far ahead by an earlier setval: listed, never lowered; its own rows go up to 40
                "far ahead (1):\n    seqa.company_seq  ->  seqa.company.fts_id\n        why: its next value 10000050 is "
                "10000009 numbers past where it should be, 41 (Oracle's value 36; seqa.company.fts_id holds its own "
                "rows up to 40); 15 values from 10000000 to 10000014 sit in between",
                "fix: a lowering, for the DBA and only if the team wants it: SELECT setval('seqa.company_seq', 41, "
                "false); it then runs into 10000000 after 9999959 inserts",
                "warning: sequence_ignore.txt line 2: no reason after #, so it is not used",
                "Needs a person: 12, each with the reason and a suggested fix (1 known item left off"):
            self.assertIn(expected, self.output)
        self.assertNotIn("schema not in the confs", self.output)
        manual = list((self.tmp / "logs").glob("sequences-manual-*.csv"))
        self.assertEqual(len(manual), 1)
        text = manual[0].read_text()
        self.assertEqual(len(text.splitlines()), 14)
        self.assertIn("not migrated from Oracle", text)
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

    def test_connections_from_env_file_win_over_the_confs(self):
        for schema in SCHEMAS:      # confs that point nowhere: only .env can make this work
            path = self.tmp / "content" / schema / "ora2pg_set_1.conf"
            path.write_text(path.read_text().replace(f"port={PGPORT}", "port=1"))
        (self.tmp / ".env").write_text(f"ORACLE_URL=oracle://seq%40reader:s3cret%23x@ora-host:1522/FTXUAT\n"
                                       f"PG_URL=postgresql://{os.environ.get('USER')}:unused@localhost:{PGPORT}/{DB}\n")
        seen = self.tmp / "seen.json"
        self.env["FAKE_ORA_SEEN"] = str(seen)
        self.assertEqual(self.sequences("plan"), 0, self.output)
        self.assertEqual(json.loads(seen.read_text()), ["dbi:Oracle:host=ora-host;SERVICE_NAME=FTXUAT;port=1522",
                                                        "seq@reader", "s3cret#x"])
        self.assertIn("Oracle host=ora-host;SERVICE_NAME=FTXUAT;port=1522 (ORACLE_URL in .env): 14 sequences",
                      self.output)
        self.assertIn(f"PostgreSQL localhost:{PGPORT}/{DB} (PG_URL in .env; the confs point at localhost:1/{DB})",
                      self.output)
        self.assertNotIn("s3cret", self.output)
        self.assertNotIn("unused", self.output)
        self.write_settings(f"localhost:1/{DB}")       # what the confs say: PG_URL wins, so it is refused
        self.assertEqual(self.sequences("plan"), 2, self.output)
        self.assertIn(f"PG_URL in .env points at localhost:{PGPORT}/{DB} but dataload.toml expects localhost:1/{DB}",
                      self.output)

    def behind_pgbouncer(self, fail_on=""):
        self.write_settings(f"localhost:{PGPORT}/{DB}", psql=ROOT / "tests" / "fake" / "psql_bouncer")
        self.env.update(REAL_PSQL=PSQL, FAKE_PSQL_FAIL_ON=fail_on)

    def test_reads_columns_through_pgbouncer(self):
        self.behind_pgbouncer()
        self.assertEqual(self.sequences("plan"), 0, self.output)
        self.assertNotIn("unsupported startup parameter", self.output)
        self.assertNotIn("column not read", self.output)
        self.assertIn("seqa.company.fts_id holds its own rows up to 40", self.output)    # the data was read

    def test_unread_columns_suggest_no_lowering_and_no_wrong_default(self):
        self.behind_pgbouncer(fail_on='"fts_id"')       # seqa.company.fts_id and seqa.reversal.fts_id
        self.assertEqual(self.sequences("plan"), 0, self.output)
        self.assertIn("column not read", self.output)
        self.assertIn("seqa.company_seq  ->  seqa.company.fts_id", self.output)
        self.assertNotIn("far ahead", self.output)
        self.assertIn("fts_id highest not read; silver_bullet_id highest 2 (key). Not judged: fts_id could not be read",
                      self.output)
        self.assertNotIn("ALTER TABLE seqa.reversal ALTER COLUMN fts_id DROP DEFAULT", self.output)

    def test_a_tie_between_two_sequences_names_neither(self):
        sql("ALTER SEQUENCE seqb.ftype_seq RESTART 22")             # now level with seqb.wrong_seq
        self.sequences("plan")
        self.assertIn("seqb.ftype_seq and seqb.wrong_seq are equally close (next 22): the values cannot tell which "
                      "one is used", self.output)
        self.assertIn("fix: find which one the application calls (nextval in its code or a trigger) and make that one "
                      "the column default", self.output)
        self.assertNotIn("closest above it", self.output)

    def test_sequence_raise_file_lets_apply_go_past_a_known_block(self):
        (self.tmp / "sequence_raise.txt").write_text("seqb.bank_seq  # 6621 was typed in by hand; skip past it\n"
                                                     "seqa.order_seq\n")
        self.assertEqual(self.sequences("apply", "--yes"), 1, self.output)
        self.assertEqual(self.next_value("seqb.bank_seq"), 6622)
        self.assertEqual(self.next_value("seqa.order_seq"), 51)        # its line has no reason: not used
        self.assertIn("warning: sequence_raise.txt line 2: no reason after #, so it is not used", self.output)
        self.assertNotIn("seqb.bank_seq  ->  seqb.bank.bank_id", self.output)
        self.assertIn("as sequence_raise.txt says (6621 was typed in by hand; skip past it)",
                      sorted((self.tmp / "logs").glob("sequences-apply-*.csv"))[-1].read_text())

    def test_far_ahead_only_past_ahead_warn_gap(self):
        self.write_settings(f"localhost:{PGPORT}/{DB}", "[sequences]\nahead_warn_gap = 20000000\n")
        self.assertEqual(self.sequences("plan"), 0, self.output)
        self.assertNotIn("far ahead", self.output)
        self.write_settings(f"localhost:{PGPORT}/{DB}", "[sequences]\nahead_warn_gap = 300\n")
        self.sequences("plan")
        self.assertIn("far ahead (2):", self.output)
        self.assertIn("seqa.ahead_seq\n        why: its next value 500 is 400 numbers past where it should be, 100 "
                      "(Oracle's value 100); nothing collides", self.output)

    def apply_csv(self):
        return str(sorted((self.tmp / "logs").glob("sequences-apply-*.csv"))[-1])

    def test_undo_puts_back_what_apply_set_unless_used_since(self):
        before = self.values()
        self.sequences("apply", "--yes")
        sql("SELECT nextval('seqa.order_seq')")                     # used since the apply: must stay
        self.assertEqual(self.sequences("undo", self.apply_csv()), 2, self.output)
        self.assertIn("pass --yes", self.output)
        self.assertEqual(self.sequences("undo", self.apply_csv(), "--yes"), 1, self.output)
        self.assertIn("8 of 8 sequences put back", self.output)
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
        self.assertIn("seqa.order_seq  ->  seqa.orders.order_id\n        why: rows from 1 to 50 already hold numbers",
                      self.output)
        self.assertIn("fix: SELECT setval('seqa.order_seq', 51, false); (apply does this on its own)", self.output)
        self.assertIn("(1 known item left off, as sequence_ignore.txt says)", self.output)
        self.assertTrue(list((self.tmp / "logs").glob("sequences-check-*.csv")))


if __name__ == "__main__":
    unittest.main()
