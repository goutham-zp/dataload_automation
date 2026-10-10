"""Tests that need no database. The plan tests only read server_content; they run nothing in it."""
import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path

from dataload import counts, plan
from dataload.conf import OraConf
from dataload.redact import redact
from dataload.config import Config
from dataload.runner import Engine, Flags, first_error, human, last_output_line, parse_chunk_output, parse_test_count

ROOT = Path(__file__).resolve().parents[1]
CONTENT = ROOT / "server_content"


def real_config():
    return Config(root=ROOT, content=CONTENT, state=ROOT / "x", logs=ROOT / "x", work=ROOT / "x")


class ConfOrder(unittest.TestCase):
    def test_numbered_confs_in_number_order_then_unnumbered(self):
        names = ["ora2pg_set_10.conf", "ora2pg_a_return_data.conf", "ora2pg_set_2.conf", "ora2pg_set_1.conf"]
        ordered = sorted((Path(n) for n in names), key=plan.conf_sort_key)
        self.assertEqual([p.name for p in ordered],
                         ["ora2pg_set_1.conf", "ora2pg_set_2.conf", "ora2pg_set_10.conf", "ora2pg_a_return_data.conf"])


@unittest.skipUnless(CONTENT.is_dir(), "server_content not present")
class RealPlan(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.steps = plan.build(real_config())
        cls.loads = [s for s in cls.steps if s.phase == "load"]

    def test_fts_confs_run_in_number_order(self):
        fts = [s.id for s in self.loads if s.id.startswith("FTS/") and s.kind == "conf"]
        self.assertEqual(fts, [f"FTS/ora2pg_set_{i}.conf" for i in range(1, 9)] + ["FTS/ora2pg_a_return_data.conf"])

    def test_folders_with_a_script_run_the_script_not_the_conf(self):
        chunked = {s.id for s in self.loads if s.kind == "chunk"}
        self.assertEqual(chunked, {
            "FAST/EMAIL_TO_SEND_ARCHIVE_1/run_EMAIL_TO_SEND_ARCHIVE.sh", "FTS/A_EDI_DATA/run_A_EDI_DATA_1.sh",
            "FTS/A_EDI_SUMMASY/run_A_EDI_SUMMARY.sh", "FTS/A_EFT_PROCESSED/run_A_EFT_PROCESSED.sh",
            "FTS/SAC_DATA/run_SAC_DATA.sh"})
        ids = {s.id for s in self.loads}
        self.assertNotIn("FTS/A_EDI_DATA/ora2pg_A_EDI_DATA.conf", ids)
        self.assertTrue(all(s.chunks_overridable for s in self.loads if s.kind == "chunk"))

    def test_schema_order_follows_schema_list(self):
        seen = list(dict.fromkeys(s.schema for s in self.loads))
        self.assertEqual(seen[:4], ["fts", "fast", "ft", "fast_track"])

    def test_only_limits_loads_and_sql_to_that_folder(self):
        cfg = real_config()
        cfg.only = ["SFTP_CONFIG"]
        self.assertEqual({s.schema for s in plan.build(cfg)}, {"sftp_config"})

    def test_every_table_is_planned_once(self):
        tables = [f"{s.schema}.{t}" for s in self.loads if s.kind == "conf" for t in s.units]
        self.assertEqual(len(tables), 831)
        self.assertEqual(len(set(tables)), 831)

    def test_drop_and_recreate_orders(self):
        drops = [s.sql_kind for s in self.steps if s.phase == "drop" and s.schema == "fts"]
        recreates = [s.sql_kind for s in self.steps if s.phase == "recreate" and s.schema == "fts"]
        self.assertEqual(drops, ["FK", "PK", "Other", "indexes"])
        self.assertEqual(recreates, ["Other", "PK", "indexes", "FK"])
        all_drops = [s.sql_kind for s in self.steps if s.phase == "drop"]
        self.assertLess(max(i for i, k in enumerate(all_drops) if k == "FK"), all_drops.index("PK"),
                        "every schema's foreign keys go before any primary key")
        all_recreates = [s.sql_kind for s in self.steps if s.phase == "recreate"]
        self.assertGreater(all_recreates.index("FK"), max(i for i, k in enumerate(all_recreates) if k != "FK"))
        fk_drop = next(s for s in self.steps if s.id == "drop/fts/FK")
        self.assertEqual(len(fk_drop.units), 118)


class Parsing(unittest.TestCase):
    def test_derive_narrows_tables_and_leaves_original_alone(self):
        with tempfile.TemporaryDirectory() as d:
            src = Path(d) / "a.conf"
            src.write_text("SCHEMA FTS\nALLOW  A B C\n\nTABLE  A B C\nPG_SCHEMA FTS\n#TABLE x\n")
            out = OraConf(src).derive(Path(d) / "w" / "b.conf", ["B"])
            self.assertEqual(OraConf(out).tables(), ["B"])
            self.assertEqual(OraConf(out).get("ALLOW"), "B")
            self.assertEqual(OraConf(src).tables(), ["A", "B", "C"])
            self.assertEqual(OraConf(src).pg_schema(), "fts")

    def test_target_from_dsn(self):
        with tempfile.TemporaryDirectory() as d:
            src = Path(d) / "a.conf"
            src.write_text("PG_DSN          dbi:Pg:dbname=ftxe01;host=h.example.net;port=5900\nPG_USER u\nPG_PWD p@1\n")
            self.assertEqual(OraConf(src).target(), "h.example.net:5900/ftxe01")
            self.assertEqual(OraConf(src).pg_env()["PGPASSWORD"], "p@1")

    def test_test_count_output(self):
        text = "[TEST ROWS COUNT]\nORACLEDB:T1:10\nPOSTGRES:t1:10\nORACLEDB:T2:20\nPOSTGRES:fts.t2:7\n[ERRORS ROWS COUNT]\n"
        self.assertEqual(parse_test_count(text), {"T1": (10, 10), "T2": (20, 7)})

    def test_chunk_output_keeps_exact_conditions(self):
        text = ("\r 50% 1:1=0s\r[2026-01-01 10:00:00] Starting chunk: ID BETWEEN 1  AND  5\n"
                "[SUCCESS] Finished: ID BETWEEN 1  AND  5\n"
                "[ERROR] Failed: ID BETWEEN 6  AND  9. Review logs at: ./migration_logs/chunk_x_1.log\n")
        done, failed = parse_chunk_output(text)
        self.assertEqual(done, {"ID BETWEEN 1  AND  5"})
        self.assertEqual(failed, {"ID BETWEEN 6  AND  9": "./migration_logs/chunk_x_1.log"})

    def test_first_error_finds_real_log_lines(self):
        self.assertIn("ORA-08177", first_error("progress\rERROR: ORA-08177: can't serialize access\nAborting export..."))
        self.assertEqual(first_error("[====>] 10/10 rows (100.0%) Table X"), "")

    def test_split_statements(self):
        self.assertEqual(plan.split_statements("A;\n\nB\n  C;\n-- note\nD;\n"), ["A;", "B\n  C;", "D;"])

    def test_test_count_output_of_real_ora2pg(self):
        # Copied from a real ora2pg 25.0 run: the PostgreSQL line carries the schema.
        text = "[TEST ROWS COUNT]\nORACLEDB:X_DIVISION_STORES:76596\nPOSTGRES:fts.x_division_stores:76596\n"
        self.assertEqual(parse_test_count(text), {"X_DIVISION_STORES": (76596, 76596)})


class Counting(unittest.TestCase):
    def test_where_directive(self):
        self.assertEqual(counts.where_filters("A_RETURN_DATA[FTS_RETURN_ID NOT IN (4262, 2462)]"),
                         {"A_RETURN_DATA": "FTS_RETURN_ID NOT IN (4262, 2462)"})
        self.assertEqual(counts.where_filters("T1[A = 1] T2[B IN (SELECT X FROM Y WHERE Z[1] = 2)]"),
                         {"T1": "A = 1", "T2": "B IN (SELECT X FROM Y WHERE Z[1] = 2)"})
        self.assertEqual(counts.where_filters("ROWNUM < 10"), {"*": "ROWNUM < 10"})
        self.assertEqual(counts.where_filters(""), {})

    def test_real_conf_filter_reaches_the_oracle_count(self):
        cfg = real_config()
        steps = {s.id: s for s in plan.load_steps(cfg)}
        with tempfile.TemporaryDirectory() as tmp:
            cfg.logs = cfg.work = Path(tmp)
            engine = Engine(cfg, None, Flags())
            spec = engine.specs_for(steps["FTS/ora2pg_a_return_data.conf"])[0]
            self.assertEqual(spec.oracle_count_sql(),
                             'SELECT COUNT(1) FROM "FTS"."A_RETURN_DATA" WHERE FTS_RETURN_ID NOT IN (4262, 2462)')
            self.assertEqual(spec.pg_count_sql(), 'SELECT count(1) FROM "fts"."a_return_data"')
            chunked = engine.specs_for(steps["FTS/SAC_DATA/run_SAC_DATA.sh"])[0]
            self.assertEqual(chunked.filter.count(" OR "), 9)
            self.assertIn(" WHERE (FTS_SAC_ID BETWEEN", chunked.pg_count_sql())

    def test_sizes_read_well(self):
        self.assertEqual([counts.human_bytes(n) for n in (None, 512, 1536, 5 * 1024 ** 3)], ["", "512 B", "1.5 KB", "5.0 GB"])


class Redaction(unittest.TestCase):
    def test_values_are_hidden_and_the_error_is_kept(self):
        cases = {
            "FATAL: ERROR:  Account Number: 412718071   | Substring: 4127 | Error Message : function public.digest(bytea, unknown) does not exist":
                "FATAL: ERROR:  Account Number: ***   | Substring: *** | Error Message : function public.digest(bytea, unknown) does not exist",
            'ERROR:  duplicate key value violates unique constraint "x_pkey" DETAIL:  Key (id, email)=(42, a.b@example.com) already exists.':
                'ERROR:  duplicate key value violates unique constraint "x_pkey" DETAIL:  Key (id, email)=(***) already exists.',
            'ERROR: null value in column "name" violates not-null constraint DETAIL:  Failing row contains (17, null, Jane Doe, 1 Main St).':
                'ERROR: null value in column "name" violates not-null constraint DETAIL:  Failing row contains (***).',
            'ERROR:  invalid input syntax for type integer: "12a-secret" CONTEXT:  COPY r_company, line 88, column tax_id: "12a-secret"':
                'ERROR:  invalid input syntax for type integer: "***" CONTEXT:  COPY r_company, line 88, column tax_id: "***"',
            "contact jane.doe@example.com about it": "contact *** about it",
        }
        for text, expected in cases.items():
            self.assertEqual(redact(text), expected)

    def test_codes_counts_and_names_are_left_alone(self):
        for text in ("FATAL: ORA-03114: not connected to ORACLE",
                     "FATAL: 12545 ... ORA-12545: Connect failed"[13:],
                     "row count mismatch: oracle=436889 postgres=0",
                     'relation "fts.a_eft_processed_p20260101" does not exist',
                     "PL/pgSQL function mask_account_number(text) line 18 at RAISE",
                     "ora2pg exit code 2", ""):
            self.assertEqual(redact(text), text)
        self.assertIsNone(redact(None))


class FriendlyStartErrors(unittest.TestCase):
    def run_cli(self, *argv):
        from dataload.cli import main
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = main(list(argv))
        return code, out.getvalue()

    def test_missing_settings_file_is_explained(self):
        with tempfile.TemporaryDirectory() as tmp:
            code, out = self.run_cli("--config", f"{tmp}/dataload.toml", "plan")
        self.assertEqual(code, 2)
        self.assertIn("Settings file not found", out)
        self.assertIn("Run this from the folder that holds dataload.toml", out)

    def test_deleted_current_folder_is_explained(self):
        before = os.getcwd()
        gone = tempfile.mkdtemp()
        os.chdir(gone)
        os.rmdir(gone)
        try:
            code, out = self.run_cli("plan")
        finally:
            os.chdir(before)
        self.assertEqual(code, 2)
        self.assertIn("no longer exists", out)
        self.assertNotIn("Traceback", out)


class Progress(unittest.TestCase):
    def test_human_durations(self):
        self.assertEqual([human(5), human(125), human(3725)], ["5s", "2m 05s", "1h 02m"])

    def test_last_output_line_drops_timestamp_and_control_codes(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "x.log"
            log.write_text("[2026-10-10 06:16:47] first\n[====>  ] 10/20 rows (50.0%)\r"
                           "[2026-10-10 06:16:48] \x1b[0mExtracted records from table X\n\n")
            self.assertEqual(last_output_line(log), "[0mExtracted records from table X")
            self.assertEqual(last_output_line(Path(tmp) / "missing.log"), "")

    def test_long_command_reports_that_it_is_still_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            cfg = Config(root=tmp, content=tmp, state=tmp / "s", logs=tmp / "logs", work=tmp / "work", heartbeat=1)
            engine = Engine(cfg, None, Flags())
            lines = []
            engine.log = lines.append
            rc = engine.exec_logged(["bash", "-c", "echo copying rows; sleep 2.5"], tmp, tmp / "out.log", label="STEP copy")
            self.assertEqual(rc, 0)
            self.assertTrue(lines and lines[0].startswith("STEP copy: still running after 1s | copying rows"), lines)
            quiet = []
            engine.log = quiet.append
            engine.exec_logged(["bash", "-c", "sleep 1.5"], tmp, tmp / "out2.log")
            self.assertEqual(quiet, [], "no label means no progress lines")


if __name__ == "__main__":
    unittest.main()
