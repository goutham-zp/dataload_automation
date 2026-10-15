"""End-to-end tests against a local PostgreSQL 18, with tests/fake/ora2pg standing in for Oracle.

They build their own small content folder; server_content is only used as the source of one
patched run_*.sh, which is copied and run against the local database.

    DATALOAD_TEST_PGPORT (default 54318), DATALOAD_TEST_PGBIN (default Homebrew postgresql@18)
"""
import getpass
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PGBIN = Path(os.environ.get("DATALOAD_TEST_PGBIN", "/opt/homebrew/opt/postgresql@18/bin"))
PGPORT = os.environ.get("DATALOAD_TEST_PGPORT", "54318")
PSQL = str(PGBIN / "psql")
DB = "dataload_test"
USER = getpass.getuser()
REAL_SCRIPT = ROOT / "server_content" / "FTS" / "SAC_DATA" / "run_SAC_DATA.sh"

DDL = """
DROP SCHEMA IF EXISTS alpha CASCADE; DROP SCHEMA IF EXISTS beta CASCADE;
CREATE SCHEMA alpha; CREATE SCHEMA beta;
CREATE TABLE alpha.t1 (id int CONSTRAINT t1_id_not_null NOT NULL, CONSTRAINT t1_pk PRIMARY KEY (id));
CREATE TABLE alpha.t2 (id int CONSTRAINT t2_id_not_null NOT NULL, CONSTRAINT t2_pk PRIMARY KEY (id));
CREATE TABLE alpha.t3 (id int CONSTRAINT t3_id_not_null NOT NULL, CONSTRAINT t3_pk PRIMARY KEY (id));
CREATE TABLE alpha.big (id int CONSTRAINT big_id_not_null NOT NULL, CONSTRAINT big_pk PRIMARY KEY (id));
CREATE INDEX t2_idx ON alpha.t2 USING btree (id);
CREATE TABLE beta.b1 (id int CONSTRAINT b1_id_not_null NOT NULL, t1_id int DEFAULT 1,
    CONSTRAINT b1_pk PRIMARY KEY (id), CONSTRAINT b1_t1_fk FOREIGN KEY (t1_id) REFERENCES alpha.t1(id));
"""
GENERATED = {
    "alpha": {
        "PK_drop_script.sql": [f"ALTER TABLE alpha.{t} DROP CONSTRAINT IF EXISTS {t}_pk;" for t in ("big", "t3", "t2", "t1")],
        "PK_recreate_script.sql": [f"ALTER TABLE alpha.{t} ADD CONSTRAINT {t}_pk PRIMARY KEY (id);" for t in ("t1", "t2", "t3", "big")],
        "Other_drop_script.sql": [f"ALTER TABLE alpha.{t} DROP CONSTRAINT IF EXISTS {t}_id_not_null;" for t in ("big", "t3", "t2", "t1")],
        "Other_recreate_script.sql": [f"ALTER TABLE alpha.{t} ADD CONSTRAINT {t}_id_not_null NOT NULL id;" for t in ("t1", "t2", "t3", "big")],
        "all_indexes_drop.sql": ["DROP INDEX IF EXISTS alpha.t2_idx;"],
        "all_indexes_create.sql": ["CREATE INDEX t2_idx ON alpha.t2 USING btree (id);"],
    },
    "beta": {
        "FK_drop_script.sql": ["ALTER TABLE beta.b1 DROP CONSTRAINT IF EXISTS b1_t1_fk;"],
        "FK_recreate_script.sql": ["ALTER TABLE beta.b1 ADD CONSTRAINT b1_t1_fk FOREIGN KEY (t1_id) REFERENCES alpha.t1(id);"],
        "PK_drop_script.sql": ["ALTER TABLE beta.b1 DROP CONSTRAINT IF EXISTS b1_pk;"],
        "PK_recreate_script.sql": ["ALTER TABLE beta.b1 ADD CONSTRAINT b1_pk PRIMARY KEY (id);"],
        "Other_drop_script.sql": ["ALTER TABLE beta.b1 DROP CONSTRAINT IF EXISTS b1_id_not_null;"],
        "Other_recreate_script.sql": ["ALTER TABLE beta.b1 ADD CONSTRAINT b1_id_not_null NOT NULL id;"],
    },
}
SOURCE = {"ALPHA.T1": 10, "ALPHA.T2": 20, "ALPHA.T3": 5, "ALPHA.BIG": 100, "BETA.B1": 8}
CHUNKS = ["ID BETWEEN 1 AND 25", "ID BETWEEN 26 AND 50", "ID BETWEEN 51 AND 75", "ID BETWEEN 76 AND 9999"]
CONSTRAINTS_SQL = ("SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace "
                   "WHERE n.nspname IN ('alpha', 'beta')")


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


def conf_text(schema, tables=None):
    lines = ["ORACLE_DSN  dbi:Oracle:host=fake", f"SCHEMA  {schema}", "TYPE  COPY"]
    if tables:
        lines += ["TRUNCATE_TABLE  1", "PG_TRUNCATE_OPTION CASCADE",
                  f"ALLOW  {' '.join(tables)}", "", f"TABLE  {' '.join(tables)}"]
    lines += [f"PG_DSN  dbi:Pg:dbname={DB};host=localhost;port={PGPORT}", f"PG_USER  {USER}", "PG_PWD  unused",
              f"PG_SCHEMA  {schema.lower()}"]
    return "\n".join(lines) + "\n"


@unittest.skipUnless(database_available(), f"no PostgreSQL 18 with database {DB} on localhost:{PGPORT}")
@unittest.skipUnless(shutil.which("parallel"), "GNU parallel not installed")
class EndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="dataload_e2e_"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        sql(DDL)
        content = self.tmp / "content"
        (content / "ALPHA" / "BIG").mkdir(parents=True)
        (content / "BETA").mkdir()
        (content / "schema_list.txt").write_text("alpha\nbeta\nalpha\nnot_here\n")
        # Written out of order on purpose: set_2 must still run after set_1.
        (content / "ALPHA" / "ora2pg_set_2.conf").write_text(conf_text("ALPHA", ["T3"]))
        (content / "ALPHA" / "ora2pg_set_1.conf").write_text(conf_text("ALPHA", ["T1", "T2"]))
        (content / "ALPHA" / "BIG" / "ora2pg_BIG.conf").write_text(conf_text("ALPHA"))
        (content / "ALPHA" / "BIG" / "chunks.txt").write_text("\n".join(CHUNKS) + "\n")
        script = REAL_SCRIPT.read_text()
        script = script.replace("ora2pg_SAC_DATA.conf", "ora2pg_BIG.conf").replace('TABLE_NAME="SAC_DATA"', 'TABLE_NAME="BIG"')
        script = re.sub(r"PARALLEL_JOBS=\d+", "PARALLEL_JOBS=2", script)
        (content / "ALPHA" / "BIG" / "run_BIG.sh").write_text(script)
        (content / "BETA" / "ora2pg_set_1.conf").write_text(conf_text("BETA", ["B1"]))
        for schema, files in GENERATED.items():
            folder = content / "generated_scripts_for_all_schema" / f"{schema}_generated_scripts"
            folder.mkdir(parents=True)
            for name, statements in files.items():
                (folder / name).write_text("\n".join(statements) + "\n")
        (self.tmp / "dataload.toml").write_text(f"""
[paths]
content = "content"
[tools]
psql = "{PSQL}"
oracle_count = "{ROOT / 'tests' / 'fake' / 'oracle_count'}"
[target]
expected = "localhost:{PGPORT}/{DB}"
[retry]
attempts = 3
backoff_seconds = 0
[preflight]
perl_dbd_pg = false
[email]
enabled = false
outbox = "outbox"
""")
        self.faults = self.tmp / "faults.json"
        self.calls = self.tmp / "calls.log"
        self.set_faults([])
        self.calls.write_text("")
        (self.tmp / "source.json").write_text(json.dumps(SOURCE))
        self.env = {**os.environ, "PATH": f"{ROOT / 'tests' / 'fake'}:{os.environ['PATH']}",
                    "PYTHONPATH": str(ROOT), "FAKE_PSQL": PSQL, "FAKE_ORA_SOURCE": str(self.tmp / "source.json"),
                    "FAKE_ORA_FAULTS": str(self.faults), "FAKE_ORA_CALLS": str(self.calls)}
        self.all_constraints = int(sql(CONSTRAINTS_SQL))

    # ----- helpers -----

    def set_faults(self, faults):
        self.faults.write_text(json.dumps(faults))

    def argv(self, *args):
        return [sys.executable, "-m", "dataload", "--config", str(self.tmp / "dataload.toml"), *args]

    def dataload(self, *args):
        r = subprocess.run(self.argv(*args), env=self.env, capture_output=True, text=True, cwd=self.tmp)
        self.output = r.stdout + r.stderr
        return r.returncode

    def count(self, table):
        return int(sql(f"SELECT count(*) FROM {table}"))

    def loaded_tables(self):
        return [line.split("|")[0] for line in self.calls.read_text().splitlines()]

    def subjects(self):
        files = sorted((self.tmp / "outbox").glob("*.txt")) if (self.tmp / "outbox").exists() else []
        return [f.read_text().splitlines()[0].replace("Subject: [dataload] ", "") for f in files]

    def assert_everything_loaded(self):
        for name, rows in SOURCE.items():
            self.assertEqual(self.count(name.lower()), rows, name)
        self.assertEqual(self.count("(SELECT DISTINCT id FROM alpha.big) d"), 100)
        self.assertEqual(int(sql(CONSTRAINTS_SQL)), self.all_constraints)
        self.assertEqual(sql("SELECT count(*) FROM pg_indexes WHERE indexname = 't2_idx'"), "1")

    # ----- tests -----

    def test_clean_run_loads_everything_and_restores_constraints(self):
        self.assertEqual(self.dataload("run"), 0, self.output)
        self.assert_everything_loaded()
        tables = self.loaded_tables()
        self.assertEqual(tables[:3], ["T1", "T2", "T3"])
        self.assertEqual(tables.count("BIG"), 4)
        self.assertEqual(tables[-1], "B1")
        self.assertEqual(self.subjects(), ["Phase finished: drop", "Phase finished: load",
                                           "Phase finished: recreate", "Run finished: all done"])
        for expected in ("load step 1 of 4: ALPHA/ora2pg_set_1.conf, 2 of 2 tables to do",
                         "ALPHA/ora2pg_set_1.conf: copying 2 table(s) with ora2pg (T1, T2)",
                         "ALPHA/ora2pg_set_1.conf: T1: oracle=10 postgres=10 equal",
                         "ALPHA/BIG/run_BIG.sh: running 4 chunk(s) of BIG",
                         "4 of 4 chunks finished"):
            self.assertIn(expected, self.output)
        self.assertEqual(self.dataload("status", "--counts"), 0, self.output)
        self.assertIn("5 table(s), 5 compared after the load, 0 different", self.output)
        self.assertRegex(self.output, r"ALPHA\.BIG \(filtered\)\s+100\s+0\s+100\s+100")
        html = (self.tmp / "report.html").read_text()
        self.assertIn("Completed, all tables match", html)
        self.assertEqual(html.count('<span class="pill ok">Match</span>'), 5)
        self.assertIn("4 chunks", html)
        self.dataload("report")
        import sqlite3
        db = sqlite3.connect(self.tmp / "state" / "run.sqlite")
        sizes = db.execute("SELECT tbl, pg_bytes_loaded, pg_bytes_after FROM snapshot").fetchall()
        self.assertTrue(all(loaded and after for _, loaded, after in sizes), sizes)
        t1 = [row for row in sizes if row[0] == "T1"][0]
        self.assertLess(t1[1], t1[2], "the size after load is taken before the primary key index is rebuilt")
        self.assertEqual(self.dataload("resume"), 0, self.output)
        self.assertEqual(self.loaded_tables(), tables, "a resume of a finished run must load nothing")

    def test_before_counts_are_taken_first_and_kept_across_a_resume(self):
        sql("INSERT INTO alpha.t3 SELECT generate_series(1, 3)")  # rows left by an earlier load
        self.set_faults([{"match": "T2", "mode": "fatal", "times": 99}])
        self.assertEqual(self.dataload("run"), 1, self.output)
        self.assertLess(self.output.index("before-load snapshot took"), self.output.index("drop/"))
        self.assertIn("5 table(s), 143 rows to load from Oracle, 3 rows in PostgreSQL now", self.output)
        html = (self.tmp / "report.html").read_text()
        self.assertIn("Needs attention", html)
        self.assertIn("<strong>ALPHA.T2</strong>: Failed", html)
        self.set_faults([])
        self.assertEqual(self.dataload("resume"), 0, self.output)
        self.assertNotIn("before-load snapshot", self.output, "a resume must not recount what it already recorded")
        self.dataload("status", "--counts")
        self.assertRegex(self.output, r"ALPHA\.T3\s+5\s+3\s+5\s+5", "the before count is the one from before the load")
        self.assertEqual(self.dataload("report"), 0, self.output)

    def test_a_failed_before_count_stops_the_run_before_anything_changes(self):
        self.env["FAKE_COUNT_FAIL"] = "T3"
        self.assertEqual(self.dataload("run"), 1, self.output)
        self.assertIn("the before-load count failed for 1 table(s), so nothing was changed", self.output)
        self.assertEqual(self.loaded_tables(), [])
        self.assertEqual(int(sql(CONSTRAINTS_SQL)), self.all_constraints)
        del self.env["FAKE_COUNT_FAIL"]
        self.assertEqual(self.dataload("resume"), 0, self.output)
        self.assert_everything_loaded()

    def test_test_count_method_still_works(self):
        toml = self.tmp / "dataload.toml"
        toml.write_text(toml.read_text() + '\n[validate]\nmethod = "test_count"\n')
        self.assertEqual(self.dataload("run"), 0, self.output)
        self.assertIn("(ora2pg TEST_COUNT)", self.output)
        self.assert_everything_loaded()
        self.dataload("status", "--counts")
        self.assertIn("5 compared after the load, 0 different", self.output)

    def test_transient_chunk_failure_is_retried_without_duplicate_rows(self):
        self.set_faults([{"match": "ID BETWEEN 26 AND 50", "mode": "transient", "times": 1}])
        self.assertEqual(self.dataload("run"), 0, self.output)
        self.assert_everything_loaded()
        self.assertEqual(self.loaded_tables().count("BIG"), 5)
        self.assertNotIn("Step failed: ALPHA/BIG/run_BIG.sh", self.subjects())

    def test_transient_table_failure_reloads_only_that_table(self):
        self.set_faults([{"match": "T2", "mode": "transient", "times": 1}])
        self.assertEqual(self.dataload("run"), 0, self.output)
        self.assert_everything_loaded()
        self.assertEqual(self.loaded_tables()[:3], ["T1", "T2", "T2"])

    def test_persistent_failure_blocks_recreate_and_resume_does_only_what_failed(self):
        self.set_faults([{"match": "T2", "mode": "fatal", "times": 99},
                         {"match": "ID BETWEEN 51 AND 75", "mode": "fatal", "times": 99}])
        self.assertEqual(self.dataload("run"), 1, self.output)
        self.assertEqual(self.count("alpha.t3"), 5, "later steps still load after a failure")
        self.assertEqual(self.count("beta.b1"), 8)
        self.assertEqual(sql("SELECT count(*) FROM pg_constraint WHERE conname = 't1_pk'"), "0",
                         "constraints must not be recreated on an incomplete load")
        subjects = self.subjects()
        self.assertIn("Step failed: ALPHA/ora2pg_set_1.conf", subjects)
        self.assertIn("Step failed: ALPHA/BIG/run_BIG.sh", subjects)
        self.assertEqual(subjects[-1], "Run finished: NEEDS ATTENTION")
        self.assertEqual(self.dataload("status", "--failed"), 0)
        self.assertIn("duplicate key", self.output)
        self.assertIn("Key (id)=(4471882) already exists", self.output, "the full error stays on the machine")
        mailed = "".join(f.read_text() for f in (self.tmp / "outbox").glob("*.txt"))
        self.assertIn("duplicate key", mailed)
        self.assertIn("Key (id)=(***) already exists", mailed)
        self.assertNotIn("4471882", mailed)
        self.assertNotIn("4471882", (self.tmp / "report.html").read_text())

        self.set_faults([])
        self.calls.write_text("")
        self.assertEqual(self.dataload("resume"), 0, self.output)
        self.assertEqual(sorted(self.loaded_tables()), ["BIG", "T2"])
        self.assertIn("|ID BETWEEN 51 AND 75", self.calls.read_text())
        self.assert_everything_loaded()
        self.assertIn("Run resumed", self.subjects())
        self.assertEqual(self.subjects()[-1], "Run finished: all done")

    def test_error_with_exit_code_zero_is_caught_by_row_count(self):
        self.set_faults([{"match": "T3", "mode": "exit0", "times": 1}])
        self.assertEqual(self.dataload("run"), 1, self.output)
        self.dataload("status", "--failed")
        self.assertIn("row count mismatch: oracle=5 postgres=2", self.output)
        self.assertEqual(self.dataload("resume"), 0, self.output)
        self.assert_everything_loaded()

    def test_killed_mid_chunk_resumes_cleanly(self):
        self.set_faults([{"match": "ID BETWEEN 51 AND 75", "mode": "hang", "times": 1}])
        proc = subprocess.Popen(self.argv("run"), env=self.env, cwd=self.tmp, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        deadline = time.time() + 60
        while "ID BETWEEN 51 AND 75" not in self.calls.read_text() and time.time() < deadline:
            time.sleep(0.2)
        time.sleep(1.5)  # let the hanging chunk write its partial rows
        proc.send_signal(signal.SIGTERM)
        out, _ = proc.communicate(timeout=60)
        self.assertEqual(proc.returncode, 130, out)
        self.assertGreater(self.count("alpha.big"), 0)
        self.assertEqual(self.dataload("resume"), 0, self.output)
        self.assert_everything_loaded()
        calls = self.calls.read_text()
        self.assertEqual(calls.count("|ID BETWEEN 51 AND 75"), 2, "the interrupted chunk runs again")
        self.assertEqual(calls.count("BIG|"), 5, "chunks that had finished before the stop are not repeated")

    def test_chunk_lines_written_by_dataload_chunks_load_every_row_once(self):
        chunk_file = self.tmp / "content" / "ALPHA" / "BIG" / "chunks.txt"
        # As make writes them: from the lowest ID, each next one at the end plus one, the last to the column's ceiling.
        chunk_file.write_text("ID BETWEEN 1 AND 30\nID BETWEEN 31 AND 60\nID BETWEEN 61 AND 90\nID BETWEEN 91 AND 9999999999\n")
        self.assertEqual(self.dataload("run"), 0, self.output)
        self.assert_everything_loaded()
        self.assertEqual(self.count("alpha.big"), 100, "no row twice")
        self.assertIn("run_BIG.sh: BIG: oracle=100 postgres=100 equal", (self.tmp / "logs" / "dataload.log").read_text())
        calls = self.calls.read_text()
        for line in chunk_file.read_text().splitlines():
            self.assertIn(f"BIG|{line}\n", calls)

    def test_resume_refuses_a_chunk_file_that_changed_since_the_run_started(self):
        self.set_faults([{"match": "ID BETWEEN 76 AND 9999", "mode": "fatal", "times": 99}])
        self.assertNotEqual(self.dataload("run"), 0, self.output)
        chunk_file = self.tmp / "content" / "ALPHA" / "BIG" / "chunks.txt"
        original = chunk_file.read_text()
        chunk_file.write_text("ID <= 60\nID > 60\n")
        calls = self.calls.read_text()
        self.assertEqual(self.dataload("resume"), 2, self.output)
        self.assertIn("resume refused: the chunk file of ALPHA/BIG/run_BIG.sh changed", self.output)
        self.assertEqual(self.calls.read_text(), calls, "nothing was loaded")
        self.set_faults([])
        chunk_file.write_text(original)
        self.assertEqual(self.dataload("resume"), 0, self.output)
        self.assert_everything_loaded()

    def test_status_tells_a_stop_from_a_failure(self):
        self.set_faults([{"match": "T2", "mode": "fatal", "times": 99},
                         {"match": "ID BETWEEN 51 AND 75", "mode": "hang", "times": 1}])
        proc = subprocess.Popen(self.argv("run"), env=self.env, cwd=self.tmp, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        deadline = time.time() + 60
        while "ID BETWEEN 51 AND 75" not in self.calls.read_text() and time.time() < deadline:
            time.sleep(0.2)
        time.sleep(1)
        self.dataload("status")
        self.assertIn("running now", self.output)
        self.assertRegex(self.output, r"Running now \(\d\)\n  ALPHA/BIG/run_BIG.sh: \d chunk\(s\), log ")
        proc.send_signal(signal.SIGINT)
        proc.communicate(timeout=60)
        self.dataload("status")
        self.assertIn("not running (it stopped before the end)", self.output)
        self.assertIn("Failed (1): fix the cause, then run 'resume'\n  ALPHA/ora2pg_set_1.conf\n    table:     T2", self.output)
        self.assertIn("Stopped part-way (1): nothing to fix, 'resume' does these again\n  ALPHA/BIG/run_BIG.sh: 1 chunk(s)",
                      self.output)
        self.assertIn("Next: fix the failures above", self.output)
        outbox = "".join(f.read_text() for f in (self.tmp / "outbox").glob("*.txt"))
        self.assertNotIn("stopped part-way", outbox, "nothing was mailed after the stop")
        self.set_faults([])
        self.assertEqual(self.dataload("resume"), 0, self.output)
        self.dataload("status")
        self.assertIn("Nothing is waiting.", self.output)

    def test_copy_that_lost_its_connection_is_stopped_and_retried(self):
        toml = self.tmp / "dataload.toml"
        toml.write_text(toml.read_text().replace("backoff_seconds = 0", "backoff_seconds = 0\nstall_minutes = 0.05"))
        self.set_faults([{"match": "T3", "mode": "hang", "times": 1},
                         {"match": "ID BETWEEN 51 AND 75", "mode": "hang", "times": 1}])
        self.assertEqual(self.dataload("run"), 0, self.output)
        self.assertIn("ALPHA/ora2pg_set_2.conf copy: dataload: stalled.", self.output)
        self.assertIn("ALPHA/BIG/run_BIG.sh: dataload: stalled.", self.output)
        self.assertIn("retry 2/3", self.output)
        self.assert_everything_loaded()
        calls = self.calls.read_text()
        self.assertEqual(calls.count("T3|"), 2)
        self.assertEqual(calls.count("BIG|"), 5, "only the stalled chunk is copied again")
        left = subprocess.run(["pgrep", "-fl", str(self.tmp)], capture_output=True, text=True).stdout
        self.assertEqual(left, "")

    def test_stop_leaves_no_loader_process_behind(self):
        # T3 stands for an ora2pg reader in the middle of a fetch, which does not act on a polite stop.
        self.set_faults([{"match": "T3", "mode": "stubborn", "times": 1}])
        proc = subprocess.Popen(self.argv("run"), env=self.env, cwd=self.tmp, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        deadline = time.time() + 60
        while "T3|" not in self.calls.read_text() and time.time() < deadline:
            time.sleep(0.2)
        time.sleep(1)
        proc.send_signal(signal.SIGINT)
        out, _ = proc.communicate(timeout=60)
        self.assertEqual(proc.returncode, 130, out)
        left = subprocess.run(["pgrep", "-fl", str(self.tmp)], capture_output=True, text=True).stdout
        self.assertEqual(left, "", "nothing may keep copying after a stop")
        self.assertEqual(self.dataload("resume"), 0, self.output)
        self.assert_everything_loaded()

    def test_chunk_reported_failed_but_fully_loaded_counts_as_done(self):
        self.set_faults([{"match": "ID BETWEEN 26 AND 50", "mode": "falsefail", "times": 1}])
        self.assertEqual(self.dataload("run"), 0, self.output)
        self.assertIn("both hold 25 rows in that range, so it is counted as done", self.output)
        self.assertEqual(self.loaded_tables().count("BIG"), 4, "the chunk is not loaded a second time")
        self.assert_everything_loaded()

    def test_hard_kill_mid_chunk_keeps_the_chunks_that_finished(self):
        self.set_faults([{"match": "ID BETWEEN 51 AND 75", "mode": "hang", "times": 1}])
        proc = subprocess.Popen(self.argv("run"), env=self.env, cwd=self.tmp, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        log = None
        deadline = time.time() + 60
        while time.time() < deadline:   # wait until the three other chunks are reported as finished
            logs = list((self.tmp / "logs").glob("ALPHA__BIG__run_BIG.sh.*.log")) if (self.tmp / "logs").exists() else []
            if logs and logs[0].read_text(errors="replace").count("[SUCCESS] Finished") == 3:
                log = logs[0]
                break
            time.sleep(0.2)
        self.assertIsNotNone(log, "the other chunks never finished")
        proc.kill()                     # SIGKILL: the tool gets no chance to record anything
        proc.communicate(timeout=60)
        subprocess.run(["pkill", "-f", str(self.tmp)])  # the orphaned hanging chunk
        time.sleep(1)
        self.assertEqual(self.dataload("resume"), 0, self.output)
        self.assert_everything_loaded()
        calls = self.calls.read_text()
        self.assertEqual(calls.count("|ID BETWEEN 51 AND 75"), 2)
        self.assertEqual(calls.count("BIG|"), 5, "only the chunk that was still copying is repeated")

    def test_skipping_fk_drop_refuses_to_truncate(self):
        self.assertEqual(self.dataload("run", "--skip-constraints"), 1, self.output)
        self.assertIn("foreign key(s) still reference", self.output)
        self.assertEqual(self.loaded_tables(), [])
        self.assertEqual(int(sql(CONSTRAINTS_SQL)), self.all_constraints)
        self.assertEqual(self.dataload("resume"), 1, "a plain resume keeps the flags of the run")
        self.assertIn("continuing with the flags this run was started with: --skip-drop --skip-recreate", self.output)
        self.assertEqual(self.loaded_tables(), [])
        self.assertEqual(self.dataload("resume", "--no-skip"), 0, self.output)
        self.assert_everything_loaded()

    def test_skip_constraints_leaves_constraints_and_indexes_alone(self):
        sql("ALTER TABLE beta.b1 DROP CONSTRAINT b1_t1_fk")  # dropped by hand, as the flag assumes
        self.all_constraints -= 1
        self.assertEqual(self.dataload("run", "--skip-constraints"), 0, self.output)
        self.assert_everything_loaded()
        self.dataload("status")
        self.assertRegex(self.output, r"drop\s+0 / 12\s+skipped by flag")
        self.assertRegex(self.output, r"recreate\s+0 / 12\s+skipped by flag")
        self.assertIn("Flags     --skip-drop --skip-recreate", self.output)
        self.assertNotIn("drop/alpha", self.output, "skipped steps are not listed")
        self.assertEqual(self.subjects(), ["Phase finished: load", "Run finished: all done"])
        outbox = sorted((self.tmp / "outbox").glob("*.txt"))[-1].read_text()
        self.assertIn("drop      skipped by flag", outbox)

    def test_skip_drop_and_skip_recreate_work_on_their_own(self):
        sql("ALTER TABLE beta.b1 DROP CONSTRAINT b1_t1_fk")  # by hand, so the load may truncate
        self.assertEqual(self.dataload("run", "--skip-drop"), 0, self.output)
        self.assertNotIn("] drop/", self.output)
        self.assertIn("recreate/beta/FK: done", self.output, "recreate still runs and restores the FK")
        self.assert_everything_loaded()
        sql("TRUNCATE alpha.big")  # chunked tables are not truncated by the load, so a second run needs this
        self.assertEqual(self.dataload("run", "--new", "--skip-recreate"), 0, self.output)
        self.assertIn("drop/beta/FK: done", self.output)
        self.assertNotIn("] recreate/", self.output)
        self.assertEqual(sql("SELECT count(*) FROM pg_constraint WHERE conname IN ('t1_pk', 'b1_t1_fk')"), "0")
        self.assertEqual(self.dataload("resume", "--no-skip"), 0, self.output)
        self.assertEqual(int(sql(CONSTRAINTS_SQL)), self.all_constraints, "a later resume recreates them")

    def test_manual_drop_beforehand_is_harmless(self):
        sql("ALTER TABLE beta.b1 DROP CONSTRAINT b1_t1_fk; DROP INDEX alpha.t2_idx;")
        self.assertEqual(self.dataload("run"), 0, self.output)
        self.assert_everything_loaded()

    # ----- lanes -----

    LANES = {"big": ["ALPHA/BIG/run_BIG.sh"], "rest": ["*"]}

    def use_lanes(self, enabled=True, **lanes):
        lines = ["", "[lanes]", f"enabled = {str(enabled).lower()}"]
        lines += [f"{name} = {json.dumps(steps)}" for name, steps in lanes.items()]
        toml = self.tmp / "dataload.toml"
        toml.write_text(toml.read_text() + "\n".join(lines) + "\n")

    def test_lanes_run_side_by_side_and_recreate_waits_for_all_of_them(self):
        self.use_lanes(**self.LANES)
        self.set_faults([{"match": "ID BETWEEN 1 AND 25", "mode": "slow", "times": 1}])
        self.assertEqual(self.dataload("plan"), 0, self.output)
        self.assertIn("== load lanes: 2, side by side ==", self.output)
        self.assertIn("rest: 3 steps", self.output)
        self.assertEqual(self.dataload("run"), 0, self.output)
        self.assert_everything_loaded()
        out = self.output
        self.assertIn("load runs in 2 lanes side by side: big (1 step), rest (3 steps)", out)
        self.assertIn("rest: step 1 of 3: ALPHA/ora2pg_set_1.conf, 2 of 2 tables to do", out)
        self.assertIn("big: step 1 of 1: ALPHA/BIG/run_BIG.sh, 4 of 4 chunks to do", out)
        self.assertLess(out.index("rest: finished"), out.index("big: finished"), "the small steps waited for the chunks")
        self.assertLess(out.index("big: finished"), out.index("phase load finished"))
        self.assertLess(out.index("phase load finished"), out.index("recreate/"))
        self.assertEqual(self.subjects(), ["Phase finished: drop", "Phase finished: load",
                                           "Phase finished: recreate", "Run finished: all done"])
        self.dataload("status")
        self.assertRegex(self.output, r"big\s+ALPHA/BIG/run_BIG\.sh\s+4 / 4\s+chunks\s+done")
        self.assertRegex(self.output, r"rest\s+BETA/ora2pg_set_1\.conf\s+1 / 1\s+tables\s+done")

    def test_a_failure_in_one_lane_leaves_the_other_lanes_running(self):
        self.use_lanes(**self.LANES)
        self.set_faults([{"match": "T2", "mode": "fatal", "times": 99}])
        self.assertEqual(self.dataload("run"), 1, self.output)
        self.assertEqual(self.count("(SELECT DISTINCT id FROM alpha.big) d"), 100)
        self.assertEqual(self.count("alpha.t3"), 5, "the lane with the failure carries on with its next step")
        self.assertEqual(self.count("beta.b1"), 8)
        self.assertIn("rest: finished in", self.output)
        self.assertIn(", 1 unit(s) failed", self.output)
        self.assertIn("recreate NOT started", self.output)
        self.assertIn("Step failed: ALPHA/ora2pg_set_1.conf", self.subjects())
        self.set_faults([])
        self.calls.write_text("")
        self.assertEqual(self.dataload("resume"), 0, self.output)
        self.assertEqual(self.loaded_tables(), ["T2"])
        self.assert_everything_loaded()

    def test_stop_halts_every_lane_and_resume_finishes_them(self):
        self.use_lanes(**self.LANES)
        self.set_faults([{"match": "T3", "mode": "hang", "times": 1},
                         {"match": "ID BETWEEN 51 AND 75", "mode": "hang", "times": 1}])
        proc = subprocess.Popen(self.argv("run"), env=self.env, cwd=self.tmp, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        deadline = time.time() + 60
        while time.time() < deadline:
            calls = self.calls.read_text()
            if "T3|" in calls and "|ID BETWEEN 51 AND 75" in calls:
                break
            time.sleep(0.2)
        time.sleep(1.5)  # both lanes are now in the middle of a copy
        proc.send_signal(signal.SIGINT)
        out, _ = proc.communicate(timeout=60)
        self.assertEqual(proc.returncode, 130, out)
        left = subprocess.run(["pgrep", "-fl", str(self.tmp)], capture_output=True, text=True).stdout
        self.assertEqual(left, "", "nothing may keep copying in any lane after a stop")
        self.assertEqual(self.dataload("resume"), 0, self.output)
        self.assert_everything_loaded()
        calls = self.calls.read_text()
        self.assertEqual(calls.count("T3|"), 2)
        self.assertEqual(calls.count("|ID BETWEEN 51 AND 75"), 2)
        self.assertEqual(calls.count("T1|"), 1, "steps that had finished are not repeated")

    def test_a_table_in_two_lanes_stops_the_run_before_anything_changes(self):
        (self.tmp / "content" / "ALPHA" / "ora2pg_set_2.conf").write_text(conf_text("ALPHA", ["T3", "T1"]))
        self.use_lanes(a=["ALPHA/ora2pg_set_1.conf"], b=["*"])
        self.assertEqual(self.dataload("run"), 2, self.output)
        self.assertIn("preflight FAILED: alpha.t1 is loaded by ALPHA/ora2pg_set_1.conf in lane a", self.output)
        self.assertEqual(self.loaded_tables(), [])
        self.assertEqual(int(sql(CONSTRAINTS_SQL)), self.all_constraints)
        self.assertEqual(self.dataload("plan"), 2, self.output)
        self.assertIn("alpha.t1 is loaded by", self.output)

    def test_lanes_never_load_beside_a_foreign_key(self):
        self.use_lanes(**self.LANES)
        self.assertEqual(self.dataload("run", "--skip-constraints", "--allow-remaining-fks"), 1, self.output)
        self.assertIn("with lanes on, --allow-remaining-fks is not accepted", self.output)
        self.assertEqual(self.loaded_tables(), [])

    def test_lanes_listed_but_switched_off_run_one_step_at_a_time(self):
        self.use_lanes(enabled=False, **self.LANES)
        self.assertEqual(self.dataload("run"), 0, self.output)
        self.assert_everything_loaded()
        self.assertIn("load step 1 of 4: ALPHA/ora2pg_set_1.conf, 2 of 2 tables to do", self.output)
        self.assertNotIn("side by side", self.output)
        self.assertEqual(self.loaded_tables()[:3], ["T1", "T2", "T3"])
        self.assertEqual(self.loaded_tables()[-1], "B1")

    def test_second_run_is_refused_and_wrong_target_stops_preflight(self):
        toml = self.tmp / "dataload.toml"
        good = toml.read_text()
        toml.write_text(good.replace(f'expected = "localhost:{PGPORT}/{DB}"', 'expected = ""'))
        self.assertEqual(self.dataload("run"), 2)
        self.assertIn("expected is not set", self.output)
        self.assertEqual(self.dataload("resume"), 2)
        self.assertIn("nothing to resume", self.output)
        toml.write_text(good)
        self.assertEqual(self.dataload("run"), 0, "a run that stopped at preflight must not block the next run")
        self.assertEqual(self.dataload("run"), 2)
        self.assertIn("Use 'resume'", self.output)
        toml = self.tmp / "dataload.toml"
        toml.write_text(toml.read_text().replace(f"localhost:{PGPORT}/{DB}", "prod.example.net:5900/x"))
        self.assertEqual(self.dataload("run", "--new"), 2)
        self.assertIn("expects prod.example.net:5900/x", self.output)


if __name__ == "__main__":
    unittest.main()
