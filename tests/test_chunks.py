"""Tests for dataload.chunks, with tests/fake/oracle_query standing in for Oracle. No database is needed."""
import contextlib
import fcntl
import io
import json
import os
import re
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from dataload import chunks, config, plan
from datetime import datetime, timedelta

from dataload.chunks import BIGINT_MAX, DATE_CEILING, Chunker, ceiling_for, conditions, date_conditions, day_starts

ROOT = Path(__file__).resolve().parents[1]
REAL_SCRIPT = ROOT / "server_content" / "FTS" / "SAC_DATA" / "run_SAC_DATA.sh"


def holds(condition, value):
    """Whether a row with this ID falls in the chunk, for the BETWEEN lines chunks.py writes."""
    m = re.fullmatch(r"\w+ BETWEEN (\d+) AND (\d+)", condition)
    return int(m.group(1)) <= value <= int(m.group(2))


DATE_LINE = re.compile(r"(\w+) >= DATE '(\d{4}-\d\d-\d\d)' AND \1 < DATE '(\d{4}-\d\d-\d\d)'")


def holds_date(condition, value):
    """Whether a row with this DATE ("YYYY-MM-DD HH:MM:SS") falls in a half-open day chunk."""
    m = DATE_LINE.fullmatch(condition)
    return m.group(2) <= value < m.group(3)


def every_hours(count, hours, start="2024-01-01 00:00:00"):
    first = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
    return [f"{first + timedelta(hours=hours * i):%Y-%m-%d %H:%M:%S}" for i in range(count)]


class Conditions(unittest.TestCase):
    def test_one_chunk_runs_from_the_lowest_id_to_the_ceiling(self):
        self.assertEqual(conditions("ID", 3, [9], 99), ["ID BETWEEN 3 AND 99"])

    def test_each_chunk_starts_one_above_the_previous_end(self):
        self.assertEqual(conditions("ID", 3, [375, 750, 900], 9999999999),
                         ["ID BETWEEN 3 AND 375", "ID BETWEEN 376 AND 750", "ID BETWEEN 751 AND 9999999999"])

    def test_date_chunks_are_half_open_days_from_the_earliest_to_the_ceiling(self):
        self.assertEqual(date_conditions("D", "2024-01-01", []),
                         [f"D >= DATE '2024-01-01' AND D < DATE '{DATE_CEILING}'"])
        self.assertEqual(date_conditions("D", "2024-01-01", ["2024-02-01", "2024-03-05"]),
                         ["D >= DATE '2024-01-01' AND D < DATE '2024-02-01'",
                          "D >= DATE '2024-02-01' AND D < DATE '2024-03-05'",
                          f"D >= DATE '2024-03-05' AND D < DATE '{DATE_CEILING}'"])

    def test_a_chunk_closes_once_it_holds_enough_rows(self):
        days = [("d1", 3), ("d2", 3), ("d3", 10), ("d4", 1), ("d5", 1), ("d6", 1)]
        self.assertEqual(day_starts(days, 5), ["d3", "d4"])     # 6 rows, then the busy day alone, then the rest
        self.assertEqual(day_starts(days, 100), [])
        self.assertEqual(day_starts(days, 1), ["d2", "d3", "d4", "d5", "d6"])

    def test_ceiling_is_the_largest_value_the_column_holds(self):
        self.assertEqual(ceiling_for("10"), 9999999999)
        self.assertEqual(ceiling_for("9"), 999999999)
        self.assertEqual(ceiling_for(""), BIGINT_MAX)


class Chunks(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="dataload_chunks_"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        content = self.tmp / "content"
        (content / "ALPHA").mkdir(parents=True)
        (content / "ALPHA" / "ora2pg_set_1.conf").write_text(self.conf("ALPHA", "ALLOW  T1\nTABLE  T1\n"))
        self.add_chunked("ALPHA", "BIG", "chunks.txt", ["ID BETWEEN 1 AND 50", "ID BETWEEN 51 AND 9999"])
        self.add_chunked("ALPHA", "MAIL", "chunk.txt", ["MAIL_ID > 0"])
        (self.tmp / "dataload.toml").write_text(f"""
[paths]
content = "content"
[tools]
oracle_query = "{ROOT / 'tests' / 'fake' / 'oracle_query'}"
""")
        self.source = self.tmp / "oracle.json"
        self.queries = self.tmp / "queries.log"
        self.tables = {
            "ALPHA.BIG": {"column": "ID", "ids": [i * 3 for i in range(1, 1001)], "index": "BIG_PK", "pk": ["ID"],
                          "precision": 10, "scale": 0},
            "ALPHA.MAIL": {"column": "MAIL_ID", "ids": list(range(10, 60)), "index": "MAIL_PK", "pk": ["MAIL_ID"]},
        }
        env = mock.patch.dict(os.environ, {"FAKE_ORA_TABLES": str(self.source), "FAKE_ORA_QUERY_LOG": str(self.queries)})
        env.start()
        self.addCleanup(env.stop)

    @staticmethod
    def conf(schema, extra=""):
        return (f"ORACLE_DSN  dbi:Oracle:host=fake\nORACLE_USER  u\nORACLE_PWD  p\nSCHEMA  {schema}\n{extra}"
                f"PG_DSN  dbi:Pg:dbname=x;host=localhost;port=5432\nPG_SCHEMA  {schema.lower()}\n")

    def add_chunked(self, schema, table, chunk_file, lines):
        folder = self.tmp / "content" / schema / table
        folder.mkdir()
        (folder / f"ora2pg_{table}.conf").write_text(self.conf(schema))
        script = REAL_SCRIPT.read_text().replace("ora2pg_SAC_DATA.conf", f"ora2pg_{table}.conf")
        script = script.replace('TABLE_NAME="SAC_DATA"', f'TABLE_NAME="{table}"').replace("./chunks.txt", f"./{chunk_file}")
        (folder / f"run_{table}.sh").write_text(script)
        if lines is not None:
            (folder / chunk_file).write_text("\n".join(lines) + "\n")
        return folder / chunk_file

    def chunk_file(self, table, name="chunks.txt"):
        return self.tmp / "content" / "ALPHA" / table / name

    def main(self, *args):
        self.source.write_text(json.dumps(self.tables))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = chunks.main(["--config", str(self.tmp / "dataload.toml"), *args])
        return rc, out.getvalue()

    def assert_covers_once(self, lines, ids):
        ceiling = int(lines[-1].split()[-1])
        for x in list(ids) + [max(ids) + 1, ceiling]:   # and IDs above today's highest, up to the ceiling
            self.assertEqual(sum(holds(c, x) for c in lines), 1, f"{x} is not in exactly one chunk")
        self.assertEqual(int(lines[0].split()[2]), min(ids), "the first chunk starts at the lowest ID")

    def test_make_writes_even_chunks_and_keeps_the_old_file(self):
        old = self.chunk_file("BIG").read_text()
        rc, out = self.main("make", "--chunks", "8", "--yes")
        self.assertEqual(rc, 0, out)
        lines = self.chunk_file("BIG").read_text().splitlines()
        self.assertEqual(len(lines), 8)
        self.assertEqual(lines[0], "ID BETWEEN 3 AND 375")
        self.assertEqual(lines[1], "ID BETWEEN 376 AND 750")
        self.assertEqual(lines[-1], "ID BETWEEN 2626 AND 9999999999")
        self.assert_covers_once(lines, self.tables["ALPHA.BIG"]["ids"])
        backups = list(self.chunk_file("BIG").parent.glob("chunks.txt.bak-*"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_text(), old)
        mail = self.chunk_file("MAIL", "chunk.txt").read_text().splitlines()
        self.assertEqual(mail[0], "MAIL_ID BETWEEN 10 AND 16")
        self.assertEqual(mail[-1], f"MAIL_ID BETWEEN 59 AND {BIGINT_MAX}")   # plain NUMBER: no precision
        self.assertEqual(len(mail), 8)
        # The load sees the new ranges, and the backup is not mistaken for a step.
        cfg = config.load(self.tmp / "dataload.toml")
        units = {s.table: s.units for s in plan.load_steps(cfg) if s.kind == "chunk"}
        self.assertEqual(units["BIG"], lines)
        self.assertNotIn(".new", " ".join(p.name for p in self.chunk_file("BIG").parent.iterdir()))

    def test_never_more_chunks_than_asked_with_gaps_and_repeats(self):
        self.tables["ALPHA.BIG"]["ids"] = [1, 1, 1, 1, 1, 1, 2, 7, 7, 7, 8, 100, 100, 5000, 5001, 5001, 99999]
        self.tables["ALPHA.MAIL"]["ids"] = [40, 41, 41, 300, 301, 301, 301, 302, 9000000000]
        for want in (1, 2, 3, 5, 7, 120):
            rc, out = self.main("make", "--chunks", str(want), "--yes")
            self.assertEqual(rc, 0, out)
            for table, name in (("BIG", "chunks.txt"), ("MAIL", "chunk.txt")):
                lines = self.chunk_file(table, name).read_text().splitlines()
                self.assertLessEqual(len(lines), want)
                self.assertEqual(len(set(lines)), len(lines))
                ids = self.tables[f"ALPHA.{table}"]["ids"]
                self.assert_covers_once(lines, ids)

    def test_a_count_that_does_not_divide_evenly_stays_within_the_limit(self):
        # 1,201 rows: 1201 // 120 = 10 per chunk would give 121 chunks; rounding up gives 11 per chunk, 110 chunks.
        self.tables["ALPHA.BIG"]["ids"] = list(range(1, 1_202))
        rc, out = self.main("make", "--chunks", "120", "--yes", "--table", "BIG")
        self.assertEqual(rc, 0, out)
        self.assertEqual(len(self.chunk_file("BIG").read_text().splitlines()), 110)

    def test_ids_with_decimals_are_refused(self):
        self.tables["ALPHA.MAIL"]["ids"] = [1, 2, 2.5, 3]          # a plain NUMBER: the rows are looked at
        before = self.chunk_file("MAIL", "chunk.txt").read_text()
        rc, out = self.main("make", "--chunks", "2", "--yes")
        self.assertEqual(rc, 1, out)
        self.assertIn("1 rows have a MAIL_ID with decimals", out)
        self.assertEqual(self.chunk_file("MAIL", "chunk.txt").read_text(), before)
        self.tables["ALPHA.MAIL"].update(precision=10, scale=2)  # a column that can hold decimals: refused up front
        rc, out = self.main("make", "--yes")
        self.assertEqual(rc, 2, out)
        self.assertIn("NUMBER(10,2) and can hold decimals", out)
        self.assertNotIn("TRUNC(\"ID\")", self.queries.read_text(), "a NUMBER(10,0) needs no row check")

    def test_check_reads_no_table_data_and_changes_nothing(self):
        before = self.chunk_file("BIG").read_text()
        rc, out = self.main("check", "--chunks", "100")
        self.assertEqual(rc, 0, out)
        self.assertIn("ID column ID (from chunks.txt)", out)
        self.assertIn("ID column MAIL_ID (from chunk.txt)", out)
        self.assertIn("Oracle: NUMBER(10), not null, index BIG_PK", out)
        self.assertIn("the last chunk will end at 9999999999", out)
        self.assertIn("about 10 rows per chunk", out)
        queries = self.queries.read_text()
        self.assertNotIn("COUNT(1) - COUNT", queries)
        self.assertNotIn("ROWNUM", queries)
        self.assertEqual(self.chunk_file("BIG").read_text(), before)

    def test_rows_without_an_id_stop_that_table_only(self):
        self.tables["ALPHA.BIG"]["ids"] = [1, 2, None, 4]
        before = self.chunk_file("BIG").read_text()
        rc, out = self.main("make", "--chunks", "2", "--yes")
        self.assertEqual(rc, 1, out)
        self.assertIn("1 rows have no ID", out)
        self.assertEqual(self.chunk_file("BIG").read_text(), before)
        self.assertEqual(list(self.chunk_file("BIG").parent.glob("*.bak-*")), [])
        self.assertEqual(len(self.chunk_file("MAIL", "chunk.txt").read_text().splitlines()), 2)

    def test_an_oracle_error_leaves_that_file_alone(self):
        self.tables["ALPHA.MAIL"]["fail"] = "ORA-01555: snapshot too old"
        before = self.chunk_file("MAIL", "chunk.txt").read_text()
        rc, out = self.main("make", "--chunks", "4", "--yes")
        self.assertEqual(rc, 1, out)
        self.assertIn("ALPHA.MAIL: FAILED: ORA-01555", out)
        self.assertEqual(self.chunk_file("MAIL", "chunk.txt").read_text(), before)
        self.assertEqual(len(self.chunk_file("BIG").read_text().splitlines()), 4)

    def test_no_index_stops_everything_before_any_row_is_read(self):
        self.tables["ALPHA.BIG"]["index"] = ""
        rc, out = self.main("make", "--yes")
        self.assertEqual(rc, 2, out)
        self.assertIn("no index starts with ID", out)
        self.assertNotIn("ROWNUM", self.queries.read_text())
        self.assertEqual(list(self.tmp.glob("content/ALPHA/*/*.bak-*")), [])

    def test_column_comes_from_the_primary_key_or_the_command_line(self):
        self.chunk_file("BIG").unlink()
        rc, out = self.main("check", "--table", "BIG")
        self.assertEqual(rc, 0, out)
        self.assertIn("ID column ID (from primary key)", out)
        self.tables["ALPHA.BIG"]["pk"] = ["A", "B"]
        rc, out = self.main("check", "--table", "BIG")
        self.assertEqual(rc, 1, out)
        self.assertIn("--column BIG=COLUMN", out)
        rc, out = self.main("make", "--table", "BIG", "--column", "big=id", "--chunks", "3", "--yes")
        self.assertEqual(rc, 0, out)
        self.assertEqual(len(self.chunk_file("BIG").read_text().splitlines()), 3)
        self.assertEqual(list(self.chunk_file("BIG").parent.glob("*.bak-*")), [])   # nothing to keep

    def test_limits_and_unknown_tables_are_refused(self):
        rc, out = self.main("make", "--chunks", "121", "--yes")
        self.assertEqual(rc, 2)
        self.assertIn("between 1 and 120", out)
        rc, out = self.main("make", "--table", "NOPE", "--yes")
        self.assertEqual(rc, 2)
        self.assertIn("not a chunked table: NOPE", out)

    def test_refused_while_a_run_holds_the_lock(self):
        state = self.tmp / "state" / "run.sqlite"
        state.parent.mkdir()
        with open(str(state) + ".lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            rc, out = self.main("make", "--yes")
        self.assertEqual(rc, 2)
        self.assertIn("run is in progress", out)

    def add_date_table(self, ids, **extra):
        self.add_chunked("ALPHA", "LOG", "chunks.txt", ["AUDIT_DATE >= DATE '2000-01-01' AND AUDIT_DATE < DATE '9999-12-31'"])
        self.tables["ALPHA.LOG"] = {"column": "AUDIT_DATE", "type": "DATE", "ids": ids, "index": "LOG_IX", **extra}
        return self.chunk_file("LOG")

    def assert_dates_covered_once(self, lines, ids):
        for line in lines:
            self.assertRegex(line, DATE_LINE)
        for x in [v for v in ids if v] + ["2099-06-30 23:59:59", "9999-12-30 23:59:59"]:
            self.assertEqual(sum(holds_date(c, x) for c in lines), 1, f"{x} is not in exactly one chunk")
        for a, b in zip(lines, lines[1:]):
            self.assertEqual(DATE_LINE.fullmatch(a).group(3), DATE_LINE.fullmatch(b).group(2), "chunks touch")
        self.assertEqual(DATE_LINE.fullmatch(lines[0]).group(2), min(v for v in ids if v)[:10])
        self.assertEqual(DATE_LINE.fullmatch(lines[-1]).group(3), DATE_CEILING)

    def test_a_date_column_is_cut_at_whole_days(self):
        ids = every_hours(1000, 7)                        # rows at all times of day, over about 292 days
        path = self.add_date_table(ids)
        old = path.read_text()
        rc, out = self.main("make", "--table", "LOG", "--chunks", "10", "--yes")
        self.assertEqual(rc, 0, out)
        lines = path.read_text().splitlines()
        self.assertEqual(len(lines), 10)
        self.assertEqual(lines[0], "AUDIT_DATE >= DATE '2024-01-01' AND AUDIT_DATE < DATE '2024-01-30'")
        self.assert_dates_covered_once(lines, ids)
        self.assertIn("1,000 rows over 292 days", out)
        self.assertIn("the last chunk stops at 9999-12-31", out)
        queries = self.queries.read_text()
        self.assertIn('INDEX_FFS(x "LOG_IX")', queries)
        self.assertIn('GROUP BY TRUNC(x."AUDIT_DATE")', queries)
        self.assertNotIn("ROWNUM", queries)
        self.assertEqual([b.read_text() for b in path.parent.glob("chunks.txt.bak-*")], [old])
        cfg = config.load(self.tmp / "dataload.toml")
        units = {s.table: s.units for s in plan.load_steps(cfg) if s.kind == "chunk"}
        self.assertEqual(units["LOG"], lines)

    def test_a_busy_day_is_one_chunk_and_there_are_never_more_than_asked(self):
        ids = ["2024-03-01 10:00:00"] * 500 + every_hours(60, 9, "2024-03-02 01:00:00") + ["2026-10-01 23:59:59"]
        path = self.add_date_table(ids)
        for want in (1, 2, 3, 5, 7, 120):
            rc, out = self.main("make", "--table", "LOG", "--chunks", str(want), "--yes")
            self.assertEqual(rc, 0, out)
            lines = path.read_text().splitlines()
            self.assertLessEqual(len(lines), want)
            self.assert_dates_covered_once(lines, ids)
        rc, out = self.main("make", "--table", "LOG", "--chunks", "5", "--yes")
        self.assertIn("largest 500 rows", out)

    def test_a_date_table_with_empty_dates_or_dates_at_the_ceiling_is_refused(self):
        path = self.add_date_table(every_hours(20, 5) + [None], nullable="Y")
        before = path.read_text()
        rc, out = self.main("make", "--table", "LOG", "--chunks", "4", "--yes")
        self.assertEqual(rc, 1, out)
        self.assertIn("1 rows have no AUDIT_DATE", out)
        self.tables["ALPHA.LOG"]["ids"] = every_hours(20, 5) + ["9999-12-31 00:00:00"]
        rc, out = self.main("make", "--table", "LOG", "--chunks", "4", "--yes")
        self.assertEqual(rc, 1, out)
        self.assertIn("latest AUDIT_DATE is 9999-12-31", out)
        self.assertEqual(path.read_text(), before)
        self.assertEqual(list(path.parent.glob("*.bak-*")), [])

    def test_check_shows_a_date_column_and_text_columns_are_still_refused(self):
        self.add_date_table(every_hours(30, 24))
        rc, out = self.main("check", "--table", "LOG")
        self.assertEqual(rc, 0, out)
        self.assertIn("ID column AUDIT_DATE (from chunks.txt)", out)
        self.assertIn("Oracle: DATE, not null, index LOG_IX", out)
        self.assertIn("chunks are whole days, the last one stops at 9999-12-31", out)
        self.assertNotIn("GROUP BY", self.queries.read_text())
        self.tables["ALPHA.LOG"]["type"] = "VARCHAR2"
        rc, out = self.main("check", "--table", "LOG")
        self.assertEqual(rc, 1, out)
        self.assertIn("AUDIT_DATE is VARCHAR2, neither a whole number nor a DATE", out)

    def test_answering_no_reads_and_writes_nothing(self):
        self.source.write_text(json.dumps(self.tables))
        before = self.chunk_file("BIG").read_text()
        printed = []
        chunker = Chunker(config.load(self.tmp / "dataload.toml"), out=printed.append)
        self.assertEqual(chunker.make(10, confirm=lambda _: "n"), 1)
        self.assertNotIn("ROWNUM", self.queries.read_text())
        self.assertEqual(self.chunk_file("BIG").read_text(), before)


if __name__ == "__main__":
    unittest.main()
