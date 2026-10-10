"""The SQLite record of every step and unit. This file is what makes resume exact."""
import sqlite3
import threading
from datetime import datetime

SCHEMA = """
CREATE TABLE IF NOT EXISTS run (
    id INTEGER PRIMARY KEY, started TEXT, finished TEXT, target TEXT, invocations INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS step (
    id TEXT PRIMARY KEY, seq INTEGER, phase TEXT, kind TEXT, schema TEXT, log TEXT, notified INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS unit (
    step_id TEXT, seq INTEGER, key TEXT, status TEXT DEFAULT 'pending', attempts INTEGER DEFAULT 0,
    error TEXT, ora_count INTEGER, pg_count INTEGER, updated TEXT,
    PRIMARY KEY (step_id, key)
);
CREATE TABLE IF NOT EXISTS validation (
    step_id TEXT, tbl TEXT, ora_count INTEGER, pg_count INTEGER, checked TEXT,
    PRIMARY KEY (step_id, tbl)
);
CREATE TABLE IF NOT EXISTS snapshot (
    step_id TEXT, tbl TEXT, schema TEXT, filter TEXT,
    ora_before INTEGER, pg_before INTEGER, ora_bytes INTEGER, pg_bytes_before INTEGER, before_at TEXT,
    ora_after INTEGER, pg_after INTEGER, pg_bytes_after INTEGER, after_at TEXT,
    PRIMARY KEY (step_id, tbl)
);
"""


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class State:
    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        with self.lock, self.db:
            self.db.executescript(SCHEMA)
            # Added after the first version; a state file from before it gets the column here.
            if "pg_bytes_loaded" not in [c[1] for c in self.db.execute("PRAGMA table_info(snapshot)")]:
                self.db.execute("ALTER TABLE snapshot ADD COLUMN pg_bytes_loaded INTEGER")
            if "flags" not in [c[1] for c in self.db.execute("PRAGMA table_info(run)")]:
                self.db.execute("ALTER TABLE run ADD COLUMN flags TEXT")

    def _exec(self, sql, args=()):
        with self.lock, self.db:
            return self.db.execute(sql, args).fetchall()

    def begin(self, target):
        """Returns True when this state file already held a run (i.e. this is a resume)."""
        existing = self._exec("SELECT invocations FROM run WHERE id = 1")
        if not existing:
            self._exec("INSERT INTO run (id, started, target, invocations) VALUES (1, ?, ?, 1)", (now(), target))
            return False
        self._exec("UPDATE run SET invocations = invocations + 1, finished = NULL WHERE id = 1")
        return True

    def set_flags(self, skipped):
        """The phases this run skips, kept so that a resume continues the same way."""
        self._exec("UPDATE run SET flags = ? WHERE id = 1", (",".join(sorted(skipped)),))

    def skipped(self):
        info = self.run_info()
        return set(filter(None, ((info["flags"] if info else "") or "").split(",")))

    def finish(self):
        self._exec("UPDATE run SET finished = ? WHERE id = 1", (now(),))

    def run_info(self):
        rows = self._exec("SELECT * FROM run WHERE id = 1")
        return rows[0] if rows else None

    def sync(self, steps):
        """Record steps and units not seen before. Existing records, and the SQL text
        captured on the first run, are kept even if the files change later."""
        with self.lock, self.db:
            for seq, step in enumerate(steps):
                known = self.db.execute("SELECT 1 FROM step WHERE id = ?", (step.id,)).fetchone()
                if known and step.kind == "sql":
                    continue
                self.db.execute(
                    "INSERT OR IGNORE INTO step (id, seq, phase, kind, schema) VALUES (?, ?, ?, ?, ?)",
                    (step.id, seq, step.phase, step.kind, step.schema),
                )
                self.db.executemany(
                    "INSERT OR IGNORE INTO unit (step_id, seq, key) VALUES (?, ?, ?)",
                    [(step.id, i, key) for i, key in enumerate(step.units)],
                )

    def recover_interrupted(self):
        """Anything still 'running' was cut off by a kill; it is not done."""
        self._exec("UPDATE unit SET status = 'interrupted' WHERE status = 'running'")

    def units(self, step_id, only_unfinished=False):
        sql = "SELECT * FROM unit WHERE step_id = ?"
        if only_unfinished:
            sql += " AND status != 'done'"
        return self._exec(sql + " ORDER BY seq", (step_id,))

    def set_unit(self, step_id, key, status, error=None, ora_count=None, pg_count=None, attempt=False):
        self._exec(
            "UPDATE unit SET status = ?, error = ?, ora_count = COALESCE(?, ora_count), "
            "pg_count = COALESCE(?, pg_count), attempts = attempts + ?, updated = ? "
            "WHERE step_id = ? AND key = ?",
            (status, error, ora_count, pg_count, 1 if attempt else 0, now(), step_id, key),
        )

    def reset_step(self, step_id):
        # The log is forgotten too, so chunks it reports as finished are not taken as done again.
        self._exec("UPDATE step SET log = NULL WHERE id = ?", (step_id,))
        return self._exec(
            "UPDATE unit SET status = 'interrupted', error = NULL WHERE step_id = ?", (step_id,)
        )

    def set_log(self, step_id, log):
        self._exec("UPDATE step SET log = ? WHERE id = ?", (str(log), step_id))

    def counts(self, where="1=1", args=()):
        rows = self._exec(
            f"SELECT u.status, COUNT(*) AS n FROM unit u JOIN step s ON s.id = u.step_id WHERE {where} GROUP BY u.status",
            args,
        )
        return {r["status"]: r["n"] for r in rows}

    def steps(self, phase=None):
        if phase:
            return self._exec("SELECT * FROM step WHERE phase = ? ORDER BY seq", (phase,))
        return self._exec("SELECT * FROM step ORDER BY seq")

    def failed_units(self, step_id=None, phase=None, statuses=("failed",)):
        """Units that ended in an error. A unit cut off by a stop is 'interrupted', which is not a failure:
        pass other statuses to list those."""
        sql = ("SELECT u.*, s.phase, s.kind, s.log FROM unit u JOIN step s ON s.id = u.step_id "
               f"WHERE u.status IN ({', '.join('?' * len(statuses))})")
        args = list(statuses)
        if step_id:
            sql += " AND u.step_id = ?"
            args.append(step_id)
        if phase:
            sql += " AND s.phase = ?"
            args.append(phase)
        return self._exec(sql + " ORDER BY s.seq, u.seq", args)

    def set_validation(self, step_id, tbl, ora_count, pg_count):
        self._exec(
            "INSERT OR REPLACE INTO validation (step_id, tbl, ora_count, pg_count, checked) VALUES (?, ?, ?, ?, ?)",
            (step_id, tbl, ora_count, pg_count, now()),
        )

    def set_before(self, step_id, tbl, schema, filter, ora, pg, ora_bytes, pg_bytes):
        """The table as it was before the load. Recorded once; a resume never overwrites it."""
        self._exec(
            "INSERT OR IGNORE INTO snapshot (step_id, tbl, schema, filter, ora_before, pg_before, ora_bytes, "
            "pg_bytes_before, before_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (step_id, tbl, schema, filter, ora, pg, ora_bytes, pg_bytes, now()),
        )

    def set_after(self, step_id, tbl, schema, filter, ora, pg):
        self._exec("INSERT OR IGNORE INTO snapshot (step_id, tbl, schema, filter) VALUES (?, ?, ?, ?)",
                   (step_id, tbl, schema, filter))
        self._exec("UPDATE snapshot SET ora_after = ?, pg_after = ?, after_at = ? WHERE step_id = ? AND tbl = ?",
                   (ora, pg, now(), step_id, tbl))

    def set_pg_bytes_loaded(self, step_id, tbl, pg_bytes):
        """Size right after the table's rows are in, before its indexes are rebuilt."""
        self._exec("UPDATE snapshot SET pg_bytes_loaded = ? WHERE step_id = ? AND tbl = ?", (pg_bytes, step_id, tbl))

    def set_pg_bytes_after(self, step_id, tbl, pg_bytes):
        self._exec("UPDATE snapshot SET pg_bytes_after = ? WHERE step_id = ? AND tbl = ?", (pg_bytes, step_id, tbl))

    def snapshots(self):
        return self._exec("SELECT * FROM snapshot ORDER BY schema, tbl")

    def validations(self):
        return self._exec("SELECT * FROM validation ORDER BY step_id, tbl")
