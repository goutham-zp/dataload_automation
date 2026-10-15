"""Runs the phases: preflight, drop, load, recreate, validate."""
import os
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime

from . import counts, plan, report
from .conf import OraConf
from .notify import Notifier
from .redact import redact

# FATAL and ERROR count only as a message marker ("FATAL: ...", "ERROR:  ...", "[ERROR] ..."): ora2pg's
# progress lines name the table being copied, and tables are called ERROR, WWW_ERRORS and the like.
ERROR_RE = re.compile(
    r"(?<![A-Za-z_])(?:FATAL|ERROR)(?=:|\])|ORA-\d{5}|Aborting export|an error occurs during data export"
    r"|install_driver\(\w+\) failed"
)
ALREADY_RE = re.compile(r"already exists|multiple primary keys for table")
# Drops that PostgreSQL refuses because there is nothing to do at that level: a partition's copy of
# its parent's constraint goes away with the parent's, and an identity column keeps its NOT NULL.
NOOP_DROP_RE = re.compile(r"cannot drop inherited constraint|is an identity column")
COUNT_RE = re.compile(r"^([A-Z]+DB|POSTGRES):(.+):(\d+)$")
SUCCESS_RE = re.compile(r"\[SUCCESS\] Finished: (.*)$")
FAILED_RE = re.compile(r"\[ERROR\] Failed: (.*)\. Review logs at: (.*)$")


class Stopped(Exception):
    pass


@dataclass
class Flags:
    skip_drop: set = field(default_factory=set)       # of FK, PK, Other, indexes
    skip_recreate: set = field(default_factory=set)
    skip_load: bool = False
    skip_validate: bool = False
    force_recreate: bool = False
    allow_remaining_fks: bool = False
    skipped: set = field(default_factory=set)         # phase names, for messages: drop, recreate, load, validate


def lines_of(text):
    return [l for l in re.split(r"[\r\n]+", text) if l.strip()]


def human(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60:02d}s"
    return f"{seconds // 3600}h {seconds % 3600 // 60:02d}m"


def last_output_line(path, limit=140):
    """The newest line of a log file, without ora2pg's own timestamp or terminal control codes."""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - 4096))
            lines = lines_of(f.read().decode(errors="replace"))
    except OSError:
        return ""
    if not lines:
        return ""
    line = re.sub(r"[^\x20-\x7e]", "", lines[-1]).strip()
    return re.sub(r"^\[\d{4}-\d\d-\d\d [\d:]+\]\s*", "", line)[:limit]


def first_error(text):
    for line in lines_of(text):
        if ERROR_RE.search(line):
            return line.strip()[:300]
    return ""


def parse_test_count(text):
    """ora2pg -t TEST_COUNT prints ORACLEDB:<table>:<n> and POSTGRES:<table>:<n> lines."""
    source, target = {}, {}
    for line in lines_of(text):
        m = COUNT_RE.match(line.strip())
        if m:
            name = m.group(2).split(".")[-1].strip('"').upper()
            (target if m.group(1) == "POSTGRES" else source)[name] = int(m.group(3))
    return {t: (source[t], target[t]) for t in source if t in target}


def parse_chunk_output(text):
    done, failed = set(), {}
    for line in lines_of(text):
        m = SUCCESS_RE.search(line)
        if m:
            done.add(m.group(1).strip())
            continue
        m = FAILED_RE.search(line)
        if m:
            failed[m.group(1).strip()] = m.group(2).strip()
    return done, failed


class Engine:
    def __init__(self, cfg, state, flags):
        self.cfg, self.state, self.flags = cfg, state, flags
        self.notifier = Notifier(cfg, self.log)
        self.transient_re = re.compile("|".join(cfg.transient))
        self.stopping = False
        self.children = set()
        self.children_lock = threading.RLock()   # re-entrant: the signal handler may interrupt a holder
        self.pg_env = {}
        self.target = ""
        self.source = ""
        self.stopped_pids = []
        self.log_lock = threading.Lock()         # lanes log from several threads
        cfg.logs.mkdir(parents=True, exist_ok=True)
        cfg.work.mkdir(parents=True, exist_ok=True)
        cfg.report = cfg.report or cfg.root / "report.html"
        if cfg.oracle_count:
            self.oracle_count_cmd = [cfg.oracle_count]
        else:
            script = cfg.work / "oracle_count.pl"
            script.write_text(counts.ORACLE_COUNT_PL)
            self.oracle_count_cmd = ["perl", str(script)]

    # ---------- plumbing ----------

    def log(self, message):
        line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}"
        with self.log_lock:
            print(line, flush=True)
            with open(self.cfg.logs / "dataload.log", "a") as f:
                f.write(line + "\n")

    def install_signal_handlers(self):
        def handler(signum, _frame):
            self.stopping = True
            with self.children_lock:
                roots = [child.pid for child in self.children]
            self.stopped_pids = self.kill_tree(roots)
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)

    @staticmethod
    def kill_tree(roots, sig=signal.SIGTERM):
        """Signals the given processes and everything they started. GNU parallel gives every job its own
        process group, so the group of the script is not enough, or ora2pg would keep copying."""
        parents = {}
        try:
            listing = subprocess.run(["ps", "-eo", "pid=,ppid="], capture_output=True, text=True).stdout
            for line in listing.splitlines():
                pid, ppid = line.split()
                parents.setdefault(int(ppid), []).append(int(pid))
        except (OSError, ValueError):
            pass
        victims, queue = [], list(roots)
        while queue:
            pid = queue.pop()
            victims.append(pid)
            queue.extend(parents.get(pid, []))
        for pid in victims:
            try:
                os.kill(pid, sig)
            except (ProcessLookupError, PermissionError):
                pass
        return victims

    @staticmethod
    def finish_off(pids, grace=10):
        """An ora2pg reader in the middle of a fetch does not act on a polite stop. Whatever is still
        alive after the grace period is killed outright, so nothing keeps copying in the background."""
        deadline = time.time() + grace
        alive = list(pids)
        while alive and time.time() < deadline:
            time.sleep(0.5)
            still = []
            for pid in alive:
                try:
                    os.kill(pid, 0)
                    still.append(pid)
                except (ProcessLookupError, PermissionError):
                    pass
            alive = still
        for pid in alive:
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        return alive

    def lost_connection(self, conf):
        """True when neither database is doing anything for this load: no Oracle session of this account
        has answered a call in the last two minutes and PostgreSQL has no session at work. False when
        either is busy or cannot be asked."""
        # A table with LOB columns is fetched one row per call, so its session shows INACTIVE between
        # calls while it is hard at work; the time since its last call is what tells it from a dead one.
        values, errors = self.oracle_values(conf, {"busy": (
            "SELECT COUNT(*) FROM v$session WHERE username = USER AND (status = 'ACTIVE' OR last_call_et < 120) "
            "AND sid <> SYS_CONTEXT('USERENV', 'SID')")})
        if errors or values.get("busy", 1) > 0:
            return False
        rc, out, _ = self.psql("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
                               "AND pid <> pg_backend_pid() AND state <> 'idle'")
        return rc == 0 and out.strip() == "0"

    def _spawn(self, argv, **kwargs):
        child = subprocess.Popen(argv, start_new_session=True, **kwargs)
        with self.children_lock:
            self.children.add(child)
        return child

    def _reap(self, child):
        with self.children_lock:
            self.children.discard(child)

    def exec_logged(self, argv, cwd, log_path, env=None, label=None, progress=None, conf=None, activity=None):
        """Runs argv with its output in log_path. With a label, a "still running" line is logged
        every heartbeat, carrying progress() or else the newest line of the output."""
        started = beat = checked = time.time()
        stall = self.cfg.stall_minutes * 60 if conf else 0
        killed_at, stalled_pids, stop_seen = None, [], None
        # A private temp directory: ora2pg takes any tmp_ora2pg* file in its temp directory as an error,
        # so a file left by a killed ora2pg must never be visible to the next one.
        (self.cfg.work / "tmp").mkdir(parents=True, exist_ok=True)
        private_tmp = tempfile.mkdtemp(prefix="t", dir=self.cfg.work / "tmp")
        with open(log_path, "ab") as out:
            child = self._spawn(argv, cwd=cwd, stdout=out, stderr=subprocess.STDOUT,
                                env={**os.environ, "TMPDIR": private_tmp, **(env or {})})
            try:
                while True:
                    try:
                        rc = child.wait(timeout=1)
                        break
                    except subprocess.TimeoutExpired:
                        pass
                    now = time.time()
                    if self.stopping:
                        # Asked to stop: a child that has not gone after ten seconds is killed outright.
                        stop_seen = stop_seen or now
                        if now - stop_seen > 10:
                            self.finish_off(self.stopped_pids + [child.pid], grace=0)
                        continue
                    if killed_at and now - killed_at > 30:
                        self.finish_off(stalled_pids, grace=0)
                    elif stall and not killed_at and now - checked >= min(60, stall):
                        checked = now
                        try:
                            # A chunk script's own log is no sign of life: GNU parallel redraws its progress
                            # bar there every second. The logs of the chunks themselves are what count.
                            newest = max(activity(), started) if activity else os.path.getmtime(log_path)
                        except OSError:
                            newest = now
                        if now - newest >= stall and self.lost_connection(conf):
                            note = (f"FATAL: dataload: stalled. Nothing was written for {human(now - newest)} and neither "
                                    "Oracle nor PostgreSQL is working on this load: the connection was lost. Stopped.")
                            with open(log_path, "a") as f:
                                f.write(f"\n{note}\n")
                            self.log(f"{label or argv[0]}: {note[7:]}")
                            killed_at = now
                            stalled_pids = self.kill_tree([child.pid])
                    if label and self.cfg.heartbeat and time.time() - beat >= self.cfg.heartbeat:
                        beat = time.time()
                        detail = progress() if progress else last_output_line(log_path)
                        self.log(f"{label}: still running after {human(beat - started)}"
                                 + (f" | {detail}" if detail else ""))
            finally:
                self._reap(child)
                if stalled_pids:
                    self.finish_off(stalled_pids, grace=5)
                shutil.rmtree(private_tmp, ignore_errors=True)
        if self.stopping:
            raise Stopped()
        return rc

    def psql(self, sql):
        child = self._spawn(
            [self.cfg.psql, "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", sql],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env={**os.environ, **self.pg_env},
        )
        try:
            out, err = child.communicate()
        finally:
            self._reap(child)
        return child.returncode, out.strip(), err.strip()

    @contextmanager
    def ticking(self, label, detail=None):
        """Logs a 'still running' line every heartbeat while the block runs."""
        started, ended = time.time(), threading.Event()

        def beat():
            while self.cfg.heartbeat and not ended.wait(self.cfg.heartbeat):
                extra = detail() if detail else ""
                self.log(f"{label}: still running after {human(time.time() - started)}" + (f" | {extra}" if extra else ""))

        threading.Thread(target=beat, daemon=True).start()
        try:
            yield
        finally:
            ended.set()

    def oracle_values(self, conf, queries, answered=None):
        """Runs {id: sql} on Oracle, a few at a time. Returns ({id: number}, {id: why it failed})."""
        values, errors = {}, {}
        if not queries:
            return values, errors
        env = {**os.environ, "DL_ORA_DSN": conf.get("ORACLE_DSN"), "DL_ORA_USER": conf.get("ORACLE_USER"),
               "DL_ORA_PWD": conf.get("ORACLE_PWD")}
        if conf.get("ORACLE_HOME"):
            env.setdefault("ORACLE_HOME", conf.get("ORACLE_HOME"))
        items = list(queries.items())
        workers = max(1, min(self.cfg.count_workers, len(items)))
        lock = threading.Lock()

        def work(share):
            child = self._spawn(self.oracle_count_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True, env=env)
            got, noise = {}, []
            try:
                child.stdin.write("".join(f"{i}\t{' '.join(q.split())}\n" for i, q in share))
                child.stdin.close()
                for line in child.stdout:
                    ident, tab, value = line.rstrip("\n").partition("\t")
                    if not tab:
                        noise.append(line.strip())
                        continue
                    got[ident] = value
                    if answered is not None:
                        with lock:
                            answered[0] += 1
                child.wait()
            finally:
                self._reap(child)
            with lock:
                for ident, _ in share:
                    value = got.get(ident)
                    if value is None:
                        errors[ident] = (got.get("FATAL") or " ".join(noise) or "no answer from Oracle")[:300]
                    elif value.startswith("ERROR"):
                        errors[ident] = value[5:].strip()[:300]
                    else:
                        try:
                            values[ident] = int(float(value))
                        except ValueError:
                            errors[ident] = f"unexpected answer: {value[:100]}"

        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(work, [items[k::workers] for k in range(workers)]))
        if self.stopping:
            raise Stopped()
        return values, errors

    def pg_values(self, queries, answered=None):
        """Runs {id: sql} on PostgreSQL, a few at a time. Returns ({id: number}, {id: why it failed})."""
        values, errors = {}, {}

        def work(item):
            ident, query = item
            rc, out, err = self.psql(query)
            if rc == 0 and out.strip().lstrip("-").isdigit():
                values[ident] = int(out.strip())
            else:
                errors[ident] = (err.splitlines()[0] if err else f"unexpected answer: {out[:100]}")[:300]
            if answered is not None:
                answered[0] += 1

        if queries:
            with ThreadPoolExecutor(max_workers=max(1, self.cfg.count_workers)) as pool:
                list(pool.map(work, queries.items()))
        if self.stopping:
            raise Stopped()
        return values, errors

    def specs_for(self, step, tables=None):
        conf = OraConf(step.conf_path)
        schema, pg_schema = conf.get("SCHEMA").upper(), conf.pg_schema()
        if step.kind == "chunk":
            ranges = " OR ".join(f"({c})" for c in step.units)
            return [counts.Spec(step.id, step.conf_path, schema, step.table.upper(),
                                '"%s"."%s"' % (pg_schema, step.table.lower()), ranges, True)]
        filters = counts.where_filters(conf.get("WHERE"))
        return [counts.Spec(step.id, step.conf_path, schema, t.upper(), '"%s"."%s"' % (pg_schema, t.lower()),
                            filters.get(t.upper()) or filters.get("*"))
                for t in (tables or step.units)]

    def measure(self, specs, sizes, label):
        """Row counts (and sizes) on both sides: {spec.key: {"ora", "pg", "ora_bytes", "pg_bytes", "error"}}."""
        results = {sp.key: {"ora": None, "pg": None, "ora_bytes": None, "pg_bytes": None, "error": ""} for sp in specs}
        by_id = {f"{n}": sp for n, sp in enumerate(specs)}
        answered = [0]
        total = len(specs) * (4 if sizes else 2)
        with self.ticking(label, lambda: f"{answered[0]} of {total} answers in"):
            pg_queries = {f"c{i}": sp.pg_count_sql() for i, sp in by_id.items()}
            if sizes:
                pg_queries.update({f"s{i}": sp.pg_size_sql() for i, sp in by_id.items()})
            values, errors = self.pg_values(pg_queries, answered)
            for i, sp in by_id.items():
                results[sp.key]["pg"] = values.get(f"c{i}")
                results[sp.key]["pg_bytes"] = values.get(f"s{i}")
                if f"c{i}" in errors:
                    results[sp.key]["error"] = f"PostgreSQL: {errors[f'c{i}']}"

            groups = {}
            for i, sp in by_id.items():
                groups.setdefault(sp.conf_path, []).append((i, sp))
            for conf_path, members in groups.items():
                queries = {f"c{i}": sp.oracle_count_sql() for i, sp in members}
                if sizes:
                    queries.update({f"s{i}": sp.oracle_size_sql() for i, sp in members})
                for attempt in range(1, self.cfg.attempts + 1):
                    values, errors = self.oracle_values(OraConf(conf_path), queries, answered)
                    for i, sp in members:
                        if f"c{i}" in values:
                            results[sp.key]["ora"] = values[f"c{i}"]
                        if f"s{i}" in values:
                            results[sp.key]["ora_bytes"] = values[f"s{i}"]
                    # Only the counts are retried; a size that cannot be read is simply left out.
                    queries = {q: sql for q, sql in queries.items() if q in errors and q.startswith("c")}
                    if not queries or attempt == self.cfg.attempts or not any(
                            self.is_transient(errors[q]) for q in queries):
                        break
                    self.sleep(self.cfg.backoff_seconds * attempt)
                for i, sp in members:
                    if f"c{i}" in errors and results[sp.key]["ora"] is None:
                        results[sp.key]["error"] = (results[sp.key]["error"] + f" Oracle: {errors[f'c{i}']}").strip()
        return results

    def write_report(self):
        try:
            self.cfg.report.write_text(report.build(self.state, self.cfg, self.target, self.source))
        except Exception as e:  # noqa: BLE001 - a report problem must not stop the load
            self.log(f"report not written: {e!r}")

    def sleep(self, seconds):
        deadline = time.time() + seconds
        while time.time() < deadline:
            if self.stopping:
                raise Stopped()
            time.sleep(min(1, max(0, deadline - time.time())))

    def is_transient(self, text):
        return bool(self.transient_re.search(text or ""))

    def log_path(self, step_id, suffix="log"):
        safe = step_id.replace("/", "__")
        return self.cfg.logs / f"{safe}.{datetime.now():%Y%m%d-%H%M%S}.{suffix}"

    # ---------- preflight ----------

    def preflight(self, loads):
        cfg = self.cfg
        _, problems, warnings = plan.lanes(cfg, loads)

        for tool in [cfg.ora2pg, cfg.psql, "bash"] + (["parallel"] if any(s.kind == "chunk" for s in loads) else []):
            if not shutil.which(tool):
                problems.append(f"tool not found on PATH: {tool}")
        if cfg.check_perl_dbd_pg and shutil.which("perl"):
            if subprocess.run(["perl", "-MDBD::Pg", "-e", "1"], capture_output=True).returncode != 0:
                problems.append("Perl module DBD::Pg is not installed (ora2pg cannot write to PostgreSQL)")

        targets = {}
        for s in loads:
            targets.setdefault(OraConf(s.conf_path).target(), []).append(s.id)
        if len(targets) > 1:
            problems.append("confs point at different PostgreSQL targets: "
                            + "; ".join(f"{t} ({len(ids)} steps, e.g. {ids[0]})" for t, ids in targets.items()))
        if not cfg.expected_target:
            problems.append(f"[target] expected is not set in dataload.toml; the confs point at {self.target}")
        elif set(targets) != {cfg.expected_target}:
            problems.append(f"confs point at {', '.join(targets)} but dataload.toml expects {cfg.expected_target}")

        for s in loads:
            if s.kind != "chunk":
                if not s.units:
                    problems.append(f"{s.id}: no tables listed in TABLE/ALLOW")
                continue
            if not s.chunks_overridable:
                problems.append(f"{s.id}: script is not patched (CHUNKS_FILE must be overridable)")
            if not s.conf_path.exists():
                problems.append(f"{s.id}: conf {s.conf_path.name} not found")
            if not s.units:
                problems.append(f"{s.id}: chunk file {s.chunks_file.name} is missing or empty")
            if len(set(s.units)) != len(s.units):
                problems.append(f"{s.id}: chunk file has duplicate lines")

        for schema in dict.fromkeys(s.schema for s in loads):
            folder = cfg.content / plan.GENERATED_DIR / f"{schema}_generated_scripts"
            if not folder.is_dir():
                warnings.append(f"{schema}: no generated drop/recreate scripts, constraints will be left as they are")

        if problems or not shutil.which(cfg.psql):
            return problems, warnings

        rc, _, err = self.psql("SELECT 1")
        if rc != 0:
            problems.append(f"cannot connect to PostgreSQL {self.target}: {err}")
            return problems, warnings

        first = loads[0]
        probe = subprocess.run([cfg.ora2pg, "-t", "SHOW_VERSION", "-c", str(first.conf_path)],
                               capture_output=True, text=True, cwd=first.conf_path.parent)
        if probe.returncode != 0 or first_error(probe.stdout + probe.stderr):
            problems.append("cannot connect to Oracle with ora2pg: "
                            + (first_error(probe.stdout + probe.stderr) or f"exit code {probe.returncode}"))

        if cfg.before_counts or cfg.validate_method == "direct":
            _, errors = self.oracle_values(OraConf(first.conf_path), {"probe": "SELECT 1 FROM dual"})
            if errors:
                problems.append(f"cannot count rows on Oracle ({' '.join(self.oracle_count_cmd)}): {errors['probe']}")

        names = []
        for s in loads:
            schema = OraConf(s.conf_path).pg_schema()
            for t in ([s.table] if s.kind == "chunk" else s.units):
                names.append('"%s"."%s"' % (schema, t.lower()))
        literals = ",".join("'" + n.replace("'", "''") + "'" for n in names)
        rc, out, err = self.psql(f"SELECT t FROM unnest(ARRAY[{literals}]) AS t WHERE to_regclass(t) IS NULL")
        missing = set(out.split("\n")) if out else set()
        for name in sorted(missing):
            warnings.append(f"table not found on target: {name}")

        for s in loads:
            if s.kind != "chunk":
                continue
            qualified = '"%s"."%s"' % (OraConf(s.conf_path).pg_schema(), s.table.lower())
            if qualified in missing:
                continue
            for cond in s.units:
                rc, _, err = self.psql(f"EXPLAIN DELETE FROM {qualified} WHERE ({cond})")
                if rc != 0:
                    problems.append(f"{s.id}: chunk condition is not valid: {cond!r}: {err.splitlines()[0]}")
                    break
        return problems, warnings

    # ---------- phases ----------

    def run(self):
        cfg, state = self.cfg, self.state
        steps = plan.build(cfg)
        loads = [s for s in steps if s.phase == "load"]
        if not loads:
            self.log(f"nothing to load under {cfg.content}")
            return 2
        first_conf = OraConf(loads[0].conf_path)
        self.pg_env, self.target = first_conf.pg_env(), first_conf.target()
        self.source = first_conf.get("ORACLE_DSN").split(":", 2)[-1]

        problems, warnings = self.preflight(loads)
        for w in warnings:
            self.log(f"preflight warning: {w}")
        if problems:
            for p in problems:
                self.log(f"preflight FAILED: {p}")
            return 2
        self.log(f"preflight passed, target {self.target}")

        if state.run_info() is not None:
            # A resume would run the new ranges beside the old ones, and chunked tables are never truncated.
            changed = [s.id for s in loads if s.kind == "chunk" and state.units(s.id)
                       and {u["key"] for u in state.units(s.id)} != set(s.units)]
            if changed:
                self.log("resume refused: the chunk file of " + ", ".join(changed) + " changed since this run "
                         "started, so old and new ranges could load the same rows twice. Start over with "
                         "'run --new', or put the old chunk file back (dataload.chunks keeps it as .bak-<time>).")
                return 2

        resumed = state.begin(self.target)
        state.set_flags(self.flags.skipped)
        if self.flags.skipped:
            self.log("phases skipped by flag: " + ", ".join(sorted(self.flags.skipped)))
        state.recover_interrupted()
        state.sync(steps)
        if resumed:
            c = state.counts()
            left = sum(n for s, n in c.items() if s != "done")
            self.log(f"resuming: {c.get('done', 0)} units done, {left} left")
            self.notifier.send("Run resumed", f"Target: {self.target}\nAlready done: {c.get('done', 0)} units\n"
                                              f"Left to do: {left} units\n\n{self.summary()}")
        else:
            self.log(f"new run: {sum(len(s.units) for s in steps)} units in {len(steps)} steps")

        self.install_signal_handlers()
        try:
            blocked = (self.phase_before(loads) or self.phase_drop() or self.phase_load(loads)
                       or self.phase_recreate() or "")
            mismatches = [] if blocked else self.phase_validate(loads)
        except Stopped:
            left = self.finish_off(self.stopped_pids)
            if left:
                self.log(f"{len(left)} process(es) did not stop when asked and were killed")
            self.write_report()
            self.log("stopped by signal; nothing is lost, run 'resume' to continue")
            return 130

        state.finish()
        failed = state.failed_units()
        ok = not blocked and not failed and not mismatches
        body = f"Target: {self.target}\n\n{self.summary()}"
        if blocked:
            body += f"\n\nStopped early: {blocked}"
        if failed:
            body += "\n\nFailed units:\n" + self.describe(failed)
        if mismatches:
            body += "\n\nRow-count mismatches:\n" + "\n".join(mismatches)
        checked = state.validations()
        if checked:
            equal = sum(1 for v in checked if v["ora_count"] == v["pg_count"])
            body += (f"\n\nRow counts compared for {len(checked)} table(s): {equal} equal, "
                     f"{len(checked) - equal} different (see 'status --counts')")
        self.write_report()
        self.log(f"report written: {cfg.report}")
        self.notifier.send("Run finished: " + ("all done" if ok else "NEEDS ATTENTION"), body, attach=cfg.report)
        self.log("run finished: " + ("all done" if ok else "needs attention, see 'status'"))
        return 0 if ok else 1

    def summary(self):
        lines = []
        for phase in ("drop", "load", "recreate"):
            c = self.state.counts("s.phase = ?", (phase,))
            total = sum(c.values())
            if not total:
                continue
            if phase in self.flags.skipped:
                lines.append(f"{phase:9} skipped by flag")
                continue
            stopped = c.get("interrupted", 0) + c.get("running", 0)
            lines.append(f"{phase:9} {c.get('done', 0):,} of {total:,} done, {c.get('failed', 0):,} failed"
                         + (f", {stopped:,} stopped part-way (redone on resume)" if stopped else "")
                         + (f", {c['pending']:,} not started" if c.get("pending") else ""))
        return "\n".join(lines)

    def describe(self, units, limit=30):
        # This text is mailed, so values quoted by the database are hidden; the logs keep the full error.
        hide = redact if self.cfg.hide_values else (lambda text: text)
        lines = [f"- {u['step_id']} :: {u['key'][:120]}\n    {hide(u['error']) or u['status']}" for u in units[:limit]]
        if len(units) > limit:
            lines.append(f"... and {len(units) - limit} more, see 'status --failed'")
        return "\n".join(lines)

    def report_step(self, step_id, started=None):
        took = f" in {human(time.time() - started)}" if started else ""
        failed = self.state.failed_units(step_id=step_id)
        if not failed:
            self.log(f"{step_id}: done{took}")
            return
        row = [s for s in self.state.steps() if s["id"] == step_id][0]
        self.log(f"{step_id}: {len(failed)} unit(s) FAILED{took}, log {row['log']}")
        self.notifier.send(f"Step failed: {step_id}",
                           f"Target: {self.target}\nStep: {step_id}\nLog: {row['log']}\n"
                           f"Failed: {len(failed)}\n\n{self.describe(failed)}")

    def phase_finished(self, phase, started):
        c = self.state.counts("s.phase = ?", (phase,))
        failed = c.get("failed", 0)
        minutes = (time.time() - started) / 60
        self.log(f"phase {phase} finished in {minutes:.1f} min: {c.get('done', 0)} done, {failed} failed")
        self.write_report()
        self.notifier.send(f"Phase finished: {phase}" + (" (with failures)" if failed else ""),
                           f"Target: {self.target}\nElapsed: {minutes:.1f} min\n\n{self.summary()}")

    def phase_before(self, loads):
        """Every table's row count and size on both sides, taken before anything is changed."""
        if not self.cfg.before_counts:
            return
        have = {(r["step_id"], r["tbl"]) for r in self.state.snapshots() if r["before_at"]}
        specs = [sp for s in loads for sp in self.specs_for(s) if sp.key not in have]
        if not specs:
            return
        started = time.time()
        self.log(f"before-load snapshot: counting {len(specs)} table(s) on Oracle and PostgreSQL, "
                 f"{self.cfg.count_workers} at a time")
        results = self.measure(specs, sizes=True, label="before-load snapshot")
        missing = []
        for sp in specs:
            r = results[sp.key]
            if r["ora"] is None or r["pg"] is None:
                missing.append(f"{sp.name}: {redact(r['error']) if self.cfg.hide_values else r['error']}")
                continue
            self.state.set_before(sp.step_id, sp.table, sp.schema, sp.filter, r["ora"], r["pg"], r["ora_bytes"], r["pg_bytes"])
        rows = self.state.snapshots()
        self.log(f"before-load snapshot took {human(time.time() - started)}: {len(rows)} table(s), "
                 f"{sum(r['ora_before'] or 0 for r in rows):,} rows to load from Oracle, "
                 f"{sum(r['pg_before'] or 0 for r in rows):,} rows in PostgreSQL now")
        waiting = [f"{r['schema']}.{r['tbl']} ({r['pg_before']:,} rows)" for r in rows if r["filter"] and r["pg_before"]
                   and any(s.id == r["step_id"] and s.kind == "chunk" for s in loads)]
        if waiting:
            self.log("WARNING: chunked tables are not truncated, and these already hold rows inside the ranges "
                     "about to be loaded: " + ", ".join(waiting))
        self.write_report()
        if missing:
            reason = (f"the before-load count failed for {len(missing)} table(s), so nothing was changed "
                      f"(e.g. {'; '.join(missing[:3])}). Fix the cause and run 'resume'.")
            self.log(f"load NOT started: {reason}")
            return reason

    def after_counts(self, step, tables=None):
        """{TABLE: (oracle rows, postgres rows)} for a step's tables, by the configured method."""
        if self.cfg.validate_method == "test_count":
            pairs = self.test_count(step, tables or [step.table])
        elif self.cfg.validate_method == "direct":
            specs = self.specs_for(step, tables)
            started = time.time()
            self.log(f"{step.id}: counting rows on Oracle and PostgreSQL for {len(specs)} table(s)")
            results = self.measure(specs, sizes=False, label=f"{step.id} row count")
            pairs = {}
            for sp in specs:
                r = results[sp.key]
                if r["ora"] is None or r["pg"] is None:
                    self.log(f"{step.id}: {sp.table}: rows could not be counted: {r['error']}")
                else:
                    pairs[sp.table] = (r["ora"], r["pg"])
            self.log(f"{step.id}: row count took {human(time.time() - started)}")
        else:
            return {}
        loaded = {f"{n}": sp for n, sp in enumerate(self.specs_for(step, tables)) if sp.table in pairs}
        # The size is taken now, while the table holds only its rows: indexes come back in the recreate phase.
        sizes, _ = self.pg_values({i: sp.pg_size_sql() for i, sp in loaded.items()})
        for i, sp in loaded.items():
            self.state.set_after(sp.step_id, sp.table, sp.schema, sp.filter, *pairs[sp.table])
            if i in sizes:
                self.state.set_pg_bytes_loaded(sp.step_id, sp.table, sizes[i])
        return pairs

    def phase_drop(self):
        started, ran = time.time(), False
        for row in self.state.steps("drop"):
            if row["id"].split("/")[-1] in self.flags.skip_drop:
                continue
            ran = self.run_sql_step(row, workers=1) or ran
        if ran:
            self.phase_finished("drop", started)

    def remaining_fks(self, tables):
        """Foreign keys that reference any of `tables` (quoted "schema"."table" names)."""
        names = ",".join("'" + t.replace("'", "''") + "'" for t in tables)
        rc, out, err = self.psql(
            "SELECT c.conrelid::regclass || '.' || c.conname FROM pg_constraint c "
            "WHERE c.contype = 'f' AND c.confrelid IN "
            f"(SELECT to_regclass(t) FROM unnest(ARRAY[{names}]) AS t) ORDER BY 1")
        if rc != 0:
            raise RuntimeError(f"could not check foreign keys: {err}")
        return out.split("\n") if out else []

    def phase_load(self, loads):
        if self.flags.skip_load:
            return
        todo = [s for s in loads if self.state.units(s.id, only_unfinished=True)]
        if not todo:
            return
        rc, out, _ = self.psql("SELECT pid || ' (' || left(query, 60) || ')' FROM pg_stat_activity "
                               "WHERE datname = current_database() AND pid <> pg_backend_pid() "
                               "AND state = 'active' AND query ~* '^\\s*COPY\\s'")
        if rc == 0 and out:
            reason = (f"{len(out.splitlines())} session(s) are already copying data into this database: "
                      f"{', '.join(out.splitlines()[:3])}. They are probably left from a run that was killed, and "
                      "loading beside them would duplicate rows. Wait for them or stop them, then run 'resume'.")
            self.log(f"load NOT started: {reason}")
            return reason
        lanes = plan.lanes(self.cfg, loads)[0]
        # With lanes the check is never skipped: a cascade from one lane could empty a table another is loading.
        if any(s.kind == "conf" for s in todo) and (lanes or not self.flags.allow_remaining_fks):
            truncated = ['"%s"."%s"' % (OraConf(s.conf_path).pg_schema(), u["key"].lower())
                         for s in todo if s.kind == "conf"
                         for u in self.state.units(s.id, only_unfinished=True)]
            fks = self.remaining_fks(truncated)
            if fks:
                reason = (f"{len(fks)} foreign key(s) still reference the tables being loaded "
                          f"(e.g. {', '.join(fks[:3])}). The confs use TRUNCATE ... CASCADE, which would "
                          "empty the referencing tables. Drop them" + (
                              "; with lanes on, --allow-remaining-fks is not accepted, because the cascade "
                              "could empty a table another lane is loading." if lanes
                              else ", or pass --allow-remaining-fks."))
                self.log(f"load NOT started: {reason}")
                return reason
        started = time.time()
        if lanes:
            self.run_lanes(lanes, {s.id for s in todo})
        else:
            for i, s in enumerate(todo, 1):
                self.log(f"load step {i} of {len(todo)}: {self.step_line(s)}")
                self.run_load_step(s)
        self.phase_finished("load", started)

    def step_line(self, step):
        what = "chunks" if step.kind == "chunk" else "tables"
        return f"{step.id}, {len(self.state.units(step.id, only_unfinished=True))} of {len(step.units)} {what} to do"

    def run_load_step(self, step):
        started = time.time()
        (self.run_chunk_step if step.kind == "chunk" else self.run_conf_step)(step)
        if step.kind == "chunk":
            self.count_chunked(step)
        self.report_step(step.id, started)

    def run_lanes(self, lanes, waiting):
        """Each lane runs its unfinished steps one after another, all lanes at the same time. A failed step
        does not stop its lane or the others, just as a failed step does not stop a load without lanes."""
        work = [(name, [s for s in steps if s.id in waiting]) for name, steps in lanes]
        work = [(name, steps) for name, steps in work if steps]
        self.log(f"load runs in {len(work)} lane{'s' * (len(work) != 1)} side by side: "
                 + ", ".join(f"{name} ({len(steps)} step{'s' * (len(steps) != 1)})" for name, steps in work))
        crashed = []

        def lane(name, steps):
            started = time.time()
            try:
                for i, s in enumerate(steps, 1):
                    self.log(f"{name}: step {i} of {len(steps)}: {self.step_line(s)}")
                    self.run_load_step(s)
            except Stopped:
                return
            except Exception as e:  # noqa: BLE001 - reported once the other lanes have ended
                crashed.append(e)
                self.log(f"{name}: stopped by an unexpected error: {e!r}")
                return
            failed = sum(len(self.state.failed_units(step_id=s.id)) for s in steps)
            self.log(f"{name}: finished in {human(time.time() - started)}"
                     + (f", {failed} unit(s) failed" if failed else ", all steps done"))

        threads = [threading.Thread(target=lane, args=item, name=f"lane {item[0]}", daemon=True) for item in work]
        for t in threads:
            t.start()
        # Joined with a timeout, so the main thread stays free to take Ctrl+C and pass the stop to every lane.
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(timeout=1)
        if self.stopping:
            raise Stopped()
        if crashed:
            raise crashed[0]

    def count_chunked(self, step):
        """Once every chunk of a table is in, compare both sides inside the loaded ranges."""
        if self.cfg.validate_method == "exit" or self.state.units(step.id, only_unfinished=True):
            return
        pair = self.after_counts(step).get(step.table.upper())
        if pair:
            self.state.set_validation(step.id, step.table.upper(), *pair)
            self.log(f"{step.id}: {step.table.upper()}: oracle={pair[0]} postgres={pair[1]} "
                     + ("equal" if pair[0] == pair[1] else "DIFFERENT"))

    def phase_recreate(self):
        rows = [r for r in self.state.steps("recreate") if r["id"].split("/")[-1] not in self.flags.skip_recreate]
        if not any(self.state.units(r["id"], only_unfinished=True) for r in rows):
            return
        c = self.state.counts("s.phase = 'load'")
        unfinished = sum(n for s, n in c.items() if s != "done")
        if unfinished and not self.flags.force_recreate:
            reason = (f"{unfinished} load unit(s) are not done, so constraints and indexes were not recreated. "
                      "Fix the failures and run 'resume', or pass --force-recreate.")
            self.log(f"recreate NOT started: {reason}")
            return reason
        started = time.time()
        for row in rows:
            kind = row["id"].split("/")[-1]
            self.run_sql_step(row, workers=1 if kind == "FK" else self.cfg.recreate_workers)
        self.phase_finished("recreate", started)

    def phase_validate(self, loads):
        if self.flags.skip_validate or self.cfg.validate_method == "exit":
            return []
        counted = {v["step_id"] for v in self.state.validations()}
        for s in loads:
            if s.kind == "chunk" and s.id not in counted:
                self.count_chunked(s)
        # Measured again at the very end, so this second size includes the rebuilt indexes.
        specs = {f"{n}": sp for n, sp in enumerate(sp for s in loads for sp in self.specs_for(s))}
        sizes, _ = self.pg_values({i: sp.pg_size_sql() for i, sp in specs.items()})
        for i, sp in specs.items():
            if i in sizes:
                self.state.set_pg_bytes_after(sp.step_id, sp.table, sizes[i])
        mismatches = []
        for v in self.state.validations():
            name = f"{v['step_id'].split('/')[0].upper()}.{v['tbl']}"
            if v["ora_count"] != v["pg_count"] and name not in self.cfg.validate_ignore:
                mismatches.append(f"- {name}: oracle={v['ora_count']} postgres={v['pg_count']}")
        self.log(f"validation: {len(mismatches)} row-count mismatch(es)")
        for m in mismatches:
            self.log(m)
        return mismatches

    # ---------- step runners ----------

    def run_sql_step(self, row, workers):
        step_id = row["id"]
        todo = self.state.units(step_id, only_unfinished=True)
        if not todo:
            return False
        recreate = row["phase"] == "recreate"
        log_path = self.log_path(step_id)
        self.state.set_log(step_id, log_path)
        self.log(f"{step_id}: {len(todo)} statement(s), {workers} at a time")
        log_lock = threading.Lock()
        started, finished, in_flight, ended = time.time(), [0], {}, threading.Event()

        def heartbeat():
            # A single CREATE INDEX or ADD CONSTRAINT can run for hours on a loaded table.
            while self.cfg.heartbeat and not ended.wait(self.cfg.heartbeat):
                with log_lock:
                    oldest = min(in_flight.items(), key=lambda kv: kv[1], default=None)
                now = time.time()
                self.log(f"{step_id}: {finished[0]}/{len(todo)} statements finished after {human(now - started)}"
                         + (f" | running for {human(now - oldest[1])}: {oldest[0][:110]}" if oldest else ""))

        def work(unit):
            if self.stopping:
                return
            sql = unit["key"]
            with log_lock:
                in_flight[sql] = time.time()
            try:
                run_statement(sql)
            finally:
                with log_lock:
                    in_flight.pop(sql, None)
                    finished[0] += 1

        def run_statement(sql):
            self.state.set_unit(step_id, sql, "running", attempt=True)
            err = ""
            for attempt in range(1, self.cfg.attempts + 1):
                rc, _, err = self.psql(sql)
                if rc == 0 or (recreate and ALREADY_RE.search(err)) or (not recreate and NOOP_DROP_RE.search(err)):
                    self.state.set_unit(step_id, sql, "done")
                    return
                if self.stopping:
                    return
                if not self.is_transient(err) or attempt == self.cfg.attempts:
                    break
                try:
                    self.sleep(self.cfg.backoff_seconds * attempt)
                except Stopped:
                    return
            message = err.splitlines()[0] if err else "psql failed"
            self.state.set_unit(step_id, sql, "failed", error=message)
            with log_lock, open(log_path, "a") as f:
                f.write(f"{sql}\n  -> {err}\n")

        threading.Thread(target=heartbeat, daemon=True).start()
        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(work, todo))
        finally:
            ended.set()
        if self.stopping:
            raise Stopped()
        self.report_step(step_id, started)
        return True

    def test_count(self, step, tables):
        """Row counts on both sides for `tables`, or {} when TEST_COUNT is unusable."""
        conf = OraConf(step.conf_path).derive(self.cfg.work / (step.id.replace("/", "__") + ".count.conf"), tables)
        log_path = self.log_path(step.id, "count.log")
        self.log(f"{step.id}: counting rows on Oracle and PostgreSQL for {len(tables)} table(s) "
                 f"(ora2pg TEST_COUNT), log {log_path}")
        started = time.time()
        rc = self.exec_logged([self.cfg.ora2pg, "-t", "TEST_COUNT", "-c", str(conf)], step.conf_path.parent, log_path,
                              label=f"{step.id} row count", conf=OraConf(step.conf_path))
        counts = parse_test_count(log_path.read_text(errors="replace"))
        if rc != 0 or not counts:
            self.log(f"{step.id}: TEST_COUNT gave no usable counts (exit {rc}), see {log_path}")
        else:
            self.log(f"{step.id}: row count took {human(time.time() - started)}")
        return counts

    def run_conf_step(self, step):
        schema = step.id.split("/")[0].upper()
        for attempt in range(1, self.cfg.attempts + 1):
            todo = [u["key"] for u in self.state.units(step.id, only_unfinished=True)]
            if not todo:
                return
            conf = OraConf(step.conf_path).derive(self.cfg.work / (step.id.replace("/", "__") + ".conf"), todo)
            log_path = self.log_path(step.id)
            self.state.set_log(step.id, log_path)
            for t in todo:
                self.state.set_unit(step.id, t, "running", attempt=True)
            shown = ", ".join(todo[:5]) + (f" and {len(todo) - 5} more" if len(todo) > 5 else "")
            self.log(f"{step.id}: copying {len(todo)} table(s) with ora2pg ({shown}), log {log_path}")
            started = time.time()
            rc = self.exec_logged([self.cfg.ora2pg, "-c", str(conf)], step.conf_path.parent, log_path,
                                  label=f"{step.id} copy", conf=OraConf(step.conf_path))
            text = log_path.read_text(errors="replace")
            error = first_error(text) or (f"ora2pg exit code {rc}" if rc != 0 else "")
            self.log(f"{step.id}: copy ended after {human(time.time() - started)}"
                     + (f" with an error: {error}" if error else ", no error reported"))

            pairs = self.after_counts(step, todo)
            failed, results = 0, []
            for t in todo:
                pair = pairs.get(t.upper())
                ignored = f"{schema}.{t.upper()}" in self.cfg.validate_ignore
                if pair:
                    self.state.set_validation(step.id, t.upper(), *pair)
                    verdict = "equal" if pair[0] == pair[1] else ("different, ignored by config" if ignored else "DIFFERENT")
                    results.append((pair[0] != pair[1], f"{step.id}: {t}: oracle={pair[0]} postgres={pair[1]} {verdict}"))
                if pair and not ignored:
                    if pair[0] == pair[1]:
                        self.state.set_unit(step.id, t, "done", ora_count=pair[0], pg_count=pair[1])
                        continue
                    message = f"row count mismatch: oracle={pair[0]} postgres={pair[1]}"
                    self.state.set_unit(step.id, t, "failed", error=f"{message}. {error}".strip(),
                                        ora_count=pair[0], pg_count=pair[1])
                elif not error and (pair or self.cfg.validate_method != "direct"):
                    self.state.set_unit(step.id, t, "done")
                    continue
                else:
                    self.state.set_unit(step.id, t, "failed", error=error or "the rows could not be counted, so the "
                                                                          "load of this table is unconfirmed")
                failed += 1
            # Every differing table is listed; equal ones only when the list is short.
            listed = [line for differs, line in results if differs or len(results) <= 25]
            for line in listed:
                self.log(line)
            if len(listed) < len(results):
                self.log(f"{step.id}: {len(results) - len(listed)} other table(s) have equal counts")
            if not failed:
                return
            if not self.is_transient(text) or attempt == self.cfg.attempts:
                return
            self.log(f"{step.id}: {failed} table(s) failed on a transient error, retry {attempt + 1}/{self.cfg.attempts}")
            self.sleep(self.cfg.backoff_seconds * attempt)

    def record_finished_chunks(self, step, log_path, statuses=("running",)):
        """A chunk is done the moment the script reports it, not when the whole script ends, so a stop
        or a crash part-way through costs only the chunks that were still copying."""
        try:
            with open(log_path, errors="replace") as f:
                finished, _ = parse_chunk_output(f.read())
        except (OSError, TypeError):
            return
        for u in self.state.units(step.id, only_unfinished=True):
            if u["key"] in finished and u["status"] in statuses:
                self.state.set_unit(step.id, u["key"], "done")

    def chunk_rows_match(self, step, key):
        """ora2pg can report an error for a chunk whose rows all arrived. The rows decide."""
        if self.cfg.validate_method != "direct":
            return False
        spec = self.specs_for(step)[0]
        spec.filter = f"({key})"
        r = self.measure([spec], sizes=False, label=f"{step.id} chunk check")[spec.key]
        if r["ora"] is None or r["ora"] != r["pg"]:
            return False
        self.log(f"{step.id}: the script reported a failure for chunk [{key}], but Oracle and PostgreSQL both hold "
                 f"{r['ora']:,} rows in that range, so it is counted as done")
        return True

    def run_chunk_step(self, step):
        qualified = '"%s"."%s"' % (OraConf(step.conf_path).pg_schema(), step.table.lower())
        # After a kill that left no time to record anything, the last log still says which chunks finished.
        last_log = next((s["log"] for s in self.state.steps("load") if s["id"] == step.id), None)
        if last_log:
            self.record_finished_chunks(step, last_log, statuses=("interrupted",))
        retry_only = None
        for attempt in range(1, self.cfg.attempts + 1):
            units = self.state.units(step.id, only_unfinished=True)
            if retry_only is not None:
                units = [u for u in units if u["key"] in retry_only]
            todo = []
            for u in units:
                if u["attempts"] > 0:
                    # A chunk that ran before may have left rows behind; clear its range first.
                    rc, _, err = self.psql(f"DELETE FROM {qualified} WHERE ({u['key']})")
                    if rc != 0:
                        self.state.set_unit(step.id, u["key"], "failed",
                                            error=f"could not clear partial rows before retry: {err.splitlines()[0]}")
                        continue
                todo.append(u["key"])
            if not todo:
                return
            remaining = self.cfg.work / (step.id.replace("/", "__") + ".chunks.txt")
            remaining.write_text("\n".join(todo) + "\n")
            log_path = self.log_path(step.id)
            self.state.set_log(step.id, log_path)
            for key in todo:
                self.state.set_unit(step.id, key, "running", attempt=True)
            self.log(f"{step.id}: running {len(todo)} chunk(s) of {step.table} with {step.path.name}, log {log_path}")
            started = time.time()

            def progress():
                self.record_finished_chunks(step, log_path)
                finished, broken = parse_chunk_output(log_path.read_text(errors="replace"))
                return f"{len(finished)} of {len(todo)} chunks finished, {len(broken)} failed"

            def chunk_logs():   # each chunk writes its own log under the script's folder
                times = [f.stat().st_mtime for f in step.path.parent.glob("migration_logs*/*") if f.is_file()]
                return max(times, default=0)

            try:
                self.exec_logged(["bash", step.path.name], step.path.parent, log_path,
                                 env={"CHUNKS_FILE": str(remaining), "SHELL": shutil.which("bash") or "/bin/bash"},
                                 label=step.id, progress=progress, conf=OraConf(step.conf_path), activity=chunk_logs)
            finally:
                self.record_finished_chunks(step, log_path)
            done, failed = parse_chunk_output(log_path.read_text(errors="replace"))
            self.log(f"{step.id}: script ended after {human(time.time() - started)}: "
                     f"{len(done & set(todo))} of {len(todo)} chunks finished")

            retry_only = set()
            for key in todo:
                if key in done:
                    self.state.set_unit(step.id, key, "done")
                    continue
                if self.chunk_rows_match(step, key):
                    self.state.set_unit(step.id, key, "done")
                    continue
                chunk_log = step.path.parent / failed[key] if key in failed else None
                detail = chunk_log.read_text(errors="replace") if chunk_log and chunk_log.exists() else ""
                error = first_error(detail) or ("chunk failed, see " + str(chunk_log) if chunk_log
                                                else "script reported no result for this chunk")
                stalled = "dataload: stalled" in log_path.read_text(errors="replace") and key not in failed
                if stalled:
                    error = "the connection was lost while this chunk was copying (no progress, both databases idle)"
                self.state.set_unit(step.id, key, "failed", error=error)
                if stalled or self.is_transient(detail):
                    retry_only.add(key)
            if not retry_only or attempt == self.cfg.attempts:
                return
            self.log(f"{step.id}: {len(retry_only)} chunk(s) failed on a transient error, "
                     f"retry {attempt + 1}/{self.cfg.attempts}")
            self.sleep(self.cfg.backoff_seconds * attempt)
