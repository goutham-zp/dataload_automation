"""Command line: plan, preflight, run, resume, status, retry."""
import argparse
import fcntl
import os
import sys
from datetime import datetime
from pathlib import Path

from . import config, plan
from .conf import OraConf
from .runner import Engine, Flags
from .state import State

KINDS = ("FK", "PK", "Other", "indexes")


def add_run_flags(p):
    p.add_argument("--skip-constraints", action="store_true",
                   help="leave foreign keys, primary keys, other constraints and indexes alone: "
                        "no drop before the load and no recreate after it (--skip-drop and --skip-recreate together)")
    p.add_argument("--skip-drop", action="store_true",
                   help="do not drop constraints and indexes before the load (they are still recreated after it)")
    p.add_argument("--skip-recreate", action="store_true",
                   help="do not recreate constraints and indexes after the load")
    p.add_argument("--skip-load", action="store_true")
    p.add_argument("--skip-validate", action="store_true")
    p.add_argument("--no-skip", action="store_true",
                   help="on resume: run every phase, forgetting the skip flags the run was started with")
    p.add_argument("--force-recreate", action="store_true",
                   help="recreate constraints and indexes even if some load units are not done")
    p.add_argument("--allow-remaining-fks", action="store_true",
                   help="DANGEROUS: load even though foreign keys still reference the tables (TRUNCATE CASCADE)")


def skips_from(args):
    names = {"drop", "recreate"} if args.skip_constraints else set()
    return names | {name for name in ("drop", "recreate", "load", "validate") if getattr(args, f"skip_{name}")}


def flags_from(args, skipped):
    return Flags(skip_drop=set(KINDS) if "drop" in skipped else set(),
                 skip_recreate=set(KINDS) if "recreate" in skipped else set(),
                 skip_load="load" in skipped, skipped=set(skipped),
                 skip_validate="validate" in skipped, force_recreate=args.force_recreate,
                 allow_remaining_fks=args.allow_remaining_fks)


def cmd_plan(cfg, _args):
    steps = plan.build(cfg)
    loads = [s for s in steps if s.phase == "load"]
    targets = sorted({OraConf(s.conf_path).target() for s in loads})
    print(f"Target(s) in confs: {', '.join(targets)}")
    for phase in ("drop", "load", "recreate"):
        chosen = [s for s in steps if s.phase == phase]
        print(f"\n== {phase}: {len(chosen)} steps, {sum(len(s.units) for s in chosen)} units ==")
        for i, s in enumerate(chosen, 1):
            what = {"sql": "statements", "conf": "tables", "chunk": "chunks"}[s.kind]
            how = f"bash {s.path.name}" if s.kind == "chunk" else ("ora2pg -c" if s.kind == "conf" else "psql")
            print(f"{i:4}. {s.id:60} {len(s.units):5} {what:10} [{how}]")
    return 0


def cmd_status(cfg, args):
    if not cfg.state.exists():
        print("no run recorded yet")
        return 0
    state = State(cfg.state)
    info = state.run_info()
    if args.counts:
        rows = state.snapshots()
        print(f"\n{'table':44} {'oracle before':>15} {'pg before':>15} {'oracle after':>15} {'pg after':>15}")
        different = 0
        for r in rows:
            cells = [("-" if r[c] is None else f"{r[c]:,}") for c in ("ora_before", "pg_before", "ora_after", "pg_after")]
            differs = r["ora_after"] is not None and r["ora_after"] != r["pg_after"]
            different += differs
            name = f"{r['schema']}.{r['tbl']}" + (" (filtered)" if r["filter"] else "")
            print(f"{name:44} {cells[0]:>15} {cells[1]:>15} {cells[2]:>15} {cells[3]:>15}" + ("  DIFFERENT" if differs else ""))
        checked = sum(1 for r in rows if r["ora_after"] is not None)
        print(f"\n{len(rows)} table(s), {checked} compared after the load, {different} different")
        return 0
    skipped = state.skipped()
    active = run_is_active(cfg)
    finished = bool(info and info["finished"])
    if info:
        when = (f"finished {info['finished']}" if finished else
                "running now" if active else "not running (it stopped before the end)")
        print(f"Target    {info['target']}")
        print(f"Started   {info['started']}, {when}, started {info['invocations']} time(s)")
        if skipped:
            print("Flags     " + " ".join(f"--skip-{name}" for name in sorted(skipped)))

    def tally(where, args_):
        c = state.counts(where, args_)
        cut_off = c.get("interrupted", 0) + (0 if active else c.get("running", 0))
        return sum(c.values()), c.get("done", 0), c.get("failed", 0), c.get("running", 0) if active else 0, cut_off

    def describe(total, done, failed, running, cut_off, skip=False):
        if failed:
            return f"{failed:,} FAILED"
        if total and done == total:
            return "done"
        if skip:
            return "skipped by flag"
        if running:
            return "running"
        return "stopped part-way" if done or cut_off else "not started"

    steps = state.steps()
    if not args.failed:
        print(f"\n{'Phase':10} {'Done':>9}   {'Total':<9} State")
        for phase in ("drop", "load", "recreate"):
            numbers = tally("s.phase = ?", (phase,))
            if numbers[0]:
                print(f"{phase:10} {numbers[1]:>9,} / {numbers[0]:<9,} {describe(*numbers, skip=phase in skipped)}")
        print("\nLoad steps")
        for row in steps:
            numbers = tally("s.id = ?", (row["id"],))
            what = "chunks" if row["kind"] == "chunk" else "tables" if row["kind"] == "conf" else "statements"
            # Drop and recreate have dozens of steps each; only the ones needing attention are listed.
            if row["phase"] == "load" or args.all or numbers[2]:
                print(f"  {row['id']:58} {numbers[1]:>6,} / {numbers[0]:<6,} {what:10} "
                      f"{describe(*numbers, skip=row['phase'] in skipped)}")

    WORD = {"chunk": "chunk", "conf": "table", "sql": "statement"}

    def grouped(units):
        by_step = {}
        for u in units:
            by_step.setdefault(u["step_id"], []).append(u)
        return by_step

    failed = state.failed_units()
    if failed:
        print(f"\nFailed ({len(failed)}): fix the cause, then run 'resume'")
        limit = None if args.failed else 5
        for step_id, units in grouped(failed).items():
            print(f"  {step_id}")
            for u in units[:limit]:
                print(f"    {WORD[u['kind']] + ':':10} {u['key'][:110]}")
                print(f"    {'error:':10} {(u['error'] or 'no message recorded')[:300]}")
            if limit and len(units) > limit:
                print(f"    ... and {len(units) - limit} more in this step ('status --failed' lists all)")
            print(f"    {'log:':10} {units[0]['log']}")

    cut_off = state.failed_units(statuses=("interrupted",) if active else ("interrupted", "running"))
    if cut_off and not args.failed:
        print(f"\nStopped part-way ({len(cut_off)}): nothing to fix, 'resume' does these again")
        for step_id, units in grouped(cut_off).items():
            note = "  (its phase is skipped by flag, so it is left as it is)" if units[0]["phase"] in skipped else ""
            print(f"  {step_id}: {len(units)} {WORD[units[0]['kind']]}(s){note}")

    running = state.failed_units(statuses=("running",)) if active else []
    if running and not args.failed:
        print(f"\nRunning now ({len(running)})")
        for step_id, units in grouped(running).items():
            print(f"  {step_id}: {len(units)} {WORD[units[0]['kind']]}(s), log {units[0]['log']}")

    if not args.failed:
        left = sum(n for status, n in state.counts().items() if status != "done")
        if active:
            print("\nA run is in progress.")
        elif failed:
            print("\nNext: fix the failures above, then run 'python3 -m dataload resume'.")
        elif finished and not cut_off:
            print("\nNothing is waiting." if not left else "\nThe last start finished; the skipped phases were not run.")
        else:
            print("\nNext: 'python3 -m dataload resume' continues from here.")
    return 0


def run_is_active(cfg):
    """True while a run or resume holds the lock on the state file."""
    try:
        with open(str(cfg.state) + ".lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return False
    except BlockingIOError:
        return True
    except OSError:
        return False


def cmd_report(cfg, _args):
    if not cfg.state.exists():
        print("no run recorded yet")
        return 0
    loads = plan.load_steps(cfg)
    first = OraConf(loads[0].conf_path)
    engine = Engine(cfg, State(cfg.state), Flags())
    engine.target, engine.source = first.target(), first.get("ORACLE_DSN").split(":", 2)[-1]
    engine.write_report()
    print(f"report written: {cfg.report}")
    return 0


def cmd_retry(cfg, args):
    state = State(cfg.state)
    if not state.units(args.step):
        print(f"unknown step: {args.step} (see 'status' for step names)")
        return 2
    state.reset_step(args.step)
    print(f"{args.step}: every unit marked to run again; start it with 'resume'")
    return 0


def cmd_run(cfg, args):
    cfg.state.parent.mkdir(parents=True, exist_ok=True)
    lock = open(str(cfg.state) + ".lock", "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("another dataload run is already in progress (state file is locked)")
        return 2
    # A state file with no run row is left by a run that stopped at preflight; it is not a run.
    recorded = cfg.state.exists() and State(cfg.state).run_info() is not None
    if args.command == "run" and recorded:
        if not args.new:
            print(f"a run is already recorded in {cfg.state}. Use 'resume' to continue it, "
                  "or 'run --new' to archive it and start over.")
            return 2
        cfg.state.rename(cfg.state.with_name(f"{cfg.state.name}.{datetime.now():%Y%m%d-%H%M%S}.bak"))
    if args.command == "resume" and not recorded:
        print("nothing to resume; use 'run'")
        return 2
    state = State(cfg.state)
    skipped = skips_from(args)
    if args.command == "resume" and not skipped and not args.no_skip and state.skipped():
        # A resume continues the run as it was started unless told otherwise.
        skipped = state.skipped()
        print("continuing with the flags this run was started with: "
              + " ".join(f"--skip-{name}" for name in sorted(skipped)) + "  (--no-skip runs every phase)")
    return Engine(cfg, state, flags_from(args, skipped)).run()


def cmd_preflight(cfg, _args):
    engine = Engine(cfg, None, Flags())
    loads = plan.load_steps(cfg)
    first = OraConf(loads[0].conf_path)
    engine.pg_env, engine.target = first.pg_env(), first.target()
    problems, warnings = engine.preflight(loads)
    for w in warnings:
        print(f"warning: {w}")
    for p in problems:
        print(f"FAILED:  {p}")
    print("preflight passed" if not problems else f"{len(problems)} problem(s)")
    return 2 if problems else 0


def main(argv=None):
    parser = argparse.ArgumentParser(prog="dataload", description="Resumable ora2pg data load")
    parser.add_argument("--config", default="dataload.toml")
    parser.add_argument("--only", action="append", default=[], metavar="FOLDER",
                        help="limit everything to this schema folder (repeatable), e.g. --only SFTP_CONFIG")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("plan", help="show what would run, without running anything")
    sub.add_parser("preflight", help="check tools, connections and inputs")
    run = sub.add_parser("run", help="start a new run")
    run.add_argument("--new", action="store_true", help="archive the recorded run and start over")
    add_run_flags(run)
    add_run_flags(sub.add_parser("resume", help="continue the recorded run, doing only what is not done"))
    status = sub.add_parser("status", help="show progress")
    status.add_argument("--failed", action="store_true", help="only steps with failures, all failed units")
    status.add_argument("--all", action="store_true", help="list every drop and recreate step too")
    status.add_argument("--counts", action="store_true", help="Oracle and PostgreSQL row counts per table")
    sub.add_parser("report", help="write the HTML report again from the recorded run")
    retry = sub.add_parser("retry", help="mark a whole step to run again on the next resume")
    retry.add_argument("step")
    args = parser.parse_args(argv)

    try:
        os.getcwd()
    except FileNotFoundError:
        print("The folder this shell is in no longer exists (it was deleted or rebuilt). "
              "Change into it again, for example: cd \"$PWD\"")
        return 2
    if not Path(args.config).is_file():
        print(f"Settings file not found: {Path(args.config).resolve()}\n"
              "Run this from the folder that holds dataload.toml, or pass --config PATH before the command.")
        return 2
    cfg = config.load(args.config)
    cfg.only = [name.upper() for name in args.only]
    try:
        return {"plan": cmd_plan, "preflight": cmd_preflight, "run": cmd_run, "resume": cmd_run,
                "status": cmd_status, "retry": cmd_retry, "report": cmd_report}[args.command](cfg, args)
    except plan.PlanError as e:
        print(f"plan error: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
