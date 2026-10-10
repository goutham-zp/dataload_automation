"""Command line: plan, preflight, run, resume, status, retry."""
import argparse
import fcntl
import sys
from datetime import datetime

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
    p.add_argument("--force-recreate", action="store_true",
                   help="recreate constraints and indexes even if some load units are not done")
    p.add_argument("--allow-remaining-fks", action="store_true",
                   help="DANGEROUS: load even though foreign keys still reference the tables (TRUNCATE CASCADE)")


def flags_from(args):
    return Flags(skip_drop=set(KINDS) if args.skip_constraints or args.skip_drop else set(),
                 skip_recreate=set(KINDS) if args.skip_constraints or args.skip_recreate else set(),
                 skip_load=args.skip_load,
                 skip_validate=args.skip_validate, force_recreate=args.force_recreate,
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
    if info:
        print(f"Target {info['target']}  started {info['started']}  finished {info['finished'] or '-'}  "
              f"invocations {info['invocations']}")
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
    for row in state.steps():
        c = state.counts("s.id = ?", (row["id"],))
        total = sum(c.values())
        failed = sum(n for s, n in c.items() if s not in ("done", "pending", "running"))
        if args.failed and not failed:
            continue
        label = "done" if c.get("done", 0) == total else ("FAILED" if failed else
                                                          ("running" if c.get("running") else "pending"))
        print(f"{row['phase']:9} {row['id']:60} {c.get('done', 0):5}/{total:<5} {label}"
              + (f"  ({failed} failed)" if failed else ""))
    failed = state.failed_units()
    if failed:
        print(f"\n{len(failed)} unit(s) not done after an attempt:")
        for u in failed[: None if args.failed else 20]:
            print(f"- {u['step_id']} :: {u['key'][:100]}\n    [{u['status']}] {u['error'] or ''}\n    log: {u['log']}")
    return 0


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
    return Engine(cfg, State(cfg.state), flags_from(args)).run()


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
    status.add_argument("--counts", action="store_true", help="Oracle and PostgreSQL row counts per table")
    sub.add_parser("report", help="write the HTML report again from the recorded run")
    retry = sub.add_parser("retry", help="mark a whole step to run again on the next resume")
    retry.add_argument("step")
    args = parser.parse_args(argv)

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
