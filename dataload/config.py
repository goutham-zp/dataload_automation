"""Settings from dataload.toml plus secrets from a .env file beside it."""
import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_TRANSIENT = [
    r"ORA-08177",                      # can't serialize access for this transaction
    r"ORA-0311[34]",                   # end-of-file on communication channel / not connected
    r"ORA-03135",                      # connection lost contact
    r"ORA-12\d{3}",                    # TNS errors
    r"OCIStmtFetch",
    r"server closed the connection",
    r"could not connect to server",
    r"connection to server .* failed",
    r"deadlock detected",
    r"dataload: stalled",              # written by this tool when it stops a copy that lost its connection
]

DROP_ORDER = ["FK", "PK", "Other", "indexes"]
RECREATE_ORDER = ["Other", "PK", "indexes", "FK"]


@dataclass
class Config:
    root: Path
    content: Path
    state: Path
    logs: Path
    work: Path
    report: Path = None
    ora2pg: str = "ora2pg"
    psql: str = "psql"
    expected_target: str = ""
    exclude: list = field(default_factory=list)
    only: list = field(default_factory=list)
    attempts: int = 3
    backoff_seconds: int = 30
    stall_minutes: float = 15
    transient: list = field(default_factory=lambda: list(DEFAULT_TRANSIENT))
    recreate_workers: int = 4
    validate_method: str = "direct"
    before_counts: bool = True
    count_workers: int = 4
    oracle_count: str = ""
    validate_ignore: list = field(default_factory=list)
    check_perl_dbd_pg: bool = True
    heartbeat: int = 60
    hide_values: bool = True
    email: dict = field(default_factory=dict)


def load_env_file(path):
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def load(path):
    path = Path(path).resolve()
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    root = path.parent
    load_env_file(root / ".env")

    paths = raw.get("paths", {})
    tools = raw.get("tools", {})
    retry = raw.get("retry", {})
    validate = raw.get("validate", {})

    def resolve(key, default):
        return (root / paths.get(key, default)).resolve()

    return Config(
        root=root,
        content=resolve("content", "server_content"),
        state=resolve("state", "state/run.sqlite"),
        logs=resolve("logs", "logs"),
        work=resolve("work", "work"),
        report=resolve("report", "report.html"),
        ora2pg=tools.get("ora2pg", "ora2pg"),
        psql=tools.get("psql", "psql"),
        expected_target=raw.get("target", {}).get("expected", ""),
        exclude=[e.upper() for e in raw.get("order", {}).get("exclude", [])],
        attempts=int(retry.get("attempts", 3)),
        backoff_seconds=int(retry.get("backoff_seconds", 30)),
        stall_minutes=float(retry.get("stall_minutes", 15)),
        transient=retry.get("transient_patterns", list(DEFAULT_TRANSIENT)),
        recreate_workers=int(raw.get("recreate", {}).get("workers", 4)),
        validate_method=validate.get("method", "direct"),
        before_counts=bool(validate.get("before_counts", True)),
        count_workers=int(validate.get("workers", 4)),
        oracle_count=tools.get("oracle_count", ""),
        validate_ignore=[t.upper() for t in validate.get("ignore", [])],
        check_perl_dbd_pg=bool(raw.get("preflight", {}).get("perl_dbd_pg", True)),
        heartbeat=int(raw.get("output", {}).get("heartbeat_seconds", 60)),
        hide_values=bool(raw.get("output", {}).get("hide_values", True)),
        email=raw.get("email", {}),
    )
