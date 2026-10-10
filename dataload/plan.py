"""Turn the server_content folder into an ordered list of steps."""
import re
from dataclasses import dataclass, field
from pathlib import Path

from .conf import OraConf
from .config import DROP_ORDER, RECREATE_ORDER

GENERATED_DIR = "generated_scripts_for_all_schema"
SQL_FILES = {
    ("drop", "FK"): "FK_drop_script.sql",
    ("drop", "PK"): "PK_drop_script.sql",
    ("drop", "Other"): "Other_drop_script.sql",
    ("drop", "indexes"): "all_indexes_drop.sql",
    ("recreate", "FK"): "FK_recreate_script.sql",
    ("recreate", "PK"): "PK_recreate_script.sql",
    ("recreate", "Other"): "Other_recreate_script.sql",
    ("recreate", "indexes"): "all_indexes_create.sql",
}


class PlanError(Exception):
    pass


@dataclass
class Step:
    id: str
    phase: str              # drop | load | recreate
    kind: str               # sql | conf | chunk
    schema: str             # lower-case target schema folder name
    units: list = field(default_factory=list)
    path: Path = None       # conf file or shell script
    conf_path: Path = None  # the ora2pg conf this step uses
    table: str = ""         # chunk steps: the table being loaded
    chunks_file: Path = None
    chunks_overridable: bool = False
    sql_kind: str = ""      # sql steps: FK | PK | Other | indexes


def conf_sort_key(path):
    """Numbered confs in number order, unnumbered ones after them."""
    number = re.search(r"(\d+)$", path.stem)
    if number:
        return (0, int(number.group(1)), path.name)
    return (1, 0, path.name)


def read_lines(path):
    return [l.strip() for l in path.read_text().splitlines() if l.strip() and not l.strip().startswith("#")]


def parse_script(script):
    text = script.read_text()

    def var(name):
        m = re.search(rf'^{name}="(.*?)"', text, re.M)
        if not m:
            raise PlanError(f"{script}: {name} not found")
        return m.group(1)

    chunks = var("CHUNKS_FILE")
    m = re.fullmatch(r"\$\{CHUNKS_FILE:-(.*)\}", chunks)
    return {
        "conf": (script.parent / var("CONFIG_FILE")).resolve(),
        "table": var("TABLE_NAME"),
        "chunks": (script.parent / (m.group(1) if m else chunks)).resolve(),
        "overridable": bool(m),
    }


def _walk(folder, content, schema, steps):
    scripts = sorted(folder.glob("*.sh"))
    if len(scripts) > 1:
        raise PlanError(f"{folder}: more than one shell script, cannot tell which to run")
    if scripts:
        info = parse_script(scripts[0])
        steps.append(Step(
            id=str(scripts[0].relative_to(content)), phase="load", kind="chunk", schema=schema,
            units=read_lines(info["chunks"]) if info["chunks"].exists() else [],
            path=scripts[0], conf_path=info["conf"], table=info["table"],
            chunks_file=info["chunks"], chunks_overridable=info["overridable"],
        ))
    else:
        for conf in sorted(folder.glob("*.conf"), key=conf_sort_key):
            steps.append(Step(
                id=str(conf.relative_to(content)), phase="load", kind="conf", schema=schema,
                units=OraConf(conf).tables(), path=conf, conf_path=conf,
            ))
    for sub in sorted(p for p in folder.iterdir() if p.is_dir() and not p.name.startswith("migration_logs")):
        _walk(sub, content, schema, steps)


def load_steps(cfg):
    content = cfg.content
    folders = {p.name.upper(): p for p in content.iterdir() if p.is_dir() and p.name != GENERATED_DIR}
    order = []
    list_file = content / "schema_list.txt"
    for name in (read_lines(list_file) if list_file.exists() else []):
        if name.upper() in folders and name.upper() not in order:
            order.append(name.upper())
    order += sorted(n for n in folders if n not in order)

    steps = []
    for name in order:
        if name in cfg.exclude or (cfg.only and name not in cfg.only):
            continue
        _walk(folders[name], content, name.lower(), steps)
    return steps


def split_statements(text):
    """One statement per ';' at end of line, which is how the generated files are written."""
    statements, current = [], []
    for line in text.splitlines():
        if not line.strip() or line.strip().startswith("--"):
            continue
        current.append(line)
        if line.rstrip().endswith(";"):
            statements.append("\n".join(current).strip())
            current = []
    if current:
        statements.append("\n".join(current).strip())
    return statements


def sql_steps(cfg, schemas):
    steps = []
    for phase, order in (("drop", DROP_ORDER), ("recreate", RECREATE_ORDER)):
        # Kind first, schema second: foreign keys cross schemas, so every schema's FKs
        # must be gone before any primary key is dropped, and back only after all keys exist.
        for kind in order:
            for schema in schemas:
                folder = cfg.content / GENERATED_DIR / f"{schema}_generated_scripts"
                path = folder / SQL_FILES[(phase, kind)]
                if not path.exists():
                    continue
                steps.append(Step(
                    id=f"{phase}/{schema}/{kind}", phase=phase, kind="sql", schema=schema,
                    units=split_statements(path.read_text()), path=path, sql_kind=kind,
                ))
    return steps


def build(cfg):
    loads = load_steps(cfg)
    schemas = list(dict.fromkeys(s.schema for s in loads))
    sql = sql_steps(cfg, schemas)
    return [s for s in sql if s.phase == "drop"] + loads + [s for s in sql if s.phase == "recreate"]
