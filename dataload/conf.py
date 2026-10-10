"""Read an ora2pg conf file and write narrowed copies of it. Originals are never edited."""
import re
from pathlib import Path

TABLE_KEYS = ("ALLOW", "TABLE")


class OraConf:
    def __init__(self, path):
        self.path = Path(path)
        self.lines = self.path.read_text(errors="replace").splitlines()

    def get(self, key, default=""):
        for line in self.lines:
            parts = line.split(None, 1)
            if parts and parts[0] == key:
                return parts[1].strip() if len(parts) > 1 else ""
        return default

    def tables(self):
        for key in ("TABLE", "ALLOW"):
            value = self.get(key)
            if value:
                return value.split()
        return []

    def _dsn(self):
        dsn = self.get("PG_DSN")
        return dict(p.split("=", 1) for p in dsn.split(":", 2)[-1].split(";") if "=" in p)

    def target(self):
        d = self._dsn()
        return f"{d.get('host', '')}:{d.get('port', '5432')}/{d.get('dbname', '')}"

    def pg_env(self):
        d = self._dsn()
        return {
            "PGHOST": d.get("host", ""),
            "PGPORT": d.get("port", "5432"),
            "PGDATABASE": d.get("dbname", ""),
            "PGUSER": self.get("PG_USER"),
            "PGPASSWORD": self.get("PG_PWD"),
        }

    def pg_schema(self):
        return self.get("PG_SCHEMA").lower()

    def derive(self, dest, tables):
        """Write a copy whose ALLOW/TABLE lines list only `tables`."""
        out, seen = [], False
        for line in self.lines:
            m = re.match(r"^(ALLOW|TABLE)(\s+)", line)
            if m:
                seen = True
                out.append(f"{m.group(1)}{m.group(2)}{' '.join(tables)}")
            else:
                out.append(line)
        if not seen:
            out.append(f"ALLOW  {' '.join(tables)}")
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("\n".join(out) + "\n")
        return dest
