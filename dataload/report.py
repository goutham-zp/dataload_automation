"""The HTML report: every table's rows and size on both sides, before and after the load.

One self-contained file with no external resources, so it can be mailed or opened from disk.
"""
from datetime import datetime
from html import escape

from .counts import human_bytes
from .redact import redact

CSS = """
:root { --ink:#1c2430; --muted:#5f6b7a; --line:#dfe4ea; --bg:#f5f7fa; --card:#ffffff; --head:#17324d;
        --ok:#1f7a4d; --ok-bg:#e3f4ea; --bad:#b3261e; --bad-bg:#fbe6e4; --wait:#8a5a00; --wait-bg:#fdf1d6; }
* { box-sizing: border-box; }
body { margin:0; background:var(--bg); color:var(--ink);
       font:14px/1.45 -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
header { background:var(--head); color:#fff; padding:28px 32px; }
header h1 { margin:0 0 6px; font-size:22px; font-weight:600; }
header p { margin:2px 0; color:#c9d6e3; }
main { padding:24px 32px 48px; max-width:1500px; margin:0 auto; }
h2 { font-size:16px; margin:32px 0 12px; }
.pill { display:inline-block; padding:2px 10px; border-radius:999px; font-weight:600; font-size:12px; white-space:nowrap; }
.ok { color:var(--ok); background:var(--ok-bg); } .bad { color:var(--bad); background:var(--bad-bg); }
.wait { color:var(--wait); background:var(--wait-bg); }
header .pill { font-size:13px; padding:4px 14px; margin-left:12px; vertical-align:middle; }
.tiles { display:grid; grid-template-columns:repeat(auto-fit, minmax(190px, 1fr)); gap:12px; }
.tile { background:var(--card); border:1px solid var(--line); border-radius:8px; padding:14px 16px; }
.tile .label { color:var(--muted); font-size:12px; }
.tile .value { font-size:22px; font-weight:600; margin-top:4px; font-variant-numeric:tabular-nums; }
.tile .note { color:var(--muted); font-size:12px; margin-top:2px; }
.scroll { overflow-x:auto; background:var(--card); border:1px solid var(--line); border-radius:8px; }
table { border-collapse:collapse; width:100%; }
th, td { padding:8px 12px; border-bottom:1px solid var(--line); text-align:left; white-space:nowrap; }
th { background:#eef2f6; font-size:12px; color:var(--muted); font-weight:600; position:sticky; top:0; }
td.n, th.n { text-align:right; font-variant-numeric:tabular-nums; }
tr:last-child td { border-bottom:0; }
tr.problem td { background:#fff8f7; }
.group th { text-align:center; border-left:1px solid var(--line); }
.muted { color:var(--muted); } .small { font-size:12px; }
.controls { margin:0 0 10px; display:flex; gap:16px; align-items:center; flex-wrap:wrap; }
.controls input[type=search] { padding:6px 10px; border:1px solid var(--line); border-radius:6px; width:260px; font:inherit; }
ul.issues { background:var(--card); border:1px solid var(--line); border-radius:8px; margin:0; padding:12px 16px 12px 32px; }
ul.issues li { margin:4px 0; } code { font-size:12px; }
footer { color:var(--muted); font-size:12px; margin-top:28px; }
@media print { header { -webkit-print-color-adjust:exact; print-color-adjust:exact; } .controls { display:none; } }
"""

JS = """
var q = document.getElementById("q"), only = document.getElementById("only");
function apply() {
  var text = q.value.toLowerCase();
  document.querySelectorAll("#tables tbody tr").forEach(function (row) {
    var show = row.textContent.toLowerCase().indexOf(text) >= 0 && (!only.checked || row.className === "problem");
    row.style.display = show ? "" : "none";
  });
}
q.addEventListener("input", apply); only.addEventListener("change", apply);
"""


def num(value):
    return "" if value is None else f"{value:,}"


def pill(kind, text):
    return f'<span class="pill {kind}">{escape(text)}</span>'


def table_rows(state):
    """One dict per table, with its result worked out from the snapshot and the unit records."""
    units = {}
    for step in state.steps("load"):
        for u in state.units(step["id"]):
            units.setdefault(step["id"], []).append(u)
    kinds = {s["id"]: s["kind"] for s in state.steps("load")}
    rows = []
    for snap in state.snapshots():
        step_units = units.get(snap["step_id"], [])
        chunked = kinds.get(snap["step_id"]) == "chunk"
        if chunked:
            done = sum(1 for u in step_units if u["status"] == "done")
            how = f"{len(step_units)} chunks"
            failed = [u for u in step_units if u["status"] == "failed"]
            finished = bool(step_units) and done == len(step_units)
            progress = f"{done} of {len(step_units)} chunks loaded"
        else:
            unit = next((u for u in step_units if u["key"].upper() == snap["tbl"]), None)
            how = "filtered" if snap["filter"] else "whole table"
            failed = [unit] if unit and unit["status"] == "failed" else []
            finished = bool(unit) and unit["status"] == "done"
            progress = "not loaded yet"
        ora, pg = snap["ora_after"], snap["pg_after"]
        if failed:
            result, kind = "Failed", "bad"
        elif ora is not None and pg is not None and ora != pg:
            result, kind = "Different", "bad"
        elif finished and ora is not None and ora == pg:
            result, kind = "Match", "ok"
        elif finished:
            result, kind = "Loaded, not counted", "wait"
        else:
            result, kind = progress[0].upper() + progress[1:], "wait"
        rows.append({"snap": snap, "how": how, "result": result, "kind": kind,
                     "error": (failed[0]["error"] or failed[0]["status"]) if failed else ""})
    return rows


def build(state, cfg, target, source):
    info = state.run_info()
    rows = table_rows(state)
    hide = redact if cfg.hide_values else (lambda text: text)
    failed_units = state.failed_units()
    matched = sum(1 for r in rows if r["kind"] == "ok")
    bad = sum(1 for r in rows if r["kind"] == "bad")
    waiting = len(rows) - matched - bad
    finished = bool(info and info["finished"])
    if bad or failed_units:
        overall = pill("bad", "Needs attention")
    elif finished and waiting == 0:
        overall = pill("ok", "Completed, all tables match")
    else:
        overall = pill("wait", "In progress" if not finished else "Incomplete")

    def total(column):
        return sum(r["snap"][column] or 0 for r in rows)

    duration = ""
    if info and info["started"]:
        end = datetime.strptime(info["finished"], "%Y-%m-%d %H:%M:%S") if finished else datetime.now()
        seconds = int((end - datetime.strptime(info["started"], "%Y-%m-%d %H:%M:%S")).total_seconds())
        duration = f"{seconds // 3600}h {seconds % 3600 // 60:02d}m" if seconds >= 3600 else f"{seconds // 60}m {seconds % 60:02d}s"

    tiles = [
        ("Tables in this load", num(len(rows)), f"{matched} match, {bad} with a problem, {waiting} pending"),
        ("Rows in Oracle (source)", num(total("ora_after") if matched + bad else total("ora_before")),
         "counted after the load" if matched + bad else "counted before the load"),
        ("Rows in PostgreSQL before", num(total("pg_before")), human_bytes(total("pg_bytes_before")) + " on disk"),
        ("Rows in PostgreSQL after", num(total("pg_after")),
         (human_bytes(total("pg_bytes_loaded")) + " on disk before indexes"
          + (", " + human_bytes(total("pg_bytes_after")) + " with indexes" if total("pg_bytes_after") else ""))
         if total("pg_bytes_loaded") else "size is measured as each table loads"),
        ("Elapsed", duration or "-", f"{info['invocations']} start(s) of the run" if info else ""),
    ]

    phases = []
    for phase, label in (("drop", "Drop constraints and indexes"), ("load", "Load data"),
                         ("recreate", "Recreate constraints and indexes")):
        c = state.counts("s.phase = ?", (phase,))
        count_all = sum(c.values())
        if not count_all:
            continue
        done, failed = c.get("done", 0), c.get("failed", 0)
        if failed:
            state_pill = pill("bad", f"{failed} failed")
        elif done == count_all:
            state_pill = pill("ok", "Done")
        elif phase in state.skipped():
            state_pill = '<span class="muted">Skipped for this run</span>'
        else:
            state_pill = pill("wait", "Not finished")
        phases.append(f"<tr><td>{label}</td><td class=n>{num(done)}</td><td class=n>{num(count_all)}</td>"
                      f"<td class=n>{num(failed)}</td><td>{state_pill}</td></tr>")

    body = []
    for r in sorted(rows, key=lambda r: (r["kind"] != "bad", r["snap"]["schema"], r["snap"]["tbl"])):
        s = r["snap"]
        diff = None if s["ora_after"] is None or s["pg_after"] is None else s["pg_after"] - s["ora_after"]
        how = escape(r["how"])
        if s["filter"]:
            how = f'<span title="{escape(s["filter"][:600])}">{how} &#9432;</span>'
        body.append(
            f'<tr class="{"problem" if r["kind"] == "bad" else ""}"><td>{escape(s["schema"])}</td>'
            f'<td>{escape(s["tbl"])}</td><td>{how}</td>'
            f'<td class=n>{num(s["ora_before"])}</td><td class=n>{num(s["pg_before"])}</td>'
            f'<td class=n>{num(s["ora_after"])}</td><td class=n>{num(s["pg_after"])}</td>'
            f'<td class=n>{"" if diff is None else f"{diff:+,}" if diff else "0"}</td>'
            f'<td class=n>{human_bytes(s["ora_bytes"])}</td><td class=n>{human_bytes(s["pg_bytes_before"])}</td>'
            f'<td class=n>{human_bytes(s["pg_bytes_loaded"])}</td><td class=n>{human_bytes(s["pg_bytes_after"])}</td>'
            f'<td>{pill(r["kind"], r["result"])}</td><td class="muted small">{escape(s["after_at"] or "")}</td></tr>')

    issues = [f"<li><strong>{escape(r['snap']['schema'])}.{escape(r['snap']['tbl'])}</strong>: "
              f"{escape(r['result'])}{' &mdash; ' + escape(hide(r['error'])[:300]) if r['error'] else ''}</li>"
              for r in rows if r["kind"] == "bad"]
    other = [u for u in failed_units if u["phase"] != "load"]
    issues += [f"<li>{escape(u['phase'])} step <code>{escape(u['step_id'])}</code>: <code>{escape(u['key'][:140])}</code>"
               f" &mdash; {escape(hide(u['error'] or u['status'])[:300])}</li>" for u in other[:40]]
    if len(other) > 40:
        issues.append(f"<li>and {len(other) - 40} more failed statements; see <code>status --failed</code></li>")

    tiles_html = "".join(f'<div class="tile"><div class="label">{escape(a)}</div><div class="value">{escape(b)}</div>'
                         f'<div class="note">{escape(c)}</div></div>' for a, b, c in tiles)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Data load report</title><style>{CSS}</style></head><body>
<header><h1>Oracle to PostgreSQL data load {overall}</h1>
<p>Source: {escape(source or "-")}</p><p>Target: {escape(target or "-")}</p>
<p>Started {escape(info["started"] if info else "-")} &middot; {("Finished " + escape(info["finished"])) if finished else "Not finished"}
 &middot; Report written {datetime.now():%Y-%m-%d %H:%M:%S}</p></header>
<main>
<div class="tiles">{tiles_html}</div>
{"<h2>Needs attention</h2><ul class=issues>" + "".join(issues) + "</ul>" if issues else ""}
<h2>Phases</h2>
<div class="scroll"><table><thead><tr><th>Phase</th><th class=n>Done</th><th class=n>Total</th><th class=n>Failed</th>
<th>State</th></tr></thead><tbody>{"".join(phases)}</tbody></table></div>
<h2>Tables: rows and size before and after</h2>
<div class="controls"><input id="q" type="search" placeholder="Filter by schema or table">
<label><input id="only" type="checkbox"> Only tables with a problem</label></div>
<div class="scroll"><table id="tables"><thead>
<tr class="group"><th colspan=3></th><th colspan=2>Rows before the load</th><th colspan=3>Rows after the load</th>
<th colspan=4>Size on disk</th><th colspan=2></th></tr>
<tr><th>Schema</th><th>Table</th><th>Loaded as</th><th class=n>Oracle</th><th class=n>PostgreSQL</th>
<th class=n>Oracle</th><th class=n>PostgreSQL</th><th class=n>Difference</th><th class=n>Oracle</th>
<th class=n>PostgreSQL before</th><th class=n>PostgreSQL after load</th><th class=n>PostgreSQL with indexes</th><th>Result</th><th>Checked</th></tr></thead>
<tbody>{"".join(body)}</tbody></table></div>
<footer><p>Rows are exact <code>count(1)</code> results taken on both databases. For a filtered table, Oracle is
counted with the same filter the load used. For a chunked table, both sides are counted inside the loaded ranges only
(hover the &#9432; mark to see the filter). Oracle size is the table and its LOB segments without indexes.
"PostgreSQL after load" is measured as soon as the table's rows are in, before its indexes are rebuilt, so it compares
with the Oracle size; "PostgreSQL with indexes" is measured at the end of the run.</p></footer>
</main><script>{JS}</script></body></html>
"""
