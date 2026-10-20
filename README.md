# dataload

Runs the Oracle to PostgreSQL data copy (ora2pg) step by step, records what finished, emails the outcome, and
continues from where it stopped.

It does not replace ora2pg or the confs. It runs the same confs and `run_*.sh` scripts you would run by hand, in
the order of `schema_list.txt`, and keeps a record of every table and chunk.

## What a run does

| Phase | What happens | Skip with |
|---|---|---|
| before | Counts rows and measures size of every table on both sides | `before_counts = false` |
| drop | Drops foreign keys, primary keys, other constraints, indexes | `--skip-drop` |
| load | Runs every conf and every `run_*.sh`, then compares row counts per table | `--skip-load` |
| recreate | Recreates what the drop phase removed | `--skip-recreate` |
| validate | Final row counts and sizes, writes `report.html` | `--skip-validate` |

`--skip-constraints` is `--skip-drop` and `--skip-recreate` together. Use it when constraints are dropped and
recreated by hand.

## Setup on the server

1. Check the tools. Python must be 3.11 or newer.

   ```
   python3 --version
   which ora2pg psql parallel tmux
   perl -MDBD::Oracle -MDBD::Pg -e 1
   ```

2. Put the `dataload/` folder and `dataload.toml` in one working folder. Every command below is run from that
   folder.

3. In `dataload.toml`:
   - `[paths] content`: the folder that holds `schema_list.txt` and the schema folders.
   - `[paths] state`: keep it on a local disk.
   - `[target] expected`: the PostgreSQL the confs point at, as `host:port/dbname`. This is a safety check.
     If the confs point anywhere else, nothing runs.
   - `[email]`: see below.

4. Each `run_*.sh` needs two small changes. Preflight refuses a script without the first one.

   ```bash
   CHUNKS_FILE="${CHUNKS_FILE:-./chunks.txt}"
   ```

   ```bash
   local chunk_tmp
   chunk_tmp=$(mktemp -d)
   TMPDIR="$chunk_tmp" ora2pg -c "$CONFIG_FILE" ... > "$log_file" 2>&1
   local rc=$?
   rm -rf "$chunk_tmp"
   if [ $rc -eq 0 ]; then
   ```

   The second one matters: without its own temp folder, ora2pg reports a failure for a chunk that loaded
   correctly whenever another ora2pg is running.

5. Create `.env` beside `dataload.toml` and protect it.

   ```
   SMTP_LOGIN=apikey
   SMTP_PASSWORD=<the key>
   ```

   ```
   chmod 600 .env
   ```

6. Check without changing anything.

   ```
   python3 -m dataload plan
   python3 -m dataload preflight
   ```

### Email

Any SMTP relay on port 587 works. Set `enabled = true`, `host`, `from` and `to` in `[email]`.

| Provider | `host` | `SMTP_LOGIN` | `SMTP_PASSWORD` |
|---|---|---|---|
| SendGrid | `smtp.sendgrid.net` | the literal word `apikey` | a SendGrid API key with Mail Send permission |
| Brevo | `smtp-relay.brevo.com` | the SMTP login | the SMTP key (`xsmtpsib-...`) |

`from` must be a sender the provider has verified. The server must be allowed to reach the host on port 587.
A mail that cannot be sent is written to the log and never stops the load.

## Before the load starts

- The tables loaded by a `run_*.sh` are **not emptied** by the load (their confs have no `TRUNCATE_TABLE`).
  They must be empty on the target, or the rows are added on top of what is there. The before phase warns
  when it finds rows.
- Tables loaded by a plain conf are emptied with `TRUNCATE ... CASCADE`. If a foreign key still points at one
  of them, the load refuses to start, because the cascade would empty the referencing table too. Drop the
  foreign keys first.
- Insert triggers on the target fire during the load unless they are disabled.
- Sequences are not touched by this tool.

## Running

Start inside tmux so that a dropped SSH session does not stop the load.

```
tmux new -s dataload
python3 -m dataload run --skip-constraints
```

Leave tmux with `Ctrl+B` then `D`. Come back with `tmux attach -t dataload`.

| Command | Use |
|---|---|
| `python3 -m dataload status` | Where the run is, and what to do next |
| `python3 -m dataload status --failed` | Every failure with its full error text |
| `python3 -m dataload status --counts` | Oracle and PostgreSQL row counts per table |
| `python3 -m dataload resume` | Continue. Does only what is not done, with the same skip flags |
| `python3 -m dataload retry <step>` | Mark one whole step to run again on the next `resume` |
| `python3 -m dataload report` | Write `report.html` again |
| `python3 -m dataload run --new` | Archive the recorded run and start over |

`--only FOLDER` before the command limits everything to one schema folder.

## Lanes: load steps side by side

By default the load steps run one at a time. With `[lanes] enabled = true` in `dataload.toml`, the load steps are
split into lanes: each lane runs its steps one at a time, in the order listed, and all lanes run at once. The
load ends when the slowest lane ends. Drop, recreate and validation run as before, so constraints and indexes come
back only after every lane has finished.

```toml
[lanes]
enabled = true
lane1 = ["FTS/ora2pg_set_5.conf"]
lane2 = ["FTS/ora2pg_set_3.conf", "FTS/ora2pg_set_4.conf", "FTS/ora2pg_set_6.conf"]
lane3 = ["FTS/A_EDI_DATA/run_A_EDI_DATA_1.sh", "..."]
lane4 = ["FTS/ora2pg_set_7.conf", "*"]
```

- Steps are named as `plan` lists them. `"*"` stands for every load step that no other lane names, in the usual
  order. Lane names are free (`lane1`, `big`, ...).
- `plan` shows the lanes. `plan`, `preflight` and `run` refuse a layout with an unknown name, a step in two lanes,
  a step in no lane, or one table loaded in two lanes (each step empties its tables first, so two lanes would undo
  each other's rows). A table loaded by two steps of one lane is allowed, as without lanes.
- Chunk scripts in more than one lane give a warning: each script alone opens about
  `PARALLEL_JOBS x (ORA_JOBS + PG_JOBS)` connections on both databases.
- A failed step does not stop its lane or the others. Each failure is mailed as usual.
- `Ctrl+C` stops every lane. `resume` does only what is not done, with the lanes as they are in `dataload.toml`
  at that moment, so the layout can be changed between a stop and a resume.
- `status` shows the lane of every load step. The log names the lane on each step's first line
  (`lane2: step 2 of 3: FTS/ora2pg_set_4.conf, ...`) and when a lane finishes.
- With lanes on, `--allow-remaining-fks` is refused: a `TRUNCATE ... CASCADE` in one lane could empty a table
  another lane is loading.
- The stall check (no output for 15 minutes and both databases idle) only sees the whole database. While other
  lanes keep the databases busy, a copy that lost its connection is not stopped until every lane is quiet. Watch
  the "still running" lines of each step.

Total connections are the sum over every step running at the same moment. Check them against PostgreSQL's
`max_connections` and the Oracle account's session limit before the first run with lanes.

## Sequences after the load

A data load copies rows with their IDs, so it never moves a sequence. `dataload.sequences` sets every
PostgreSQL sequence to Oracle's value, or past its own data when that is further, and lists everything it would not
decide on its own for a person, with the reason and a suggested fix. It is a separate command, not a phase: run it
once Oracle writes have stopped, after the load, and again after any reload.

```
python3 -m dataload.sequences plan          # dry run: what would change and why; changes nothing
python3 -m dataload.sequences apply         # asks, sets what is behind Oracle, then runs the check
python3 -m dataload.sequences apply --yes   # the same without asking (needed when there is no terminal)
python3 -m dataload.sequences check         # compares each sequence with the highest value in its columns
python3 -m dataload.sequences undo logs/sequences-apply-<time>.csv   # puts back what that apply set
```

- **Matching.** Named sequences are matched by schema and name. Identity columns are matched by table and column,
  because their names never match (`ISEQ$$_647701` on Oracle, `segment_rule_history_history_id_seq` here).
  Schemas are the `SCHEMA` / `PG_SCHEMA` pairs in the confs; connections come from the confs too.
- **Value.** Oracle's `LAST_NUMBER`, so the next insert gets exactly that number. A sequence already at or past
  it is left alone: apply never moves anything back (for a descending sequence, "back" is up). Running again is safe and
  only moves what fell behind.
- **Past its data.** Every sequence is also compared with the highest value in the columns it feeds (identity, OWNED BY,
  column default, `sequence_links.txt`; all-nines marker rows are stepped over). When rows already hold numbers it
  would hand out (Oracle inserts some IDs by hand), it is raised to that highest value plus one, but only when those
  rows look like its own run: they start in the lower half of the range above it and fill at least 1 in 100 of its
  numbers. A sparse or far block (reserved IDs, a few typed in by hand) is listed for a person instead, with how soon
  the sequence runs into it.
- **Refused, with the reason in the output:** Oracle's value outside the PostgreSQL sequence's range (an `integer`
  sequence), a different increment, a PostgreSQL sequence two Oracle sequences point at, no UPDATE privilege.
- **Only on Oracle:** listed, never created.
- **Only on PostgreSQL:** set from its data by the same rule, never moved back. Left alone when its columns are empty;
  listed for a person when no column is linked or its columns hold all-nines marker rows. Never dropped.
- **Needs a person.** `plan`, `apply` and `check` end with one list, most urgent first: failed, refused, rows already
  taken, a sparse block above the sequence, marker rows, a column not read in time or not a number, a
  `sequence_links.txt` line naming a missing sequence, a column linked to more than one sequence, a sequence only on
  PostgreSQL that nothing links to a column, only on Oracle, and schemas on the target that no conf names (never read
  or set). Each has the reason and a suggested fix, often the exact SQL. All of them are in
  `logs/sequences-manual-*.csv`; the screen shows up to 25 of each kind.
- **Undo.** `undo` reads an apply's CSV and moves each sequence that apply set back to its value before. A sequence
  used or moved since the apply is skipped, because moving it back could hand out numbers already used. It refuses a
  CSV written for another database, asks before changing anything (`--yes` without a terminal), and writes
  `logs/sequences-undo-*.csv`.
- **Oracle still in use:** `apply` reads Oracle again at the end. Sequences that moved meanwhile are listed and the
  exit code is 1; run `apply` again once writes have stopped.
- **Check.** Every column fed by a sequence (identity, OWNED BY, column default, plus `sequence_links.txt` for
  links PostgreSQL cannot see: Oracle triggers, packages, the application's ORM) against that sequence's next
  value. `placeholder` means all-nines marker rows above the real rows, the same as on Oracle. `needs a look` means a
  row already holds a value the sequence will hand out. The check changes nothing.
- **Output.** A summary on screen and CSV files in `logs/` (`sequences-plan-*.csv`, `sequences-apply-*.csv`,
  `sequences-check-*.csv`, `sequences-manual-*.csv`, `sequences-undo-*.csv`). No row data is printed, only sequence
  numbers and IDs.
- **Exit code.** 0 clean, 1 something to look at (a failure, Oracle moved, check findings, anything on the
  needs-a-person list, or a sequence undo skipped), 2 did not run. `plan` exits 0 whatever it lists.
  It refuses to run when the confs point somewhere other than `[target] expected`, and `apply` refuses when that
  is not set.

## Chunk files

`dataload.chunks` works out the chunks of every table loaded by a `run_*.sh` and writes them to that script's
chunk file (`chunks.txt`, or whatever `CHUNKS_FILE` names). The old file is kept beside it as
`chunks.txt.bak-<date>-<time>`.

```
python3 -m dataload.chunks check                  # each table, its ID column and index; reads no table data
python3 -m dataload.chunks make                   # asks, then cuts every chunked table into up to 100 chunks
python3 -m dataload.chunks make --chunks 120 --table SAC_DATA --yes
```

- **How it cuts.** Chunks hold equal row counts: each one ends at the highest of the next N IDs, so gaps in the
  IDs do not matter. N is the row count divided by `--chunks`, rounded up, so there are never more chunks than
  asked for. `--chunks` cannot go above 120.
- **Nothing falls through.** Lines are written like the old files, `ID BETWEEN low AND high`. The first
  starts at the table's lowest ID, each next one at the previous end plus one, and the last ends at the
  largest value the column can hold (`9999999999` for a `NUMBER(10)`), so every row is in exactly one chunk.
  This needs whole-number IDs: a column that can hold decimals, and rows that have them, are refused.
- **A DATE column** (a table with no usable numeric ID, such as `R_AUDIT_LOG` on `AUDIT_DATE`) is cut at whole
  days, from one count of rows per day read from the index: a chunk takes days until it holds N rows, so one
  busy day can make a chunk bigger. Lines are half-open, `AUDIT_DATE >= DATE '2024-01-01' AND AUDIT_DATE < DATE
  '2024-03-15'`, because a DATE has a time of day and `BETWEEN` would miss the last day after midnight. The
  first starts at the earliest day, each next one where the previous stops, and the last stops at
  `9999-12-31`. Oracle and PostgreSQL read these lines the same way, which the load needs (it checks, clears
  and counts each chunk on PostgreSQL too).
- **The ID column** is the one the current chunk file uses, else the table's one-column primary key, else
  `--column TABLE=COLUMN`.
- **It refuses** a table with rows that have no ID, IDs with decimals, a column that is neither a whole number
  nor a DATE, a DATE on or after 9999-12-31, an ID column no index starts with (every
  chunk would read the whole table), and anything while a run holds the lock. A table that fails keeps its old file; the
  others are still written.
- `make` reads every ID of each table once, one Oracle session per table, all tables at the same time
  (`--workers 1` for one at a time). Run it when the DBA says.
- After `make`, start the load with `run --new`. `resume` refuses chunk files that changed since its run
  started, because old and new ranges could load the same rows twice.

## When something goes wrong

| What you see | Meaning | What to do |
|---|---|---|
| Email "Step failed" | One table or chunk ended in an error. The run carried on with the next one | Read the error with `status --failed`, fix the cause, `resume` |
| "Stopped part-way" in status | The work was cut off by a stop. Nothing is wrong | `resume` |
| "failed on a transient error, retry 2/3" in the log | A network or connection error. It is tried 3 times with a pause | Nothing |
| "stalled" in the log | No output for 15 minutes and both databases idle. The copy is stopped and retried. With lanes, only once every lane is quiet | Nothing |
| "load NOT started: foreign key(s)" | Foreign keys still point at tables that will be truncated | Drop them, `resume` |
| "another dataload run is already in progress" | A run holds the lock | Use `status`; do not start a second one |
| "load NOT started: session(s) are already copying" | ora2pg processes from an earlier run are still writing | Wait, or end them, then `resume` |
| No email for a long time | The run may be working on a large table, or the process may be gone | `status`; see below |

### Stopping

`Ctrl+C` (or `kill <pid>`) is the safe way. The tool stops ora2pg, records what finished, and sends its
summary. It can take 10 seconds.

`kill -9`, a server reboot or the server running out of memory ends the tool with no chance to do anything. It
sends **no email**. Nothing is lost: finished tables and chunks are already recorded, and `resume` works out
the rest. But nobody is told, so if the emails stop, check:

```
python3 -m dataload status
tmux attach -t dataload
ps -e | grep ora2pg
```

If `status` does not say the run is active and it is not finished, run `resume`.

### Do not

- Run ora2pg or a `run_*.sh` by hand while the tool is running, or for a table the tool will load.
- Delete or edit anything in `state/`. It is the record of what is done.
- Use `run --new` after a partial load without emptying the chunk-loaded tables first.
- Edit a conf's table list in the middle of a run.
- Change a chunk file in the middle of a run (`resume` refuses it).

## Emails

One mail when a run resumes, one for each failed step, one when every load step of a schema has ended (with
its row counts and failures; `schema_finished = false` under `[email]` turns these off), one at the end of each
phase, and one at the finish with `report.html` attached. Row values quoted by database errors are shown as `***` in mails and in the report
(`hide_values`); the full text is in `logs/` and in `status --failed`.

## Files it writes

| Path | Content |
|---|---|
| `state/run.sqlite` | The record of every step, table and chunk |
| `logs/` | One log per step and attempt, with ora2pg's full output |
| `work/` | Temporary conf copies and remaining-chunk files |
| `report.html` | Rows and sizes before and after, per table |
| `logs/sequences-*.csv` | What `dataload.sequences` found and did, one row per sequence or column |
| `logs/chunks.log` | What `dataload.chunks` did, with times |

## Tests

They need a local PostgreSQL on `localhost:54318` with a database `dataload_test`, and use a fake ora2pg.

```
python3 -m unittest discover -s tests
```
