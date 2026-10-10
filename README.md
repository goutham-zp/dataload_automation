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

## When something goes wrong

| What you see | Meaning | What to do |
|---|---|---|
| Email "Step failed" | One table or chunk ended in an error. The run carried on with the next one | Read the error with `status --failed`, fix the cause, `resume` |
| "Stopped part-way" in status | The work was cut off by a stop. Nothing is wrong | `resume` |
| "failed on a transient error, retry 2/3" in the log | A network or connection error. It is tried 3 times with a pause | Nothing |
| "stalled" in the log | No output for 15 minutes and both databases idle. The copy is stopped and retried | Nothing |
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

## Emails

One mail when a run resumes, one for each failed step, one at the end of each phase, and one at the finish with
`report.html` attached. Row values quoted by database errors are shown as `***` in mails and in the report
(`hide_values`); the full text is in `logs/` and in `status --failed`.

## Files it writes

| Path | Content |
|---|---|
| `state/run.sqlite` | The record of every step, table and chunk |
| `logs/` | One log per step and attempt, with ora2pg's full output |
| `work/` | Temporary conf copies and remaining-chunk files |
| `report.html` | Rows and sizes before and after, per table |

## Tests

They need a local PostgreSQL on `localhost:54318` with a database `dataload_test`, and use a fake ora2pg.

```
python3 -m unittest discover -s tests
```
