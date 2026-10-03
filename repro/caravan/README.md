# Application-level reproduction (caravan)

How the stale select wakeup was found: an HTTP/1.1 server (caravan) whose
connections sometimes never answered on 4 domains.

- `old_engine.ml`: caravan's connection engine before it worked around the
  bug (caravan commit e330f1a), with stage markers. While the handler runs
  it waits on `choose [get' handler; readable socket]`, and on `Readable`
  it reads to tell a disconnect from a pipelined request. A spurious
  `Readable` makes that read wait for data that never comes.
- `stress.ml`: 20 concurrent clients, one request each. On a hang it prints
  the stages and, if `DBG_OUT` is set, dumps affect's event log there
  (always with `DBG_ALWAYS=1`).
- `rate.sh N`: hangs over N fresh processes.
- `analyze.py LOG`: events of the call stuck in a hung run's log.
- `stale.py DBG...`: wakeups by a select whose fd sets predate the wait.

Build: a dune workspace holding this affect checkout as `affect/` and
caravan e330f1a as `caravan/` (`git archive e330f1a dune-project
caravan.opam src`). Then, from the workspace root:

    dune build --root . affect/repro/caravan/stress.exe
    affect/repro/caravan/rate.sh 1000                    # fixed select
    affect/repro/caravan/rate.sh 1000 AFFECT_SNAPSHOT=0  # old select

Results (Linux, OCaml 5.5.0): 33/1000 hangs with the old select, 0/1000
with the fix. Over 50 traced runs the old select made 102 stale wakeups,
the fix none.
