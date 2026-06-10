# monitord plugin system — payload race condition (review of #2194)

**Status:** fixed
**Scope:** `pegasus-monitord` entry-point plugin system added in commit `4d37e5a45`
("Add entry-point plugin system to pegasus-monitord (#2194)") on branch
`monitord-plugin-system`.
**Fix:** one-line snapshot in `_PluginWorker.submit` + a deterministic regression
test.

---

## TL;DR

The plugin system introduced **one genuine cross-thread data race**: the event
payload dict was handed to an *asynchronous* plugin consumer **by reference**,
while monitord's main thread kept reusing/mutating that same dict. The fix is a
per-worker shallow snapshot (`dict(kw)`) at enqueue time. Everything else about
the plugin design (thread isolation, bounded queue, daemon workers, bounded
shutdown join) was reviewed and found sound.

---

## How events flow

```
monitord main thread (single parse loop)
  Workflow.output_to_db(event, kwargs)          # workflow.py:137
    └─ wf_event_sink.send(event, kwargs)
         └─ MultiplexEventSink.send(event, kw)   # event_output.py:583
              ├─ DBEventSink.send(event, kw)     # SYNCHRONOUS: copies kw -> d, processes, returns
              └─ PluginHostEventSink.send(event, kw)
                   └─ MonitordPluginManager.dispatch(STAMPEDE_NS+event, kw)
                        └─ _PluginWorker.submit(event, kw)   # ENQUEUE onto queue.Queue
                                                              # ... returns immediately ...

per-plugin worker thread (daemon)                # plugin.py:_run
  event, kw = queue.get()                        # dequeued LATER (ms later)
  plugin.handle_event(event, kw)                 # reads kw on a DIFFERENT thread
```

The single `kw` dict produced by the main thread is fanned out to **every** sink
by `MultiplexEventSink.send` (same object reference, no copy). With plugins
enabled, `wf_event_sink` is a `MultiplexEventSink` wrapping the DB sink **and**
the new `PluginHostEventSink`.

## Why this was safe before #2194, and what changed

Before the plugin system, every consumer of `kw` was **synchronous on the main
thread**:

- `DBEventSink.send` (`event_output.py:240-246`) immediately builds a *new* dict
  `d` from `kw` and processes it before returning. It never retains a reference
  to `kw`.
- The file/TCP/AMQP encoders (`json_encode`/`bson_encode`) receive `**kw`, which
  materializes a fresh kwargs dict inside the encoder — they never mutate the
  caller's dict.

So by the time `output_to_db()` returned, the payload had already been fully
consumed. That made it **safe** for monitord's producers to *reuse and mutate*
the same `kwargs` dict afterward — and they do, in at least two places (below).

`#2194` added the first **asynchronous** consumer. `PluginHostEventSink.send`
only *enqueues* the payload; a separate daemon worker thread reads it
milliseconds later via `plugin.handle_event`. The previously-safe
reuse/mutation in the producers is now a read/write data race across threads on
a shared dict.

## The race — concrete producer mutation sites

### 1. `rc.meta` — one dict reused across metadata pairs

`packages/pegasus-python/src/Pegasus/monitoring/workflow.py:2084-2100`

```python
kwargs = {}                                          # ONE dict per LFN
kwargs["xwf__id"] = my_job._wf_uuid
kwargs["lfn__id"] = lfn
kwargs["ts"] = self._current_timestamp
for key in metadata.get_attribute_keys():
    kwargs["key"]   = key                             # main thread overwrites...
    kwargs["value"] = metadata.get_attribute_value(key)
    self.output_to_db("rc.meta", kwargs)              # ...same dict enqueued each time
```

For an LFN with N metadata attributes, the plugin queue receives N references to
the **same** dict while the main thread rewrites `key`/`value` between sends. A
worker reading queued item *i* can observe the key/value of a later iteration.

### 2. `wf.plan` — key added after dispatch

`packages/pegasus-python/src/Pegasus/monitoring/workflow.py:658, 684-686`

```python
self.output_to_db("wf.plan", kwargs)                  # enqueued to the plugin host
...
if self._database_url is not None:
    kwargs["db_url"] = self._database_url             # mutates the SAME dict after dispatch
self.output_to_dashboard_db("wf.plan", kwargs)
```

The plugin's queued payload acquires a `db_url` key that was never part of the
event as dispatched.

### Severity

- Values monitord produces are immutable scalars (str/int/float/bool/None), and
  CPython's GIL makes individual dict get/set atomic, so this is **content
  tearing** (wrong/extra fields), **not** a crash or a half-built object.
- It violates the documented plugin contract: *"the payload is passed through
  unmodified, exactly as monitord produced it"* (`plugin.py` `handle_event`
  docstring).
- **Sharper edge with ≥2 enabled plugins:** before the fix, every plugin's
  worker shared the *same* dict object. If a plugin mutates `kw` in its
  `handle_event`, two worker threads can mutate one dict with no lock — which
  *can* corrupt a CPython dict. This directly undercut the "untrusted
  third-party code" hardening the `_PluginWorker` docstring claims.

## The fix

`packages/pegasus-python/src/Pegasus/monitoring/plugin.py` — `_PluginWorker.submit`:

```python
# before
self._queue.put_nowait((event, kw))
# after
self._queue.put_nowait((event, dict(kw)))
```

Why this resolves it:

- **Producer-mutation race:** the snapshot is taken *synchronously on the main
  thread* at enqueue time, before `output_to_db()` returns and before any later
  reuse/mutation of the producer's dict. The worker thread only ever sees the
  isolated copy, so subsequent mutations of the original are invisible to it.
- **Cross-plugin race:** the copy is made *per worker* (inside `submit`, which
  runs once per plugin via `dispatch`), so each plugin gets its **own** dict.
  One plugin mutating its payload can no longer affect another plugin or race
  another worker thread on a shared object.
- **Lowest common choke point:** both `MonitordPluginManager.dispatch` and
  `PluginHostEventSink.send` funnel through `submit`, so a single change covers
  every path that can enqueue an event.

## Gotchas / things to keep in mind

- **Shallow copy is sufficient *today*, but only because all payload values are
  immutable scalars.** `dict(kw)` snapshots the dict's key→value bindings, not
  nested objects. If a future producer ever stores a **mutable container**
  (list/dict/object) as a payload *value* and then mutates it in place, the
  shallow copy will **not** protect it — a `copy.deepcopy` would be needed (at
  higher cost). The places to watch first are
  `Workflow.db_send_files_metadata` (`workflow.py`) and
  `Job.create_composite_job_event` (`job.py`), which are the most
  payload-assembly-heavy. Keep payload values scalar.

- **The regression test must pin the worker thread, or it is meaningless.** A
  naive "dispatch then mutate then assert" is flaky: the worker may dequeue the
  event before the test mutates the dict, so the test could pass even with the
  bug present. The test (`test_payload_is_snapshotted_before_async_mutation`)
  uses `GatedRecordingPlugin`, which **blocks inside the first `handle_event`**.
  That guarantees the second event is still sitting un-read in the queue when
  the test mutates its payload — the exact ordering that exposes a missing
  snapshot. Verified: with the fix the test passes; with the fix reverted it
  **deterministically fails**, observing `{'key': 'MUTATED', 'value':
  'MUTATED', 'db_url': 'sqlite:///leaked.db'}`.

- **Don't record the snapshot inside the plugin and call it a test.** The
  existing `RecordingPlugin.handle_event` does `dict(kw)` when recording, but
  that copy happens *after* the worker has already dequeued the (possibly
  mutated) dict — too late to catch the race. The guard has to be the producer
  mutating *before* the worker reads, which is why the gated plugin is needed.

- **`flush()` on the plugin host is intentionally a no-op.** monitord's main
  loop calls `sink.flush()` once per iteration; for plugins, delivery is
  queue-backed and asynchronous, so there is nothing to flush synchronously.
  This means a plugin may observe an event slightly later than the DB sink
  commits it. Not a race — and the plugin contract already warns: *"do not
  assume an event has been committed to the stampede database by the time you
  observe it."*

- **Daemon workers ⇒ at-most-once delivery.** Worker threads are
  `daemon=True`, and the queue is bounded with drop-on-overflow. On a hard kill
  (SIGKILL / `os._exit`), queued events are lost silently; on queue overflow a
  slow plugin drops events (counted/logged). This is by design — a wedged
  plugin must never block or grow monitord. Plugin authors should treat
  delivery as best-effort.

## Reviewed and found sound (no race)

- **Single driver thread.** Every `send`/`flush`/`close` on the workflow sink
  originates on monitord's main parse loop (`pegasus-monitord.py:137` send,
  `:1714-1716` flush, `:215` atexit close). The only other threads in the
  process are the AMQP publisher (`event_output.py:408`) and the per-plugin
  workers (`plugin.py:135`) — both are *pure consumers* of their own private
  queues and never re-enter the sink fan-out. So `MultiplexEventSink._endpoints`
  and `MonitordPluginManager._workers` are only ever touched from one thread.
- **Signal handlers** run on the main thread (CPython); the SIGINT path reaches
  `close()` only via `atexit` *after* the stack unwinds, so there is no
  re-entrant `close()`-during-`send()` and no dict-mutated-during-iteration on
  the endpoints/workers collections.
- **`submit` TOCTOU** (`is_alive()` then `put_nowait`), the **`_dropped`
  non-atomic `+=`**, the **sentinel `task_done()`** with no `queue.join()`, and
  **`stop_all()` idempotency** are all non-issues precisely *because*
  submit/dispatch/close are serialized on the single main thread.

## Secondary finding (not a race) — unbounded `plugin.start()` at init

`MonitordPluginManager.discover_and_start()` runs synchronously inside
`PluginHostEventSink.__init__` (`event_output.py`) and calls each
`plugin.start(props)` with **no timeout** (`plugin.py:240`). Shutdown is
hardened with `join_timeout`, but startup is not — a plugin whose `start()`
blocks will hang monitord startup. Low severity (plugins are opt-in and
operator-controlled, not arbitrary runtime input). Worth a doc note that
`start()` must not block, or a startup watchdog. Left as-is for now.

## Verification

```
cd packages/pegasus-python
# (dev env: PYTHONPATH over the four packages/pegasus-*/src dirs)
pytest test/test_monitord_plugin.py -q
# 13 passed   (12 original + test_payload_is_snapshotted_before_async_mutation)
```

Proof the test guards the fix: reverting `dict(kw)` → `kw` makes
`test_payload_is_snapshotted_before_async_mutation` fail deterministically.

## Files changed

- `packages/pegasus-python/src/Pegasus/monitoring/plugin.py`
  — `_PluginWorker.submit` snapshots the payload with `dict(kw)`; docstring
  expanded to explain why.
- `packages/pegasus-python/test/test_monitord_plugin.py`
  — added `GatedRecordingPlugin` and
  `test_payload_is_snapshotted_before_async_mutation`.
