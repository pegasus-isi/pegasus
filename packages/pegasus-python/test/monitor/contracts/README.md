# Pegasus monitor JSONL v1 contract

This directory records the WP1 serialization boundary for the later WP7
writer/replay/remote implementation. The executable source of truth is the
frozen record models and explicit `to_json_dict()` codecs in
`Pegasus.monitor.models`; `jsonl-v1-records.json` documents their stable shape.

The contract is `frozen`. Executable WP4 reconciliation established that these
three redundant cursor details are deferred beyond schema version 1:

- `checkpoint.reconciliation_cursor`;
- `checkpoint.per_instance_suffix_window`;
- `gap.resume_checkpoint_sequence`.

They are intentionally absent rather than emitted with duplicate meanings.
All canonical transition records are DB-confirmed and carry exact Stampede row
identity. Checkpoints wrap a `DatabaseSnapshot`, including job and workflow
watermarks, and contain no provisional tail overlays or scheduler enrichment.
A gap invalidates incremental reconstruction until the next DB-confirmed
checkpoint. Diagnostic records explicitly do not change replayed state.
