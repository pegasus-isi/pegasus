# Pegasus monitor contracts

## JSONL v1 event log

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

## JSON output v1

`json-output-v1.json` describes the single document that
`pegasus-monitor --json` writes for the one-shot modes (`--once`, `--diagnose`,
`--why-idle`). It is a separate contract from the JSONL log: it carries
`format` and `format_version`, never the log's `schema_version`, and it
includes scheduler evidence, diagnostic suggestions, the why-idle analysis and
a one-shot stall assessment, none of which the log records.

The executable source of truth is `Pegasus.monitor.json_output`, which builds
the document from the same `to_json_dict()` codecs as the log. Each list under
`sections` is the exact, ordered key set of that object. Additive changes keep
`format_version`; removing or renaming a key, or changing its type or meaning,
increments it.
