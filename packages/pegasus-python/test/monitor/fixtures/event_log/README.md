# Schema-v1 golden event stream

`schema-v1-golden.jsonl` is the shared WP7 corpus. It contains a header,
initial checkpoint, DB-confirmed transition, diagnostic and queue enrichment,
an explicit `disk_guard` gap for sequences 5–6, a transition that must be
discarded while recovering, a recovery checkpoint, and a final transition and
checkpoint.

The complete final database snapshot is epoch 4 with workflow
`golden-workflow`, one job (`compute_ID0001`) in `JOB_SUCCESS`, and
`DB_CONFIRMED` provenance. Replay must report the gap-recovery transition as
ignored and finish complete after the recovery checkpoint (and final
checkpoint).
