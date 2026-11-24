## Pegasus 6.0.x Series

### Pegasus 6.0.0

#### New Features and Improvements

- Added `pegasus-monitor`, a native, read-only live and one-shot monitor for a
  single Pegasus workflow. It combines authoritative Stampede SQLite snapshots
  with an immediate `jobstate.log` overlay and optional UUID-scoped, bounded
  HTCondor observations. It also provides `--diagnose` and `--why-idle` modes
  and degrades explicitly when optional sources are unavailable.
- Added canonical, DB-confirmed JSONL recording to `pegasus-monitor`, including
  secure-file handling, free-space and size guards, periodic and recovery
  checkpoints, and redacted diagnostic evidence. Saved streams support
  source-free replay, explicit singleton headless servers, and bounded
  incremental SSH remote viewing with stream-replacement and gap recovery.
- `pegasus-status --watch` remains available, unchanged, and not deprecated.
  `pegasus-monitor` provides the richer live view while the existing status
  command continues to serve its established queue and DAG progress use case.
