# Pegasus Healer

An agentic auto-recovery system for Pegasus WMS that diagnoses and fixes failed
workflow jobs automatically using LLM reasoning and a configurable policy engine.

Pegasus Healer runs as a DAGMan post-script (`dagman.post`). When a job fails,
the healer inspects the failure, classifies it, and — if policy allows —
patches the submit file and exits with code 1 so DAGMan retries immediately.
No human intervention required for recoverable failures.

## How It Works

```
Job fails
   │
   └─► pegasus-healer $RETURN $JOB $RETRY $MAX_RETRIES
            │
            ├─ ObservatorAgent   — collects facts (exit code, stderr, .sub, logs)
            ├─ ClassifierAgent   — classifies failure type (OOM, MISSING_FILE, ...)
            ├─ DiagnosisAgent    — LLM ReAct reasoning over evidence
            ├─ FixPlanningAgent  — generates a concrete fix (e.g. patch request_memory)
            └─ PolicyEngine      — AUTO / ASK / STOP / ESCALATE decision
                     │
                     ├─ AUTO   → applies fix, exits 1 (DAGMan retries)
                     ├─ ASK    → writes proposal file, exits 0 (DAGMan stops)
                     └─ STOP   → escalates, exits 0 (DAGMan stops)
```

Supported failure types:

| Failure Type | Decision | Action |
|---|---|---|
| `OUT_OF_MEMORY` | AUTO | Scale `request_memory` by 1.5× in the `.sub` file |
| `DISK_EXCEEDED` | AUTO | Scale `request_disk` by 2× in the `.sub` file |
| `PREEMPTED` | AUTO | Retry immediately (no config change) |
| `NODE_FAILURE` | AUTO | Retry immediately (no config change) |
| `STAGING_FAILURE` | AUTO | Retry transfer job (transient I/O error) |
| `MPI_FAILURE` | AUTO | Retry (transient fabric issue) |
| `TRANSIENT_INFRASTRUCTURE` | AUTO | Retry (generic transient failure) |
| `WALLTIME_EXCEEDED` | ASK | Propose runtime increase (cost implications) |
| `PARTITION_MIGRATION` | ASK | Propose moving to a larger HPC partition |
| `LICENSE_UNAVAILABLE` | ASK | Propose retry on a site with available license |
| `GPU_UNAVAILABLE` | ASK | Propose retry on a GPU-capable site |
| `DATA_CORRUPTION` | ASK | Propose re-staging input from replica catalog |
| `MISSING_INPUT` | ASK | Propose replica catalog correction |
| `DATA_MISMATCH` | ASK | Propose catalog data binding fix |
| `SCRIPT_ERROR` | ASK | Propose transformation script patch |
| `APPLICATION_ERROR` | ASK | Diagnosis + proposed fix for human review |
| `CONTAINER_PULL_FAILED` | ASK | Propose image/registry fix |
| `MODULE_NOT_FOUND` | ASK | Propose site change or module install |
| `SCRATCH_FULL` | ASK | Propose TMPDIR redirect or scratch cleanup |
| `SCHEDULER_ADMISSION` | ASK | Propose scheduler config correction |
| `ACCOUNT_EXPIRED` | ESCALATE | HPC allocation exhausted — human must act |
| `SSH_AUTH_FAILURE` | ESCALATE | Credential fix required — human must act |
| `LOGIC_VALIDATION` | STOP | Scientific logic error — never auto-fix |
| `UNKNOWN` | ASK | Best-effort diagnosis surfaced for human review |

## Installation

Pegasus Healer is included in this source tree under `packages/pegasus-healer/`.
Install it alongside the main Pegasus package:

```bash
# From the repo root — installs everything including the healer
pip install -e .

# Or install the healer package alone (for development)
pip install -e packages/pegasus-healer/
```

After installation, `pegasus-healer` is available on your PATH.

## Quick Start

### 1. Configure LLM credentials

Copy the example env file and fill in your values:

```bash
cp healer.env.example ~/.pegasus/healer.env
chmod 600 ~/.pegasus/healer.env
```

Edit `~/.pegasus/healer.env`:

```bash
# Cloud (OpenAI)
LLM_MODEL=gpt-4o
LLM_API_KEY=sk-...

# Cloud (Anthropic)
LLM_MODEL=anthropic/claude-sonnet-4-6
LLM_API_KEY=sk-ant-...

# Local (Ollama — no API key needed)
LLM_MODEL=ollama/llama3.3
LLM_BASE_URL=http://localhost:11434
```

See [healer.env.example](../../healer.env.example) at the repo root for all options.

### 2. Register the healer as a DAGMan post-script

Add these properties to your `pegasus.properties` (or `.pegasusrc`):

```properties
dagman.post                      = pegasus-healer
dagman.post.path.pegasus-healer  = /path/to/bin/pegasus-healer
dagman.post.arguments            = $RETURN $JOB $RETRY $MAX_RETRIES
dagman.maxretries                = 3
```

Or set them programmatically using the Python API:

```python
from Pegasus.api import Properties

props = Properties()
props["dagman.post"]                     = "pegasus-healer"
props["dagman.post.path.pegasus-healer"] = "/path/to/bin/pegasus-healer"
props["dagman.post.arguments"]           = "$RETURN $JOB $RETRY $MAX_RETRIES"
props["dagman.maxretries"]               = "3"
props.write("pegasus.properties")
```

### 3. Plan and submit your workflow normally

```bash
pegasus-plan --conf pegasus.properties ...
```

The healer runs automatically whenever a job fails.

## Configuration Reference

### Environment Variables

All variables can be set in `~/.pegasus/healer.env`, a `.env` file in the
submit directory, or as shell environment variables (shell takes precedence).

#### LLM

| Variable | Default | Description |
|---|---|---|
| `LLM_MODEL` | `gpt-4o` | Model in LiteLLM format (e.g. `ollama/llama3.3`, `anthropic/claude-sonnet-4-6`) |
| `LLM_API_KEY` | — | API key for the provider |
| `LLM_BASE_URL` | — | Custom endpoint (local models, Azure, vLLM) |
| `DIAGNOSIS_LLM_MODEL` | `LLM_MODEL` | Override model for DiagnosisAgent |
| `FIX_PLANNING_LLM_MODEL` | `LLM_MODEL` | Override model for FixPlanningAgent |

#### Policy

| Variable | Default | Description |
|---|---|---|
| `POLICY_FILE` | auto-detected | Path to `remediation.yaml` policy config |
| `HEALER_POLICY_MODE` | `production` | `production` (conservative) or `debug` (aggressive) |
| `CONFIDENCE_THRESHOLD` | `0.80` | Minimum confidence for AUTO decisions |

#### Storage

| Variable | Default | Description |
|---|---|---|
| `HEALER_CHECKPOINT` | `~/.pegasus/healer_checkpoint.db` | LangGraph checkpoint DB (resumes graph state between retries) |
| `HEALER_MEMORY_DB` | `~/.pegasus/healer_memory.db` | Episodic memory DB (successful fix history) |

#### Observability

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`) |
| `OTLP_ENDPOINT` | — | OpenTelemetry collector endpoint |

### Policy File

The policy file (`policies/remediation.yaml`) controls which failure types are
handled automatically and what limits apply.

```yaml
failure_policies:
  OUT_OF_MEMORY:
    decision: AUTO          # AUTO | ASK | STOP | ESCALATE
    max_memory_mb: 32768    # hard ceiling for memory scaling
    memory_multiplier: 1.5  # scale factor per retry

  MISSING_INPUT_FILE:
    decision: AUTO

  STAGING_ERROR:
    decision: ASK           # always ask a human before patching transfer jobs

  RUNTIME_ERROR:
    decision: STOP
```

**Policy modes** (`HEALER_POLICY_MODE`):

- `production` (default) — conservative thresholds, higher confidence required,
  costly fixes (large memory bumps) routed to ASK.
- `debug` — lower thresholds, more AUTO decisions, larger multipliers. Useful
  during development and testing.

## OOM Auto-Fix Example

A job that fails with exit code 137 (SIGKILL / OOM) and `request_memory=256`:

```
Attempt 1  request_memory=256 MB  →  OOM (exit 137)  →  healer patches .sub → 384 MB
Attempt 2  request_memory=384 MB  →  OOM (exit 137)  →  healer patches .sub → 576 MB
Attempt 3  request_memory=576 MB  →  success
```

The healer patches both `request_memory` and `pegasus_memory_mb` in the `.sub`
file. HTCondor expands `$(request_memory)` before the job starts, so the next
retry automatically sees the updated value — no script changes needed.

To pass the current memory budget into the job itself, add this profile:

```python
job.add_profiles(Namespace.ENV, key="JOB_MEMORY_MB", value="$(request_memory)")
```

## Output Files

After each healer invocation the following files are written to the submit directory:

| File | Contents |
|---|---|
| `healer-proposal-<job>.json` | Full fix proposal with evidence, confidence, and policy decision |
| `healer-reasoning-<job>.md` | Step-by-step LLM reasoning trace |
| `healer-escalation-<job>.txt` | Human-readable escalation report (ASK/STOP decisions) |

## Supported LLM Providers

Pegasus Healer uses [LiteLLM](https://docs.litellm.ai/) — any provider it
supports works out of the box:

| Provider | Example model | Notes |
|---|---|---|
| OpenAI | `gpt-4o` | Best accuracy |
| Anthropic | `anthropic/claude-sonnet-4-6` | Strong reasoning |
| Ollama (local) | `ollama/llama3.3` | No API key, free |
| Azure OpenAI | `azure/gpt-4o` | Set `LLM_BASE_URL` |
| AWS Bedrock | `bedrock/anthropic.claude-3-opus` | Uses AWS env vars |
| vLLM | `openai/my-model` | Set `LLM_BASE_URL` |

## Development

```bash
# Install in editable mode with dev dependencies
pip install -e "packages/pegasus-healer/[dev]"

# Run tests
tox -m healer

# Lint
tox -e lint
```

## Architecture

```
packages/pegasus-healer/
├── src/Pegasus/healer/
│   ├── cli/healer.py        — entry point, argument parsing, DAGMan exit code
│   ├── graph/               — LangGraph state machine (nodes, state, graph)
│   ├── agents/              — DiagnosisAgent, FixPlanningAgent, tools
│   ├── collectors/          — evidence collectors (submit dir, files, logs)
│   ├── fixes/               — fix appliers (overlay, catalog)
│   ├── policies/            — PolicyEngine, loader, remediation.yaml
│   ├── llm/                 — LiteLLM provider abstraction
│   └── memory/              — episodic memory (successful fix history)
├── policies/remediation.yaml
└── pyproject.toml
```
