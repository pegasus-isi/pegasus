#!/usr/bin/env bash
set -euo pipefail

BRANCH="${1:-master}"
TYPE="${2:-}"

PIPELINE_VARS=()
if [[ "$TYPE" == "workflow" ]]; then
  # Define pipeline variables here
  PIPELINE_VARS=(
    "CI_PIPELINE_NAME:workflow"
  )
fi

# Build glab variable arguments
VAR_ARGS=()
for var in "${PIPELINE_VARS[@]}"; do
  VAR_ARGS+=(--variables "$var")
done

echo "Triggering pipeline on branch '$BRANCH'..."

PIPELINE_ID=$(glab pipeline run \
  -R pegasus/pegasus \
  --branch "$BRANCH" "${VAR_ARGS[@]}" | sed -E 's/.*id: ([0-9]+).*/\1/')

if [[ $? -ne 0 ]]; then
  echo "Pipeline failed to run"
  exit 1
fi

echo "Pipeline ID: $PIPELINE_ID"

# Wait for completion
while true; do
  STATUS=$(glab api -R pegasus/pegasus projects/:id/pipelines/$PIPELINE_ID | jq -r '.status')

  echo "Pipeline status: $STATUS"

  case "$STATUS" in
    success)
      echo "Pipeline succeeded"
      exit 0
      ;;
    failed)
      echo "Pipeline failed"
      break
      ;;
    canceled|cancelled|skipped)
      echo "Pipeline ended with $STATUS"
      exit 1
      ;;
  esac

  sleep 10
done

# Print logs from failed jobs
unset PAGER
glab api -R pegasus/pegasus --paginate projects/:id/pipelines/$PIPELINE_ID/jobs | jq '.[] | select(.status=="failed") | .id' |
while read -r JOB_ID; do
  echo
  echo "=================================================="
  echo "FAILED JOB $JOB_ID"
  echo "=================================================="

  glab -R pegasus/pegasus api /projects/:id/jobs/$JOB_ID/trace

  echo
done

exit 1
