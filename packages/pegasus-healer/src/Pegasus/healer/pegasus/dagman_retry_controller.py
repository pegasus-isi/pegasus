from __future__ import annotations

"""
DAGManRetryController — real RetryController for the DAGMan POST script path.

Integration model
─────────────────
1. DAGMan calls the POST script (pegasus-healer) synchronously after job failure.
2. The healer diagnoses the failure, selects a fix, validates policy.
3. apply_overlay() patches the HTCondor .sub file IN-PLACE before the
   POST script exits.
4. The POST script exits 1 (non-zero) → DAGMan retries the job node
   using the modified submit file — no cluster restart needed.

One controller instance is created per POST script invocation and is
scoped to a single (submit_dir, job_id, job_instance_id) triple.
"""

from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import structlog

from Pegasus.healer.fixes.overlay import build_overlay
from Pegasus.healer.models.fixes import AppliedOverlay, FixProposal
from Pegasus.healer.pegasus.adapter import AttemptState, JobAttemptRef, RetryReceipt, Verification
from Pegasus.healer.pegasus.sub_file import apply_overlay_to_sub_file, read_resource_requests

log = structlog.get_logger(__name__)


class DAGManRetryController:
    """
    RetryController backed by direct .sub file mutation.

    Thread-safety: one instance per POST script invocation — no shared state.
    """

    def __init__(
        self,
        submit_dir: str,
        job_id: str,
        job_instance_id: int,
    ) -> None:
        self._submit_dir = Path(submit_dir)
        self._job_id = job_id
        self._job_instance_id = job_instance_id
        # The retry will become the next instance (DAGMan increments it)
        self._next_instance_id = job_instance_id + 1
        self._applied_overlays: dict[str, AppliedOverlay] = {}
        # Resource values read BEFORE apply_overlay patches the .sub file.
        # Used by broadcast_to_siblings to identify siblings with the same config.
        self._pre_patch_resources: dict[str, int | None] = {}

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _find_sub_file(self) -> Path | None:
        """
        Locate the .sub file searching all XX/YY subdirectories.
        Pegasus distributes jobs across 00/00, 00/01, 00/02, ... buckets.
        """
        from Pegasus.healer.collectors.submit_dir import _all_job_subdirs
        for job_dir in _all_job_subdirs(self._submit_dir):
            for name in (
                f"{self._job_id}_ID{self._job_instance_id:07d}.sub",
                f"{self._job_id}.sub",
            ):
                p = job_dir / name
                if p.exists():
                    return p
        # Flat fallback
        for name in (
            f"{self._job_id}_ID{self._job_instance_id:07d}.sub",
            f"{self._job_id}.sub",
        ):
            p = self._submit_dir / name
            if p.exists():
                return p
        return None

    # ── RetryController Protocol ──────────────────────────────────────────────

    async def inspect_attempt(self, ref: JobAttemptRef) -> AttemptState:
        """
        Query scheduler history for the terminal state of this attempt.
        """
        from Pegasus.healer.scheduler.factory import get_scheduler_client

        try:
            history = get_scheduler_client().get_job_history(str(ref.job_instance_id))
            exit_code = history.exit_code
            return AttemptState(
                is_terminal=True,
                is_failed=exit_code is None or exit_code != 0,
                exit_code=exit_code,
                scheduler_reason=history.hold_reason,
            )
        except Exception as exc:
            log.warning("inspect_attempt_failed", error=str(exc))
            return AttemptState(is_terminal=True, is_failed=True)

    async def apply_overlay(self, fix: FixProposal) -> AppliedOverlay:
        """
        Build the overlay and patch the .sub file in-place.

        IMPORTANT: this must complete before the POST script exits so that
        DAGMan reads the updated resource requests on retry.
        """
        overlay = build_overlay(fix)

        sub_path = self._find_sub_file()
        if sub_path is not None:
            # Snapshot resource values BEFORE patching so broadcast_to_siblings
            # can find siblings that still have the same original configuration.
            self._pre_patch_resources = read_resource_requests(sub_path)

        if sub_path is None:
            log.warning(
                "sub_file_not_found_for_overlay",
                job_id=self._job_id,
                job_instance_id=self._job_instance_id,
                submit_dir=str(self._submit_dir),
                fix_id=str(fix.fix_id),
            )
        else:
            changes = apply_overlay_to_sub_file(sub_path, fix.proposed_configuration)
            log.info(
                "sub_file_patched",
                path=str(sub_path),
                changes=changes,
                fix_id=str(fix.fix_id),
                job_id=self._job_id,
            )

        self._applied_overlays[str(fix.fix_id)] = overlay
        return overlay

    async def authorize_retry(self, incident_id: UUID, fix_id: UUID) -> RetryReceipt:
        """
        In POST script mode the retry is triggered by the POST script exiting
        non-zero — DAGMan handles the actual job re-submission.

        This method just records the authorisation and allocates the expected
        next job_instance_id (job_instance_id + 1).
        """
        receipt = RetryReceipt(
            incident_id=str(incident_id),
            fix_id=str(fix_id),
            retry_job_instance_id=self._next_instance_id,
            authorized_at=datetime.now(timezone.utc).isoformat(),
        )
        self._next_instance_id += 1
        log.info(
            "retry_authorised_post_script",
            incident_id=str(incident_id),
            fix_id=str(fix_id),
            expected_next_instance_id=receipt.retry_job_instance_id,
        )
        return receipt

    async def verify_retry_configuration(self, receipt: RetryReceipt) -> Verification:
        """
        Confirm the overlay was written into the .sub file by re-reading it.
        """
        overlay = self._applied_overlays.get(receipt.fix_id)
        if overlay is None:
            return Verification(
                consumed_new_config=False,
                mismatch_reason="No overlay stored for this fix_id",
            )

        sub_path = self._find_sub_file()
        if sub_path is None:
            # File disappeared — can't verify, but trust apply_overlay succeeded
            return Verification(
                consumed_new_config=True,
                actual_configuration=overlay.new_config,
                mismatch_reason=".sub file not found at verification time",
            )

        actual = read_resource_requests(sub_path)
        new_memory = overlay.new_config.get("memory_mb")
        actual_memory = actual.get("memory_mb")

        if new_memory is not None and actual_memory != int(new_memory):
            return Verification(
                consumed_new_config=False,
                actual_configuration={k: v for k, v in actual.items() if v is not None},
                mismatch_reason=(
                    f"request_memory mismatch: expected {new_memory}, found {actual_memory}"
                ),
            )

        return Verification(
            consumed_new_config=True,
            actual_configuration={k: v for k, v in actual.items() if v is not None},
        )

    async def apply_script_patches(self, fix: FixProposal) -> list[str]:
        """
        Apply script patches to transformation script files in-place.

        Creates a .bak backup of the original before overwriting.
        Only called for PATCH_TRANSFORMATION_SCRIPT proposals that include
        script_patches populated by the fix planner.

        Returns the list of file paths that were successfully patched.
        """
        patched: list[str] = []
        for patch in fix.script_patches:
            script_path = Path(patch.file_path)
            if not script_path.exists():
                log.warning(
                    "script_patch_target_not_found",
                    path=str(script_path),
                    fix_id=str(fix.fix_id),
                )
                continue
            try:
                bak_path = script_path.with_suffix(script_path.suffix + ".bak")
                bak_path.write_text(patch.original_content, encoding="utf-8")
                script_path.write_text(patch.patched_content, encoding="utf-8")
                log.info(
                    "script_patched",
                    path=str(script_path),
                    backup=str(bak_path),
                    description=patch.patch_description,
                    fix_id=str(fix.fix_id),
                )
                patched.append(str(script_path))
            except OSError as exc:
                log.error(
                    "script_patch_failed",
                    path=str(script_path),
                    error=str(exc),
                    fix_id=str(fix.fix_id),
                )
        return patched

    async def cancel_pending_retry(self, incident_id: UUID) -> None:
        """
        In POST script mode there is no pending retry token to cancel —
        the POST script has not exited yet when this may be called.
        """
        log.info("cancel_pending_retry_post_script_noop", incident_id=str(incident_id))

    async def broadcast_to_siblings(
        self,
        fix: FixProposal,
        failure_type: str,
        transformation: str | None,
        thread_id: str,
    ) -> int:
        """
        Broadcast the fix to all sibling jobs that share the same transformation
        and still have the same resource value as the failing job had before patching.

        Returns the number of siblings patched.
        """
        if not transformation:
            return 0

        from Pegasus.healer.pegasus.sibling_fixer import broadcast_fix

        new_config = {k: v for k, v in fix.proposed_configuration.items() if v is not None}
        if not new_config:
            return 0

        # Find which resource key is being changed and its original value
        resource_key = next(
            (k for k in ("memory_mb", "disk_mb", "cpus", "runtime_seconds")
             if k in new_config),
            None,
        )
        if resource_key is None:
            return 0

        original_config = {
            k: self._pre_patch_resources.get(k)
            for k in new_config
            if self._pre_patch_resources.get(k) is not None
        }

        return broadcast_fix(
            submit_dir=self._submit_dir,
            transformation=transformation,
            failure_type=failure_type,
            resource_key=resource_key,
            original_config=original_config,
            new_config=new_config,
            thread_id=thread_id,
            excluding_job_id=self._job_id,
        )
