"""Compose authoritative Stampede snapshots with provisional live events.

The reconciler owns no clocks, files, database connections, tasks, or timers.
It is a deterministic state machine driven by the coordinator.  Stampede is
always authoritative; ``jobstate.log`` events remain ordered provisional
overlays until an exact/equivalent database transition or an explicitly later
authoritative transition retires them.
"""

##
#  Copyright 2026 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

from __future__ import annotations

from collections import OrderedDict, defaultdict
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import TYPE_CHECKING

from Pegasus.monitor.models import (
    ClockSample,
    DatabaseGeneration,
    DatabaseSnapshot,
    DBJobTransition,
    DBRefreshMode,
    DBRefreshResult,
    DBWorkflowTransition,
    EffectiveEvent,
    EffectiveSnapshot,
    FrozenPayload,
    HealthState,
    JobSemanticKey,
    JobSnapshot,
    JobTransitionWatermark,
    Provenance,
    SchedulerQueryKind,
    SchedulerQueryResult,
    SnapshotEpoch,
    SourceHealth,
    SourceName,
    TailGeneration,
    TailJobEvent,
    TailPollResult,
    TailSourceEvent,
    TailWorkflowEvent,
    WorkflowIdentity,
    WorkflowRestartIdentity,
    WorkflowSnapshot,
    WorkflowTransitionWatermark,
    normalize_workflow_state,
    producer_timestamp_key,
    transition_group_equivalent,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence


class ReconciliationError(RuntimeError):
    """An input violated the selected-workflow or generation boundary."""


@dataclass(frozen=True, slots=True)
class ReconciliationCursor:
    """The bounded cursor passed to a Stampede suffix refresh."""

    job_watermarks: tuple[JobTransitionWatermark, ...]
    provisional_job_keys: tuple[JobSemanticKey, ...]
    workflow_watermark: WorkflowTransitionWatermark | None


_KNOWN_JOB_STATES = frozenset(
    {
        "PRE_SCRIPT_STARTED",
        "PRE_SCRIPT_TERMINATED",
        "PRE_SCRIPT_SUCCESS",
        "PRE_SCRIPT_FAILED",
        "SUBMIT",
        "GRID_SUBMIT",
        "GLOBUS_SUBMIT",
        "SUBMIT_FAILED",
        "GRID_SUBMIT_FAILED",
        "GLOBUS_SUBMIT_FAILED",
        "JOB_RELEASED",
        "EXECUTE",
        "IMAGE_SIZE",
        "REMOTE_ERROR",
        "JOB_HELD",
        "JOB_EVICTED",
        "JOB_TERMINATED",
        "POST_SCRIPT_STARTED",
        "POST_SCRIPT_TERMINATED",
        "POST_SCRIPT_SUCCESS",
        "POST_SCRIPT_FAILED",
        "JOB_ABORTED",
        "JOB_FAILED",
        "JOB_FAILURE",
        "JOB_SUCCESS",
    }
)

_TERMINAL_STATES = frozenset(
    {
        "PRE_SCRIPT_FAILED",
        "POST_SCRIPT_FAILED",
        "SUBMIT_FAILED",
        "GRID_SUBMIT_FAILED",
        "GLOBUS_SUBMIT_FAILED",
        "JOB_ABORTED",
        "JOB_FAILED",
        "JOB_FAILURE",
        "JOB_SUCCESS",
    }
)

# Same-second supersession must be reviewed rather than inferred from a lexical
# or numeric ordering.  Later producer seconds may supersede any known earlier
# state for the same attempt; this table handles the ambiguous same-second case.
_SAME_SECOND_SUPERSESSION = {
    "PRE_SCRIPT_STARTED": _KNOWN_JOB_STATES - {"PRE_SCRIPT_STARTED"},
    "PRE_SCRIPT_TERMINATED": _KNOWN_JOB_STATES
    - {"PRE_SCRIPT_STARTED", "PRE_SCRIPT_TERMINATED"},
    "PRE_SCRIPT_SUCCESS": _KNOWN_JOB_STATES
    - {"PRE_SCRIPT_STARTED", "PRE_SCRIPT_TERMINATED", "PRE_SCRIPT_SUCCESS"},
    "SUBMIT": frozenset(
        {
            "GRID_SUBMIT",
            "GLOBUS_SUBMIT",
            "EXECUTE",
            "JOB_HELD",
            "JOB_EVICTED",
            "JOB_TERMINATED",
            "POST_SCRIPT_STARTED",
            "POST_SCRIPT_TERMINATED",
            "POST_SCRIPT_SUCCESS",
        }
    )
    | _TERMINAL_STATES,
    "GRID_SUBMIT": frozenset(
        {
            "EXECUTE",
            "JOB_HELD",
            "JOB_EVICTED",
            "JOB_TERMINATED",
            "POST_SCRIPT_STARTED",
            "POST_SCRIPT_TERMINATED",
            "POST_SCRIPT_SUCCESS",
        }
    )
    | _TERMINAL_STATES,
    "GLOBUS_SUBMIT": frozenset(
        {
            "EXECUTE",
            "JOB_HELD",
            "JOB_EVICTED",
            "JOB_TERMINATED",
            "POST_SCRIPT_STARTED",
            "POST_SCRIPT_TERMINATED",
            "POST_SCRIPT_SUCCESS",
        }
    )
    | _TERMINAL_STATES,
    "JOB_RELEASED": frozenset(
        {
            "EXECUTE",
            "JOB_HELD",
            "JOB_EVICTED",
            "JOB_TERMINATED",
            "POST_SCRIPT_STARTED",
            "POST_SCRIPT_TERMINATED",
            "POST_SCRIPT_SUCCESS",
        }
    )
    | _TERMINAL_STATES,
    "EXECUTE": frozenset(
        {
            "JOB_HELD",
            "JOB_EVICTED",
            "JOB_TERMINATED",
            "POST_SCRIPT_STARTED",
            "POST_SCRIPT_TERMINATED",
            "POST_SCRIPT_SUCCESS",
        }
    )
    | _TERMINAL_STATES,
    "JOB_HELD": frozenset(
        {
            "JOB_RELEASED",
            "EXECUTE",
            "JOB_EVICTED",
            "JOB_TERMINATED",
            "POST_SCRIPT_STARTED",
            "POST_SCRIPT_TERMINATED",
            "POST_SCRIPT_SUCCESS",
        }
    )
    | _TERMINAL_STATES,
    "JOB_EVICTED": frozenset(
        {
            "JOB_RELEASED",
            "EXECUTE",
            "JOB_HELD",
            "JOB_TERMINATED",
            "POST_SCRIPT_STARTED",
            "POST_SCRIPT_TERMINATED",
            "POST_SCRIPT_SUCCESS",
        }
    )
    | _TERMINAL_STATES,
    "JOB_TERMINATED": frozenset(
        {"POST_SCRIPT_STARTED", "POST_SCRIPT_TERMINATED", "POST_SCRIPT_SUCCESS"}
    )
    | _TERMINAL_STATES,
    "POST_SCRIPT_STARTED": frozenset({"POST_SCRIPT_TERMINATED", "POST_SCRIPT_SUCCESS"})
    | _TERMINAL_STATES,
    "POST_SCRIPT_TERMINATED": frozenset({"POST_SCRIPT_SUCCESS"}) | _TERMINAL_STATES,
}


def _event_offset(event: TailJobEvent | TailWorkflowEvent) -> int:
    return event.identity.start_offset


def _job_attempt_key(event: TailJobEvent) -> tuple[str, int]:
    return event.exec_job_id, event.job_submit_seq


def _db_attempt_key(transition: DBJobTransition) -> tuple[str, int]:
    return transition.exec_job_id, transition.job_submit_seq


def _producer_second(value: Decimal) -> Decimal:
    return Decimal(producer_timestamp_key(value))


def _same_generation(
    events: Sequence[TailJobEvent | TailWorkflowEvent],
) -> TailGeneration | None:
    generations = {event.identity.source_generation for event in events}
    if len(generations) > 1:
        raise ReconciliationError("pending overlays span multiple tail generations")
    return next(iter(generations), None)


class Reconciler:
    """Deterministically merge one workflow's DB base and tail overlay."""

    def __init__(
        self,
        workflow: WorkflowIdentity,
        *,
        max_pending_events: int = 4096,
        max_pending_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        if max_pending_events <= 0 or max_pending_bytes <= 0:
            raise ValueError("pending overlay bounds must be positive")
        self.workflow = workflow
        self.max_pending_events = max_pending_events
        self.max_pending_bytes = max_pending_bytes
        self._database: DatabaseSnapshot | None = None
        self._database_jobs_by_exec_id: dict[str, JobSnapshot] = {}
        self._pending_jobs: list[TailJobEvent] = []
        self._pending_workflow: list[TailWorkflowEvent] = []
        self._quarantined_jobs: list[TailJobEvent] = []
        self._quarantined_workflow: list[TailWorkflowEvent] = []
        self._tail_generation: TailGeneration | None = None
        self._job_cursors: dict[tuple[str, int], JobTransitionWatermark] = {}
        self._uncursored_attempts: set[tuple[str, int]] = set()
        self._workflow_cursor: WorkflowTransitionWatermark | None = None
        self._health: dict[SourceName, SourceHealth] = {}
        self._scheduler: dict[SchedulerQueryKind, SchedulerQueryResult] = {}
        self._scheduler_by_id: dict[
            SchedulerQueryKind, dict[str, tuple[FrozenPayload, ...]]
        ] = {}
        self._scheduler_by_name: dict[
            SchedulerQueryKind, dict[str, tuple[FrozenPayload, ...]]
        ] = {}
        self._scheduler_jobs_revision = 0
        self._effective_jobs_database: DatabaseSnapshot | None = None
        self._effective_jobs_pending: tuple[object, ...] = ()
        self._effective_jobs_scheduler_revision = -1
        self._effective_jobs_cache: tuple[JobSnapshot, ...] | None = None
        self._effective_base_database: DatabaseSnapshot | None = None
        self._effective_base_scheduler_revision = -1
        self._effective_base_jobs: tuple[JobSnapshot, ...] = ()
        self._force_full_refresh = False
        self._database_rebootstrap = False
        self._publication_frozen = False
        self._tail_resync = False
        self._tail_gap_active = False
        self._tail_reattaching_active = False
        self._tail_rearm_required = False
        self._workflow_identity_conflict = False
        self._frozen: EffectiveSnapshot | None = None
        self._event_order = 0
        self._event_orders: OrderedDict[tuple[object, ...], int] = OrderedDict()
        self._max_event_order_keys = 16384

    @property
    def database(self) -> DatabaseSnapshot | None:
        return self._database

    @property
    def database_generation(self) -> DatabaseGeneration | None:
        return self._database.generation if self._database is not None else None

    @property
    def tail_generation(self) -> TailGeneration | None:
        return self._tail_generation

    @property
    def pending_count(self) -> int:
        return len(self._pending_jobs) + len(self._pending_workflow)

    @property
    def buffered_count(self) -> int:
        return (
            self.pending_count
            + len(self._quarantined_jobs)
            + len(self._quarantined_workflow)
        )

    @property
    def semantic_progress(self) -> int:
        """Session-monotonic event order; refresh-only polls leave it unchanged."""

        return self._event_order

    def unconfirmed_tail_events(self) -> tuple[EffectiveEvent, ...]:
        """Return a bounded pre-DB preview without constructing workflow state.

        A continuity boundary quarantines or drops the overlay, so no preview is
        exposed until an authoritative database base has repaired that boundary.
        The returned events use the same session-monotonic ordering registry as
        an eventual :class:`EffectiveSnapshot`.
        """

        if (
            self._database is not None
            or self._publication_frozen
            or self._tail_gap_active
            or self._tail_reattaching_active
            or self._tail_rearm_required
        ):
            return ()
        pending: tuple[TailJobEvent | TailWorkflowEvent, ...] = tuple(
            sorted((*self._pending_jobs, *self._pending_workflow), key=_event_offset)
        )
        return self._ordered_effective_events(
            (Provenance.TAIL_PENDING, event) for event in pending
        )

    @property
    def source_health(self) -> tuple[SourceHealth, ...]:
        return self._source_health()

    @property
    def needs_full_refresh(self) -> bool:
        return (
            self._database is None
            or self._force_full_refresh
            or self._database_rebootstrap
        )

    @property
    def tail_rearm_required(self) -> bool:
        return self._tail_rearm_required

    def mark_tail_rearmed(self) -> None:
        """A new provider was armed at its current EOF after an overlay gap."""

        self._tail_rearm_required = False
        # The replacement provider owns the next generation identity.  Leaving
        # the old value here would mistake the expected first poll for another
        # unplanned replacement and force a redundant rebootstrap.
        self._tail_generation = None

    def refresh_mode(self) -> DBRefreshMode:
        if self.needs_full_refresh:
            return DBRefreshMode.FULL_REBOOTSTRAP
        if self.pending_count:
            return DBRefreshMode.BOUNDED_SUFFIX
        return DBRefreshMode.CURRENT_SNAPSHOT

    def reconciliation_cursor(self) -> ReconciliationCursor:
        database = self._database
        if database is None or not self.pending_count:
            return ReconciliationCursor((), (), None)

        provisional_keys: list[JobSemanticKey] = []
        seen_attempts: set[tuple[str, int]] = set()
        for event in self._pending_jobs:
            attempt = _job_attempt_key(event)
            if attempt not in self._job_cursors and attempt not in seen_attempts:
                provisional_keys.append(event.semantic_key)
                seen_attempts.add(attempt)

        return ReconciliationCursor(
            tuple(
                sorted(
                    self._job_cursors.values(), key=lambda item: item.job_instance_id
                )
            ),
            tuple(provisional_keys),
            self._workflow_cursor if self._pending_workflow else None,
        )

    def update_health(self, health: SourceHealth) -> None:
        self._health[health.source] = health

    def update_scheduler(self, result: SchedulerQueryResult) -> None:
        self._scheduler[result.request.kind] = result
        by_id: dict[str, list[FrozenPayload]] = defaultdict(list)
        by_name: dict[str, list[FrozenPayload]] = defaultdict(list)
        for evidence in result.evidence:
            target = evidence.target.to_json_dict()
            cluster = target.get("ClusterId")
            process = target.get("ProcId")
            if cluster is not None and process is not None:
                by_id[f"{cluster}.{process}"].append(evidence.payload)
            dag_node = target.get("DAGNodeName")
            if isinstance(dag_node, str):
                by_name[dag_node].append(evidence.payload)
        self._scheduler_by_id[result.request.kind] = {
            key: tuple(values) for key, values in by_id.items()
        }
        self._scheduler_by_name[result.request.kind] = {
            key: tuple(values) for key, values in by_name.items()
        }
        if result.request.kind in {
            SchedulerQueryKind.QUEUE,
            SchedulerQueryKind.HISTORY,
        }:
            self._scheduler_jobs_revision += 1
        self.update_health(result.health)

    def ingest_tail(self, result: TailPollResult) -> bool:
        """Add one bounded tail batch and return whether DB refresh is urgent."""

        self.update_health(result.health)
        ordered_events = sorted(
            (*result.job_events, *result.workflow_events, *result.source_events),
            key=lambda event: event.identity.start_offset,
        )
        events: list[TailJobEvent | TailWorkflowEvent] = [
            event
            for event in ordered_events
            if isinstance(event, (TailJobEvent, TailWorkflowEvent))
        ]
        if any(event.workflow != self.workflow for event in events):
            raise ReconciliationError("tail batch crossed the selected workflow scope")

        generation_changed = (
            result.generation is not None
            and self._tail_generation is not None
            and result.generation != self._tail_generation
        )
        urgent = bool(
            result.gaps
            or generation_changed
            or result.health.state in {HealthState.GAP, HealthState.REATTACHING}
        )
        if urgent:
            # EffectiveSnapshot intentionally permits one tail generation.
            # Freeze the old publication and quarantine both the old pending
            # prefix and replacement-generation input until one full DB base
            # can confirm, discard, or safely rebase them.
            self._quarantined_jobs.extend(self._pending_jobs)
            self._quarantined_workflow.extend(self._pending_workflow)
            self._pending_jobs.clear()
            self._pending_workflow.clear()
            self._job_cursors.clear()
            self._uncursored_attempts.clear()
            self._workflow_cursor = None
            self._force_full_refresh = True
            self._publication_frozen = True
            self._tail_resync = True
            self._tail_gap_active = bool(
                result.gaps or result.health.state is HealthState.GAP
            )
            self._tail_reattaching_active = not self._tail_gap_active

        if result.generation is not None:
            self._tail_generation = result.generation

        incoming_bytes = sum(
            len(event.original_line.encode("utf-8")) for event in events
        )
        buffered_events = (
            *self._pending_jobs,
            *self._pending_workflow,
            *self._quarantined_jobs,
            *self._quarantined_workflow,
        )
        pending_bytes = sum(
            len(event.original_line.encode("utf-8")) for event in buffered_events
        )
        if (
            len(buffered_events) + len(events) > self.max_pending_events
            or pending_bytes + incoming_bytes > self.max_pending_bytes
        ):
            self._pending_jobs.clear()
            self._pending_workflow.clear()
            self._quarantined_jobs.clear()
            self._quarantined_workflow.clear()
            self._job_cursors.clear()
            self._uncursored_attempts.clear()
            self._workflow_cursor = None
            self._force_full_refresh = True
            self._publication_frozen = True
            self._tail_resync = True
            self._tail_gap_active = True
            self._tail_rearm_required = True
            return True

        known_identities = {
            event.identity for event in (*self._pending_jobs, *self._pending_workflow)
        }
        for event in events:
            if event.identity in known_identities:
                continue
            known_identities.add(event.identity)
            if isinstance(event, TailJobEvent):
                database_job = self._database_jobs_by_exec_id.get(event.exec_job_id)
                attempt_visible = bool(
                    database_job is not None
                    and any(
                        attempt.identity.job_submit_seq == event.job_submit_seq
                        for attempt in database_job.attempts
                    )
                )
                if event.base_db_generation is None or not attempt_visible:
                    # The installed DB cut cannot anchor this attempt.  A
                    # semantic-key suffix must start at sequence zero even if a
                    # concurrent refresh discovers the roster before apply.
                    self._uncursored_attempts.add(_job_attempt_key(event))
            rebased = self._rebase_initial_event(event)
            if self._database_rebootstrap or self._publication_frozen:
                if isinstance(rebased, TailJobEvent):
                    self._quarantined_jobs.append(rebased)
                else:
                    self._quarantined_workflow.append(rebased)
            elif isinstance(rebased, TailJobEvent):
                self._pending_jobs.append(rebased)
            else:
                self._pending_workflow.append(rebased)

        self._pending_jobs.sort(key=_event_offset)
        self._pending_workflow.sort(key=_event_offset)
        if self._database is not None and not self._database_rebootstrap:
            self._reconcile_pending(self._database)
            self._ensure_reconciliation_cursors(self._database)

        if (
            not urgent
            and not self._force_full_refresh
            and not self._publication_frozen
            and result.health.state is HealthState.HEALTHY
        ):
            self._tail_resync = False

        terminal_marker = any(
            (
                isinstance(event, TailWorkflowEvent)
                and event.normalized_state == "WORKFLOW_TERMINATED"
            )
            or (
                isinstance(event, TailSourceEvent)
                and event.marker.value == "MONITORD_FINISHED"
            )
            for event in ordered_events
        )
        return urgent or terminal_marker

    def _rebase_initial_event(
        self, event: TailJobEvent | TailWorkflowEvent
    ) -> TailJobEvent | TailWorkflowEvent:
        generation = self.database_generation
        if event.base_db_generation is None and generation is not None:
            return replace(event, base_db_generation=generation)
        return event

    def begin_database_rebootstrap(self) -> None:
        """Quarantine overlays and freeze the last published effective view."""

        if self._database_rebootstrap:
            return
        self._database_rebootstrap = True
        self._publication_frozen = True
        self._force_full_refresh = True
        self._quarantined_jobs.extend(self._pending_jobs)
        self._quarantined_workflow.extend(self._pending_workflow)
        self._pending_jobs.clear()
        self._pending_workflow.clear()
        self._job_cursors.clear()
        self._uncursored_attempts.clear()
        self._workflow_cursor = None

    def apply_database(self, result: DBRefreshResult) -> None:
        """Install one provider result, preserving authority and generations."""

        self.update_health(result.health)
        snapshot = result.snapshot
        if snapshot is None:
            if (
                result.health.state is HealthState.RESYNC
                or result.health.error_code
                in {
                    "database_generation_changed",
                    "database_shrank",
                    "transition_watermark_rollback",
                    "workflow_watermark_rollback",
                    "workflow_identity_missing",
                }
            ):
                self.begin_database_rebootstrap()
            return
        if snapshot.workflow.workflow != self.workflow:
            raise ReconciliationError("database snapshot crossed workflow scope")

        old_generation = self.database_generation
        full = result.request.mode is DBRefreshMode.FULL_REBOOTSTRAP
        if old_generation is not None and snapshot.generation != old_generation:
            if not full:
                self.begin_database_rebootstrap()
                return
            self.begin_database_rebootstrap()

        self._database = snapshot
        self._database_jobs_by_exec_id = {job.exec_job_id: job for job in snapshot.jobs}
        if full:
            self._force_full_refresh = False
            self._database_rebootstrap = False
            waiting_for_old_generation = self._revalidate_quarantine(snapshot)
            self._rebase_pending_to_database(snapshot.generation)
            self._publication_frozen = waiting_for_old_generation
            self._force_full_refresh = waiting_for_old_generation
            self._tail_gap_active = False
            self._tail_reattaching_active = waiting_for_old_generation
        else:
            self._reconcile_pending(snapshot)
        self._ensure_reconciliation_cursors(snapshot)

    def _rebase_pending_to_database(self, generation: DatabaseGeneration) -> None:
        self._pending_jobs = [
            (
                replace(event, base_db_generation=generation)
                if event.base_db_generation is None
                else event
            )
            for event in self._pending_jobs
        ]
        self._pending_workflow = [
            (
                replace(event, base_db_generation=generation)
                if event.base_db_generation is None
                else event
            )
            for event in self._pending_workflow
        ]

    def _revalidate_quarantine(self, database: DatabaseSnapshot) -> bool:
        jobs = list(self._quarantined_jobs)
        workflow = list(self._quarantined_workflow)
        self._quarantined_jobs.clear()
        self._quarantined_workflow.clear()
        discarded = False
        waiting = False

        for event in jobs:
            if self._job_event_reflected(event, database):
                continue
            if self._job_event_later_than_base(event, database):
                if event.identity.source_generation == self._tail_generation:
                    self._pending_jobs.append(
                        replace(event, base_db_generation=database.generation)
                    )
                else:
                    self._quarantined_jobs.append(event)
                    waiting = True
            else:
                discarded = True

        for event in workflow:
            if self._workflow_event_reflected(event, database):
                continue
            if self._workflow_event_later_than_base(event, database):
                if event.identity.source_generation == self._tail_generation:
                    self._pending_workflow.append(
                        replace(event, base_db_generation=database.generation)
                    )
                else:
                    self._quarantined_workflow.append(event)
                    waiting = True
            else:
                discarded = True

        self._pending_jobs.sort(key=_event_offset)
        self._pending_workflow.sort(key=_event_offset)
        self._reconcile_pending(database)
        if discarded:
            self._tail_resync = True
        return waiting

    def _ensure_reconciliation_cursors(self, database: DatabaseSnapshot) -> None:
        """Freeze the DB cut at which each currently pending stream began."""

        pending_attempts = {_job_attempt_key(event) for event in self._pending_jobs}
        self._uncursored_attempts.intersection_update(pending_attempts)
        self._job_cursors = {
            key: watermark
            for key, watermark in self._job_cursors.items()
            if key in pending_attempts
        }
        if pending_attempts:
            watermarks = {item.job_instance_id: item for item in database.watermarks}
            jobs = self._jobs_by_exec_id(database)
            for key in pending_attempts - set(self._job_cursors):
                if key in self._uncursored_attempts:
                    continue
                exec_job_id, submit_seq = key
                job = jobs.get(exec_job_id)
                if job is None:
                    continue
                attempt = next(
                    (
                        item.identity
                        for item in job.attempts
                        if item.identity.job_submit_seq == submit_seq
                    ),
                    None,
                )
                if attempt is not None and attempt.job_instance_id in watermarks:
                    self._job_cursors[key] = watermarks[attempt.job_instance_id]

        if self._pending_workflow:
            if self._workflow_cursor is None:
                self._workflow_cursor = database.workflow_watermark
        else:
            self._workflow_cursor = None

    def _reconcile_pending(self, database: DatabaseSnapshot) -> None:
        self._retire_older_attempts(database)
        self._pending_jobs = self._reconcile_job_events(self._pending_jobs, database)
        self._pending_workflow = self._reconcile_workflow_events(
            self._pending_workflow, database
        )
        self._ensure_reconciliation_cursors(database)

    def _retire_older_attempts_in(
        self, pending: Sequence[TailJobEvent], database: DatabaseSnapshot
    ) -> list[TailJobEvent]:
        jobs = self._jobs_by_exec_id(database)
        retained: list[TailJobEvent] = []
        for event in pending:
            current = jobs.get(event.exec_job_id)
            if (
                current is not None
                and current.current_attempt is not None
                and current.current_attempt.job_submit_seq > event.job_submit_seq
            ):
                continue
            retained.append(event)
        return retained

    def _retire_older_attempts(self, database: DatabaseSnapshot) -> None:
        self._pending_jobs = self._retire_older_attempts_in(
            self._pending_jobs, database
        )

    def _reconcile_job_events(
        self, pending: Sequence[TailJobEvent], database: DatabaseSnapshot
    ) -> list[TailJobEvent]:
        pending_by_attempt: dict[tuple[str, int], list[TailJobEvent]] = defaultdict(
            list
        )
        for event in pending:
            pending_by_attempt[_job_attempt_key(event)].append(event)

        transitions = list(database.recent_transitions)
        known_transition_ids = {transition.identity for transition in transitions}
        jobs = self._jobs_by_exec_id(database)
        for exec_job_id in {event.exec_job_id for event in pending}:
            job = jobs.get(exec_job_id)
            if (
                job is not None
                and job.transition is not None
                and job.transition.identity not in known_transition_ids
            ):
                transitions.append(job.transition)
                known_transition_ids.add(job.transition.identity)

        db_by_attempt: dict[tuple[str, int], dict[int, list[DBJobTransition]]] = (
            defaultdict(lambda: defaultdict(list))
        )
        for transition in transitions:
            db_by_attempt[_db_attempt_key(transition)][
                transition.identity.jobstate_submit_seq
            ].append(transition)

        retained: list[TailJobEvent] = []
        for attempt, attempt_pending in pending_by_attempt.items():
            cursor = 0
            last_reconciled_group: list[DBJobTransition] | None = None
            groups = [
                sorted(group, key=lambda item: item.authoritative_sort_key)
                for _, group in sorted(db_by_attempt.get(attempt, {}).items())
            ]
            for group in groups:
                while cursor < len(attempt_pending):
                    consumed = self._matching_tail_group(
                        attempt_pending[cursor:], group
                    )
                    if consumed:
                        self._remember_job_confirmation_orders(
                            attempt_pending[cursor : cursor + consumed], group
                        )
                        cursor += consumed
                        last_reconciled_group = group
                        break
                    if self._group_supersedes_event(group, attempt_pending[cursor]):
                        cursor += 1
                        last_reconciled_group = group
                        continue
                    break
            if cursor < len(attempt_pending) and last_reconciled_group:
                highest = last_reconciled_group[0].identity.jobstate_submit_seq
                self._job_cursors[attempt] = JobTransitionWatermark(
                    last_reconciled_group[0].identity.job_instance_id,
                    highest,
                    tuple(item.identity for item in last_reconciled_group),
                )
                self._uncursored_attempts.discard(attempt)
            retained.extend(attempt_pending[cursor:])
        return sorted(retained, key=_event_offset)

    @staticmethod
    def _matching_tail_group(
        pending: Sequence[TailJobEvent], group: Sequence[DBJobTransition]
    ) -> int:
        db_states = tuple(item.identity.state for item in group)
        for length in range(min(2, len(pending)), 0, -1):
            candidate = pending[:length]
            if not transition_group_equivalent(
                tuple(event.state for event in candidate), db_states
            ):
                continue
            if Reconciler._group_timestamp_matches(candidate, group):
                return length
        return 0

    @staticmethod
    def _group_timestamp_matches(
        tail: Sequence[TailJobEvent], group: Sequence[DBJobTransition]
    ) -> bool:
        raw_tail = tuple(event.state.strip().upper() for event in tail)
        if raw_tail == ("JOB_HELD", "JOB_HELD_REASON"):
            return any(
                transition.normalized_state == "JOB_HELD"
                and transition.identity.producer_timestamp
                == producer_timestamp_key(tail[-1].event_timestamp)
                for transition in group
            )
        for event in tail:
            if not any(
                transition.normalized_state == event.normalized_state
                and transition.identity.producer_timestamp
                == producer_timestamp_key(event.event_timestamp)
                for transition in group
            ):
                return False
        return True

    @staticmethod
    def _group_supersedes_event(
        group: Sequence[DBJobTransition], event: TailJobEvent
    ) -> bool:
        if not group:
            return False
        latest = max(group, key=lambda item: item.authoritative_sort_key)
        db_state = latest.normalized_state
        tail_state = event.normalized_state
        if db_state not in _KNOWN_JOB_STATES or tail_state not in _KNOWN_JOB_STATES:
            return False
        db_second = Decimal(latest.identity.producer_timestamp)
        tail_second = _producer_second(event.event_timestamp)
        allowed = _SAME_SECOND_SUPERSESSION.get(tail_state, frozenset())
        if db_second > tail_second:
            return db_state in allowed
        if db_second < tail_second:
            return False
        return db_state in allowed

    def _reconcile_workflow_events(
        self, pending: Sequence[TailWorkflowEvent], database: DatabaseSnapshot
    ) -> list[TailWorkflowEvent]:
        self._workflow_identity_conflict = False
        if not pending:
            return []
        assigned = self._assign_workflow_epochs(
            pending, self._workflow_cursor or database.workflow_watermark
        )
        transitions = list(database.recent_workflow_transitions)
        if (
            database.workflow.transition is not None
            and database.workflow.transition.identity
            not in {item.identity for item in transitions}
        ):
            transitions.append(database.workflow.transition)
        transitions.sort(key=lambda item: item.authoritative_sort_key)
        cursor = 0
        last_reconciled: DBWorkflowTransition | None = None
        for transition in transitions:
            while cursor < len(assigned):
                event, restart_count = assigned[cursor]
                if self._workflow_match(event, restart_count, transition):
                    key = self._workflow_event_key(event, restart_count)
                    self._remember_confirmation_order(event, transition)
                    cursor += 1
                    # Equivalent duplicate markers share an epoch and do not
                    # need duplicate workflowstate rows to retire.
                    while (
                        cursor < len(assigned)
                        and self._workflow_event_key(*assigned[cursor]) == key
                    ):
                        cursor += 1
                    last_reconciled = transition
                    break
                if transition.restart_count > restart_count:
                    cursor += 1
                    last_reconciled = transition
                    continue
                if (
                    transition.restart_count == restart_count
                    and transition.normalized_state == "WORKFLOW_TERMINATED"
                    and event.normalized_state == "WORKFLOW_STARTED"
                ):
                    cursor += 1
                    last_reconciled = transition
                    continue
                break
        remaining = assigned[cursor:]
        if remaining and last_reconciled is not None:
            restart_identities = tuple(
                item.identity
                for item in transitions
                if item.restart_count == last_reconciled.restart_count
                and item.authoritative_sort_key
                <= last_reconciled.authoritative_sort_key
            )
            self._workflow_cursor = WorkflowTransitionWatermark(
                WorkflowRestartIdentity(
                    self.workflow,
                    last_reconciled.identity.wf_id,
                    last_reconciled.restart_count,
                ),
                restart_identities,
            )
        for event, restart_count in remaining:
            if any(
                transition.restart_count != restart_count
                and transition.normalized_state == event.normalized_state
                and transition.identity.producer_timestamp
                == producer_timestamp_key(event.event_timestamp)
                for transition in transitions
            ):
                self._workflow_identity_conflict = True
        return [event for event, _ in remaining]

    @staticmethod
    def _assign_workflow_epochs(
        pending: Sequence[TailWorkflowEvent],
        base: WorkflowSnapshot | WorkflowTransitionWatermark,
    ) -> list[tuple[TailWorkflowEvent, int]]:
        if isinstance(base, WorkflowTransitionWatermark):
            restart = base.restart.restart_count
            selected = (
                max(
                    base.identities,
                    key=lambda identity: (
                        0
                        if normalize_workflow_state(identity.state)
                        == "WORKFLOW_STARTED"
                        else 1,
                        identity.timestamp,
                        identity.state,
                    ),
                )
                if base.identities
                else None
            )
            opened = bool(
                selected is not None
                and normalize_workflow_state(selected.state) == "WORKFLOW_STARTED"
            )
        else:
            restart = base.restart_count
            opened = normalize_workflow_state(base.state) == "WORKFLOW_STARTED"
        assigned: list[tuple[TailWorkflowEvent, int]] = []
        for event in pending:
            if event.normalized_state == "WORKFLOW_STARTED":
                if not opened:
                    restart += 1
                    opened = True
                assigned.append((event, restart))
            else:
                assigned.append((event, restart))
                if opened:
                    opened = False
        return assigned

    @staticmethod
    def _workflow_event_key(
        event: TailWorkflowEvent, restart_count: int
    ) -> tuple[int, str, str, int | None]:
        return (
            restart_count,
            event.normalized_state,
            producer_timestamp_key(event.event_timestamp),
            event.status if event.normalized_state == "WORKFLOW_TERMINATED" else None,
        )

    @staticmethod
    def _workflow_match(
        event: TailWorkflowEvent,
        restart_count: int,
        transition: DBWorkflowTransition,
    ) -> bool:
        if (
            transition.restart_count != restart_count
            or transition.normalized_state != event.normalized_state
            or transition.identity.producer_timestamp
            != producer_timestamp_key(event.event_timestamp)
        ):
            return False
        return (
            event.normalized_state != "WORKFLOW_TERMINATED"
            or event.status is None
            or event.status == transition.status
        )

    def _job_event_reflected(
        self, event: TailJobEvent, database: DatabaseSnapshot
    ) -> bool:
        matching = [
            transition
            for transition in database.recent_transitions
            if _db_attempt_key(transition) == _job_attempt_key(event)
        ]
        groups: dict[int, list[DBJobTransition]] = defaultdict(list)
        for transition in matching:
            groups[transition.identity.jobstate_submit_seq].append(transition)
        return any(
            self._matching_tail_group((event,), tuple(group)) == 1
            for group in groups.values()
        )

    def _job_event_later_than_base(
        self, event: TailJobEvent, database: DatabaseSnapshot
    ) -> bool:
        if event.normalized_state not in _KNOWN_JOB_STATES:
            return False
        job = self._jobs_by_exec_id(database).get(event.exec_job_id)
        if job is None:
            return False
        matching_attempt = next(
            (
                attempt.identity
                for attempt in job.attempts
                if attempt.identity.job_submit_seq == event.job_submit_seq
            ),
            None,
        )
        if matching_attempt is None:
            return (
                job.current_attempt is not None
                and event.job_submit_seq > job.current_attempt.job_submit_seq
            )
        if job.transition is None:
            return True
        if job.transition.job_submit_seq != event.job_submit_seq:
            return event.job_submit_seq > job.transition.job_submit_seq
        return _producer_second(event.event_timestamp) > Decimal(
            job.transition.identity.producer_timestamp
        )

    def _workflow_event_reflected(
        self, event: TailWorkflowEvent, database: DatabaseSnapshot
    ) -> bool:
        return any(
            self._workflow_match(event, transition.restart_count, transition)
            for transition in database.recent_workflow_transitions
        )

    @staticmethod
    def _workflow_event_later_than_base(
        event: TailWorkflowEvent, database: DatabaseSnapshot
    ) -> bool:
        transition = database.workflow.transition
        if transition is None:
            return True
        event_second = _producer_second(event.event_timestamp)
        base_second = Decimal(transition.identity.producer_timestamp)
        if event_second > base_second:
            return True
        return (
            event_second == base_second
            and transition.normalized_state == "WORKFLOW_TERMINATED"
            and event.normalized_state == "WORKFLOW_STARTED"
        )

    def build_snapshot(
        self, epoch: SnapshotEpoch, clock: ClockSample
    ) -> EffectiveSnapshot | None:
        """Return an immutable view; never read or wait for a source here."""

        database = self._database
        if database is None:
            return None
        if self._publication_frozen and self._frozen is not None:
            return replace(
                self._frozen,
                epoch=epoch,
                published_at_epoch=clock.epoch,
                published_at_monotonic=clock.monotonic,
                source_health=self._source_health(),
            )

        workflow = self._effective_workflow(database.workflow)
        jobs = self._effective_jobs(database)
        pending: tuple[TailJobEvent | TailWorkflowEvent, ...] = tuple(
            sorted((*self._pending_jobs, *self._pending_workflow), key=_event_offset)
        )
        tail_generation = _same_generation(pending) or self._tail_generation
        events = self._effective_events(database, pending)
        snapshot = EffectiveSnapshot(
            epoch=epoch,
            workflow=workflow,
            jobs=jobs,
            db_generation=database.generation,
            tail_generation=tail_generation,
            published_at_epoch=clock.epoch,
            published_at_monotonic=clock.monotonic,
            source_health=self._source_health(),
            events=events,
        )
        self._frozen = snapshot
        return snapshot

    def _source_health(self) -> tuple[SourceHealth, ...]:
        health = dict(self._health)
        if SourceName.LIVE_TAIL in health and self._tail_gap_active:
            tail = health[SourceName.LIVE_TAIL]
            health[SourceName.LIVE_TAIL] = replace(
                tail,
                state=HealthState.GAP,
                error_code="tail_overlay_gap",
                detail="tail overlay capacity or source continuity was lost",
                pending_count=self.pending_count,
            )
        elif SourceName.LIVE_TAIL in health and self._tail_reattaching_active:
            tail = health[SourceName.LIVE_TAIL]
            health[SourceName.LIVE_TAIL] = replace(
                tail,
                state=HealthState.REATTACHING,
                error_code="tail_reattaching",
                detail="tail generation changed; waiting for a full DB base",
                pending_count=self.pending_count,
            )
        elif self._workflow_identity_conflict and SourceName.LIVE_TAIL in health:
            tail = health[SourceName.LIVE_TAIL]
            health[SourceName.LIVE_TAIL] = replace(
                tail,
                state=HealthState.DEGRADED,
                error_code="workflow_db_identity_conflict",
                detail=(
                    "workflowstate cannot represent a distinct rapid restart with "
                    "the same state and timestamp"
                ),
                pending_count=self.pending_count,
            )
        elif self._tail_resync and SourceName.LIVE_TAIL in health:
            tail = health[SourceName.LIVE_TAIL]
            health[SourceName.LIVE_TAIL] = replace(
                tail,
                state=HealthState.RESYNC,
                error_code="tail_resync",
                detail="tail overlay was reset at a source or DB generation boundary",
                pending_count=self.pending_count,
            )
        return tuple(health[source] for source in sorted(health, key=lambda x: x.value))

    def _effective_workflow(self, base: WorkflowSnapshot) -> WorkflowSnapshot:
        if not self._pending_workflow:
            return base
        state = base.state
        status = base.status
        restart = base.restart_count
        started_at = base.started_at
        ended_at = base.ended_at
        opened = normalize_workflow_state(base.state) == "WORKFLOW_STARTED"
        for event in self._pending_workflow:
            if event.normalized_state == "WORKFLOW_STARTED":
                if not opened:
                    restart += 1
                    opened = True
                    state = "WORKFLOW_STARTED"
                    status = event.status
                    started_at = event.event_timestamp
                    ended_at = None
            elif opened:
                opened = False
                state = "WORKFLOW_TERMINATED"
                status = event.status
                ended_at = event.event_timestamp
        return replace(
            base,
            state=state,
            status=status,
            restart_count=restart,
            started_at=started_at,
            ended_at=ended_at,
            provenance=Provenance.DB_WITH_TAIL_OVERLAY,
            pending_tail=tuple(event.identity for event in self._pending_workflow),
        )

    def _effective_jobs(self, database: DatabaseSnapshot) -> tuple[JobSnapshot, ...]:
        pending_key = tuple(event.identity for event in self._pending_jobs)
        if (
            self._effective_jobs_database is database
            and self._effective_jobs_pending == pending_key
            and self._effective_jobs_scheduler_revision == self._scheduler_jobs_revision
            and self._effective_jobs_cache is not None
        ):
            return self._effective_jobs_cache

        if (
            self._effective_base_database is not database
            or self._effective_base_scheduler_revision != self._scheduler_jobs_revision
        ):
            # Sorting and scheduler enrichment depend only on the authoritative
            # roster and scheduler evidence. Tail-only publications can reuse
            # this base and apply their bounded overlay below.
            ordered = tuple(
                sorted(
                    database.jobs,
                    key=lambda job: (
                        job.job_id is None,
                        job.job_id if job.job_id is not None else 0,
                        job.exec_job_id,
                    ),
                )
            )
            if self._scheduler_jobs_revision:
                enriched: list[JobSnapshot] = []
                for job in ordered:
                    scheduler = self._scheduler_payload(job, job.exec_job_id)
                    enriched.append(
                        job
                        if scheduler == job.scheduler
                        else replace(job, scheduler=scheduler)
                    )
                ordered = tuple(enriched)
            self._effective_base_database = database
            self._effective_base_scheduler_revision = self._scheduler_jobs_revision
            self._effective_base_jobs = ordered

        overlays: dict[str, JobSnapshot] = {}
        provisional: list[JobSnapshot] = []
        pending_by_job: dict[str, list[TailJobEvent]] = defaultdict(list)
        for event in self._pending_jobs:
            pending_by_job[event.exec_job_id].append(event)

        for exec_job_id, events in pending_by_job.items():
            final = events[-1]
            identities = tuple(event.identity for event in events)
            base = self._database_jobs_by_exec_id.get(exec_job_id)
            if base is None:
                provisional.append(
                    JobSnapshot(
                        workflow=self.workflow,
                        job_id=None,
                        exec_job_id=exec_job_id,
                        type_desc="unknown",
                        task_count=0,
                        transformations=(),
                        attempts=(),
                        current_attempt=None,
                        state=final.normalized_state,
                        state_timestamp=final.event_timestamp,
                        transition=None,
                        provenance=Provenance.PROVISIONAL_JOB,
                        pending_tail=identities,
                        scheduler=self._scheduler_payload(None, exec_job_id),
                    )
                )
            else:
                overlays[exec_job_id] = replace(
                    base,
                    state=final.normalized_state,
                    state_timestamp=final.event_timestamp,
                    provenance=Provenance.DB_WITH_TAIL_OVERLAY,
                    pending_tail=identities,
                    scheduler=self._scheduler_payload(base, exec_job_id),
                )

        known: list[JobSnapshot] = []
        unknown: list[JobSnapshot] = provisional
        for base in self._effective_base_jobs:
            effective = overlays.get(base.exec_job_id, base)
            (unknown if effective.job_id is None else known).append(effective)
        unknown.sort(key=lambda job: job.exec_job_id)
        result = (*known, *unknown)
        self._effective_jobs_database = database
        self._effective_jobs_pending = pending_key
        self._effective_jobs_scheduler_revision = self._scheduler_jobs_revision
        self._effective_jobs_cache = result
        return result

    def _jobs_by_exec_id(self, database: DatabaseSnapshot) -> dict[str, JobSnapshot]:
        if database is self._database:
            return self._database_jobs_by_exec_id
        return {job.exec_job_id: job for job in database.jobs}

    def _scheduler_payload(
        self, job: JobSnapshot | None, exec_job_id: str
    ) -> FrozenPayload:
        values: dict[str, object] = {}
        scheduler_ids = {
            attempt.scheduler_id
            for attempt in (job.attempts if job is not None else ())
            if attempt.scheduler_id
        }
        for kind in (SchedulerQueryKind.QUEUE, SchedulerQueryKind.HISTORY):
            matched_payloads: list[FrozenPayload] = []
            for scheduler_id in scheduler_ids:
                matched_payloads.extend(
                    self._scheduler_by_id.get(kind, {}).get(scheduler_id, ())
                )
            if not matched_payloads:
                matched_payloads.extend(
                    self._scheduler_by_name.get(kind, {}).get(exec_job_id, ())
                )
            matched = [payload.to_json_dict() for payload in matched_payloads]
            if matched:
                values[kind.value] = matched[0] if len(matched) == 1 else matched
        return FrozenPayload.from_mapping(values)

    def _effective_events(
        self,
        database: DatabaseSnapshot,
        pending: Iterable[TailJobEvent | TailWorkflowEvent],
    ) -> tuple[EffectiveEvent, ...]:
        values: list[
            tuple[
                Provenance,
                DBJobTransition
                | DBWorkflowTransition
                | TailJobEvent
                | TailWorkflowEvent,
            ]
        ] = []
        values.extend(
            (Provenance.DB_CONFIRMED, transition)
            for transition in database.recent_transitions
        )
        values.extend(
            (Provenance.DB_CONFIRMED, transition)
            for transition in database.recent_workflow_transitions
        )
        values.extend((Provenance.TAIL_PENDING, event) for event in pending)
        return self._ordered_effective_events(values)

    def _ordered_effective_events(
        self,
        values: Iterable[
            tuple[
                Provenance,
                DBJobTransition
                | DBWorkflowTransition
                | TailJobEvent
                | TailWorkflowEvent,
            ]
        ],
    ) -> tuple[EffectiveEvent, ...]:
        effective: list[EffectiveEvent] = []
        for provenance, event in values:
            key = self._effective_event_key(event)
            order = self._event_orders.get(key)
            if order is None:
                self._event_order += 1
                order = self._event_order
                self._event_orders[key] = order
                while len(self._event_orders) > self._max_event_order_keys:
                    self._event_orders.popitem(last=False)
            else:
                self._event_orders.move_to_end(key)
            effective.append(EffectiveEvent(order, provenance, event))
        return tuple(sorted(effective, key=lambda item: item.order))

    def _remember_job_confirmation_orders(
        self,
        tail: Sequence[TailJobEvent],
        group: Sequence[DBJobTransition],
    ) -> None:
        """Alias confirmed rows to their already-published tail event order."""

        raw_tail = tuple(event.state.strip().upper() for event in tail)
        if raw_tail == ("JOB_HELD", "JOB_HELD_REASON"):
            reason = tail[-1]
            match = next(
                (
                    transition
                    for transition in group
                    if transition.normalized_state == "JOB_HELD"
                    and transition.identity.producer_timestamp
                    == producer_timestamp_key(reason.event_timestamp)
                ),
                None,
            )
            if match is not None:
                self._remember_confirmation_order(reason, match)
            return

        available = list(group)
        for event in tail:
            match = next(
                (
                    transition
                    for transition in available
                    if transition.normalized_state == event.normalized_state
                    and transition.identity.producer_timestamp
                    == producer_timestamp_key(event.event_timestamp)
                ),
                None,
            )
            if match is None:
                continue
            self._remember_confirmation_order(event, match)
            available.remove(match)

    def _remember_confirmation_order(
        self,
        tail: TailJobEvent | TailWorkflowEvent,
        confirmed: DBJobTransition | DBWorkflowTransition,
    ) -> None:
        tail_key = self._effective_event_key(tail)
        order = self._event_orders.get(tail_key)
        if order is None:
            return
        confirmed_key = self._effective_event_key(confirmed)
        if confirmed_key in self._event_orders:
            return
        self._event_orders[confirmed_key] = order
        while len(self._event_orders) > self._max_event_order_keys:
            self._event_orders.popitem(last=False)

    @staticmethod
    def _effective_event_key(
        event: DBJobTransition
        | DBWorkflowTransition
        | TailJobEvent
        | TailWorkflowEvent,
    ) -> tuple[object, ...]:
        if isinstance(event, DBJobTransition):
            return ("db-job", event.identity)
        if isinstance(event, DBWorkflowTransition):
            return ("db-workflow", event.identity)
        if isinstance(event, TailJobEvent):
            return ("tail-job", event.identity)
        return ("tail-workflow", event.identity)
