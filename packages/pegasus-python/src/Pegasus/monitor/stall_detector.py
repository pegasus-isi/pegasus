"""Two-strike stall detection driven by coordinator semantic progress."""

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

import math
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from Pegasus.monitor.models import (
    DiagnosticEvidence,
    FrozenPayload,
    HealthState,
    Lifecycle,
    Provenance,
    SourceName,
)

if TYPE_CHECKING:
    from Pegasus.monitor.coordinator import CoordinatorSnapshot


class StallKind(str, Enum):
    ALL_HELD = "all_held"
    NO_SEMANTIC_PROGRESS = "no_semantic_progress"
    RUNNING_PLATEAU = "running_plateau"
    IDLE_TOO_LONG = "idle_too_long"


class StallEventKind(str, Enum):
    DETECTED = "stall_detected"
    RESOLVED = "stall_resolved"


@dataclass(frozen=True, slots=True)
class StallDetectorConfig:
    cooldown_seconds: float = 120.0
    no_progress_seconds: float = 60.0
    running_plateau_seconds: float = 120.0
    idle_threshold_seconds: float = 120.0
    startup_grace_seconds: float = 60.0

    def __post_init__(self) -> None:
        values = (
            self.cooldown_seconds,
            self.no_progress_seconds,
            self.running_plateau_seconds,
            self.idle_threshold_seconds,
            self.startup_grace_seconds,
        )
        if any(not math.isfinite(value) or value < 0 for value in values):
            raise ValueError(
                "stall detector thresholds must be finite and non-negative"
            )


@dataclass(frozen=True, slots=True)
class StallResult:
    event: StallEventKind
    kind: StallKind
    observed_at_epoch: float
    since_monotonic: float
    duration_seconds: float
    summary: str
    total_jobs: int
    succeeded_jobs: int
    failed_jobs: int
    held_jobs: int
    running_jobs: int
    queued_jobs: int
    semantic_progress: int
    evidence: tuple[DiagnosticEvidence, ...]


class StallDetector:
    """Stateful detector; callers supply immutable coordinator publications."""

    def __init__(self, config: StallDetectorConfig | None = None) -> None:
        self.config = config or StallDetectorConfig()
        self._started_monotonic: float | None = None
        self._last_progress_monotonic: float | None = None
        self._last_semantic_progress: int | None = None
        self._last_terminal_count = 0
        self._last_sequence = 0
        self._suspected: StallKind | None = None
        self._suspected_since: float | None = None
        self._confirmed: StallKind | None = None
        self._confirmed_since: float | None = None
        self._last_emit_monotonic = float("-inf")

    @property
    def is_stalled(self) -> bool:
        return self._confirmed is not None

    def check(self, snapshot: CoordinatorSnapshot) -> StallResult | None:
        """Inspect one new publication; snapshot/tail silence is never progress input."""

        now = snapshot.clock.monotonic
        if snapshot.sequence <= self._last_sequence:
            return None
        self._last_sequence = snapshot.sequence
        effective = snapshot.effective
        if effective is None:
            self._clear_suspicion()
            return None
        counts = _counts(effective.jobs)
        terminal = counts[Lifecycle.SUCCEEDED] + counts[Lifecycle.FAILED]
        if self._started_monotonic is None:
            self._started_monotonic = now
            self._last_progress_monotonic = now
            self._last_semantic_progress = snapshot.semantic_progress
            self._last_terminal_count = terminal
            return None

        if not _observation_available(snapshot):
            self._clear_suspicion()
            self._last_progress_monotonic = now
            self._last_semantic_progress = snapshot.semantic_progress
            self._last_terminal_count = terminal
            return None

        if snapshot.authoritative_complete:
            return self._resolve(snapshot, counts, "Workflow completed")
        progressed = (
            self._last_semantic_progress is None
            or snapshot.semantic_progress != self._last_semantic_progress
            or terminal != self._last_terminal_count
        )
        if progressed:
            self._last_semantic_progress = snapshot.semantic_progress
            self._last_terminal_count = terminal
            self._last_progress_monotonic = now
            return self._resolve(snapshot, counts, "Workflow semantic progress resumed")

        total = len(effective.jobs)
        if total == 0 or counts[Lifecycle.UNSUBMITTED] == total:
            self._clear_suspicion()
            return None
        assert self._last_progress_monotonic is not None
        assert self._started_monotonic is not None
        if now - self._started_monotonic < self.config.startup_grace_seconds:
            return None

        elapsed = max(0.0, now - self._last_progress_monotonic)
        triggered, summary = _classify(counts, total, elapsed, self.config)

        if triggered is None:
            self._clear_suspicion()
            return None
        if self._suspected is not triggered:
            self._suspected = triggered
            self._suspected_since = now
            return None
        if (
            self._confirmed is triggered
            and now - self._last_emit_monotonic < self.config.cooldown_seconds
        ):
            return None
        previous_confirmed = self._confirmed
        self._confirmed = triggered
        if self._confirmed_since is None or previous_confirmed is not triggered:
            self._confirmed_since = self._suspected_since or now
        if self._confirmed_since is None:
            self._confirmed_since = self._suspected_since or now
        self._last_emit_monotonic = now
        return self._result(
            snapshot,
            counts,
            StallEventKind.DETECTED,
            triggered,
            self._confirmed_since,
            summary,
        )

    def _resolve(
        self,
        snapshot: CoordinatorSnapshot,
        counts: dict[Lifecycle, int],
        reason: str,
    ) -> StallResult | None:
        previous = self._confirmed
        since = self._confirmed_since
        self._clear_suspicion()
        self._confirmed = None
        self._confirmed_since = None
        if previous is None or since is None:
            return None
        return self._result(
            snapshot,
            counts,
            StallEventKind.RESOLVED,
            previous,
            since,
            reason,
        )

    def _clear_suspicion(self) -> None:
        self._suspected = None
        self._suspected_since = None

    @staticmethod
    def _result(
        snapshot: CoordinatorSnapshot,
        counts: dict[Lifecycle, int],
        event: StallEventKind,
        kind: StallKind,
        since: float,
        summary: str,
    ) -> StallResult:
        total = sum(counts.values())
        assert snapshot.effective is not None
        jobs = snapshot.effective.jobs
        common = {
            "publication_sequence": snapshot.sequence,
            "semantic_progress": snapshot.semantic_progress,
            "authoritative_complete": snapshot.authoritative_complete,
        }
        evidence = [
            DiagnosticEvidence(
                SourceName.STAMPEDE,
                "authoritative_base",
                FrozenPayload.from_mapping(
                    {
                        **common,
                        "workflow_provenance": snapshot.effective.workflow.provenance.value,
                        "db_confirmed_jobs": sum(
                            job.provenance is Provenance.DB_CONFIRMED for job in jobs
                        ),
                    }
                ),
            )
        ]
        live_counts = {
            "tail_overlay_jobs": sum(
                job.provenance is Provenance.DB_WITH_TAIL_OVERLAY for job in jobs
            ),
            "provisional_jobs": sum(
                job.provenance is Provenance.PROVISIONAL_JOB for job in jobs
            ),
            "tail_pending_jobs": sum(
                job.provenance is Provenance.TAIL_PENDING for job in jobs
            ),
            "pending_tail_events": snapshot.pending_tail_events,
        }
        if any(live_counts.values()) or snapshot.effective.workflow.pending_tail:
            evidence.append(
                DiagnosticEvidence(
                    SourceName.LIVE_TAIL,
                    "live_overlay",
                    FrozenPayload.from_mapping(live_counts),
                )
            )
        return StallResult(
            event,
            kind,
            snapshot.clock.epoch,
            since,
            max(0.0, snapshot.clock.monotonic - since),
            summary,
            total,
            counts[Lifecycle.SUCCEEDED],
            counts[Lifecycle.FAILED],
            counts[Lifecycle.HELD],
            counts[Lifecycle.RUNNING],
            counts[Lifecycle.QUEUED],
            snapshot.semantic_progress,
            tuple(evidence),
        )


def _classify(
    counts: dict[Lifecycle, int],
    total: int,
    elapsed: float,
    config: StallDetectorConfig,
) -> tuple[StallKind | None, str]:
    """Name the stall condition, if any, for counts idle for ``elapsed`` seconds.

    Shared by the live detector and the one-shot assessment so both apply the
    same conditions and thresholds.
    """

    active = total - counts[Lifecycle.SUCCEEDED] - counts[Lifecycle.FAILED]
    if counts[Lifecycle.HELD] > 0 and counts[Lifecycle.HELD] == active:
        return (
            StallKind.ALL_HELD,
            f"All {counts[Lifecycle.HELD]} active job(s) are held",
        )
    if (
        counts[Lifecycle.QUEUED] > 0
        and counts[Lifecycle.RUNNING] == 0
        and elapsed >= config.idle_threshold_seconds
    ):
        return (
            StallKind.IDLE_TOO_LONG,
            f"{counts[Lifecycle.QUEUED]} job(s) queued with no running jobs "
            f"and no semantic progress for {elapsed:.0f}s",
        )
    if counts[Lifecycle.RUNNING] > 0 and elapsed >= config.running_plateau_seconds:
        return (
            StallKind.RUNNING_PLATEAU,
            f"{counts[Lifecycle.RUNNING]} running job(s) with no semantic "
            f"progress for {elapsed:.0f}s",
        )
    if (
        counts[Lifecycle.QUEUED] > 0 or counts[Lifecycle.RUNNING] > 0
    ) and elapsed >= config.no_progress_seconds:
        return (
            StallKind.NO_SEMANTIC_PROGRESS,
            f"No effective workflow state change for {elapsed:.0f}s",
        )
    return None, ""


class StallAssessmentStatus(str, Enum):
    STALLED = "stalled"
    NOT_STALLED = "not_stalled"
    NOT_ASSESSED = "not_assessed"


SNAPSHOT_TIMESTAMPS_METHOD = "snapshot_timestamps"


@dataclass(frozen=True, slots=True)
class StallAssessment:
    """Stateless stall verdict for one snapshot; never a confirmed live stall.

    The live :class:`StallDetector` times silence on a monotonic clock and
    confirms a condition over two publications. One sample cannot do either,
    so this measures the wall-clock age of the newest recorded state change
    instead and reports the method it used.
    """

    status: StallAssessmentStatus
    reason: str
    kind: StallKind | None = None
    summary: str = ""
    observed_at_epoch: float | None = None
    last_progress_epoch: float | None = None
    seconds_since_progress: float | None = None
    total_jobs: int = 0
    succeeded_jobs: int = 0
    failed_jobs: int = 0
    held_jobs: int = 0
    running_jobs: int = 0
    queued_jobs: int = 0
    unsubmitted_jobs: int = 0
    config: StallDetectorConfig = StallDetectorConfig()

    def to_json_dict(self) -> dict[str, object]:
        assessed = self.status is not StallAssessmentStatus.NOT_ASSESSED
        return {
            "assessed": assessed,
            "stalled": (
                self.status is StallAssessmentStatus.STALLED if assessed else None
            ),
            "reason": self.reason,
            "kind": None if self.kind is None else self.kind.value,
            "summary": self.summary,
            "method": SNAPSHOT_TIMESTAMPS_METHOD,
            "confirmed": False,
            "observed_at_epoch": self.observed_at_epoch,
            "last_progress_epoch": self.last_progress_epoch,
            "seconds_since_progress": self.seconds_since_progress,
            "job_counts": {
                "total": self.total_jobs,
                "succeeded": self.succeeded_jobs,
                "failed": self.failed_jobs,
                "held": self.held_jobs,
                "running": self.running_jobs,
                "queued": self.queued_jobs,
                "unsubmitted": self.unsubmitted_jobs,
            },
            "thresholds": {
                "no_progress_seconds": self.config.no_progress_seconds,
                "running_plateau_seconds": self.config.running_plateau_seconds,
                "idle_threshold_seconds": self.config.idle_threshold_seconds,
                "startup_grace_seconds": self.config.startup_grace_seconds,
            },
        }


def _last_progress_epoch(effective) -> float | None:
    """Newest state change recorded for the workflow or any of its jobs."""

    workflow = effective.workflow
    stamps = [job.state_timestamp for job in effective.jobs]
    stamps.extend((workflow.started_at, workflow.ended_at))
    if workflow.transition is not None:
        stamps.append(workflow.transition.identity.timestamp)
    known = [float(stamp) for stamp in stamps if stamp is not None]
    return max(known, default=None)


def assess_stall_once(
    snapshot: CoordinatorSnapshot, config: StallDetectorConfig | None = None
) -> StallAssessment:
    """Assess one publication from its own timestamps, without detector state.

    The result separates "not stalled" from "not assessed": a missing
    authoritative base, an unhealthy Stampede source, or a snapshot with no
    timestamps cannot support either verdict.
    """

    config = config or StallDetectorConfig()
    now = snapshot.clock.epoch
    effective = snapshot.effective
    if effective is None:
        return StallAssessment(
            StallAssessmentStatus.NOT_ASSESSED,
            "authoritative_snapshot_unavailable",
            observed_at_epoch=now,
            config=config,
        )
    counts = _counts(effective.jobs)
    total = len(effective.jobs)
    last_progress = _last_progress_epoch(effective)
    elapsed = None if last_progress is None else max(0.0, now - last_progress)

    def result(
        status: StallAssessmentStatus,
        reason: str,
        kind: StallKind | None = None,
        summary: str = "",
    ) -> StallAssessment:
        return StallAssessment(
            status,
            reason,
            kind,
            summary,
            now,
            last_progress,
            elapsed,
            total,
            counts[Lifecycle.SUCCEEDED],
            counts[Lifecycle.FAILED],
            counts[Lifecycle.HELD],
            counts[Lifecycle.RUNNING],
            counts[Lifecycle.QUEUED],
            counts[Lifecycle.UNSUBMITTED],
            config,
        )

    if snapshot.authoritative_complete:
        return result(StallAssessmentStatus.NOT_STALLED, "workflow_complete")
    if not _observation_available(snapshot):
        return result(StallAssessmentStatus.NOT_ASSESSED, "stampede_not_healthy")
    if total == 0 or counts[Lifecycle.UNSUBMITTED] == total:
        return result(StallAssessmentStatus.NOT_STALLED, "no_jobs_submitted")
    if elapsed is None:
        return result(StallAssessmentStatus.NOT_ASSESSED, "progress_time_unavailable")
    started_at = effective.workflow.started_at
    if (
        started_at is not None
        and now - float(started_at) < config.startup_grace_seconds
    ):
        return result(StallAssessmentStatus.NOT_STALLED, "startup_grace")
    kind, summary = _classify(counts, total, elapsed, config)
    if kind is None:
        return result(StallAssessmentStatus.NOT_STALLED, "within_thresholds")
    return result(StallAssessmentStatus.STALLED, kind.value, kind, summary)


def _counts(jobs) -> dict[Lifecycle, int]:
    counts = dict.fromkeys(Lifecycle, 0)
    for job in jobs:
        counts[job.lifecycle] += 1
    return counts


def _observation_available(snapshot: CoordinatorSnapshot) -> bool:
    stampede = tuple(
        health
        for health in snapshot.source_health
        if health.source is SourceName.STAMPEDE
    )
    if not stampede:
        return True
    return any(health.state is HealthState.HEALTHY for health in stampede)
