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
        active = total - counts[Lifecycle.SUCCEEDED] - counts[Lifecycle.FAILED]
        triggered: StallKind | None = None
        summary = ""
        if counts[Lifecycle.HELD] > 0 and counts[Lifecycle.HELD] == active:
            triggered = StallKind.ALL_HELD
            summary = f"All {counts[Lifecycle.HELD]} active job(s) are held"
        elif (
            counts[Lifecycle.QUEUED] > 0
            and counts[Lifecycle.RUNNING] == 0
            and elapsed >= self.config.idle_threshold_seconds
        ):
            triggered = StallKind.IDLE_TOO_LONG
            summary = (
                f"{counts[Lifecycle.QUEUED]} job(s) queued with no running jobs "
                f"and no semantic progress for {elapsed:.0f}s"
            )
        elif (
            counts[Lifecycle.RUNNING] > 0
            and elapsed >= self.config.running_plateau_seconds
        ):
            triggered = StallKind.RUNNING_PLATEAU
            summary = (
                f"{counts[Lifecycle.RUNNING]} running job(s) with no semantic "
                f"progress for {elapsed:.0f}s"
            )
        elif (
            counts[Lifecycle.QUEUED] > 0 or counts[Lifecycle.RUNNING] > 0
        ) and elapsed >= self.config.no_progress_seconds:
            triggered = StallKind.NO_SEMANTIC_PROGRESS
            summary = f"No effective workflow state change for {elapsed:.0f}s"

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
