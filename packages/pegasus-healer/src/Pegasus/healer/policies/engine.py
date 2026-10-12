from __future__ import annotations

from Pegasus.healer.models.context import FailureContext
from Pegasus.healer.models.diagnosis import Diagnosis
from Pegasus.healer.models.fixes import FixProposal, PolicyDecision, PolicyDecisionRecord
from Pegasus.healer.policies.loader import PolicyConfig

POLICY_VERSION = "1.0.0"

# Actions that are always STOP — they touch scientific logic
FORBIDDEN_ACTIONS: set[str] = {
    "MODIFY_EXECUTABLE",
    "MODIFY_ALGORITHM",
    "SCIENTIFIC_LOGIC_CHANGE",
}


class PolicyEngine:
    """
    Deterministic safety gate.

    Evaluates a FixProposal against the loaded YAML policy and a set of
    invariants defined in the spec.  Returns a PolicyDecision.

    Key guarantee: deterministic policy always overrides LLM suggestions.
    """

    def __init__(self, policy: PolicyConfig, confidence_threshold: float = 0.80) -> None:
        self._policy = policy
        # Active mode's threshold takes precedence over the settings default.
        self._threshold = (
            policy.confidence_threshold
            if policy.confidence_threshold is not None
            else confidence_threshold
        )

    def validate(
        self,
        proposal: FixProposal,
        diagnosis: Diagnosis,
        ctx: FailureContext,
    ) -> PolicyDecisionRecord:
        """
        Run all policy checks in order.

        Returns an immutable PolicyDecisionRecord with the final decision,
        the full list of passed/failed checks, and a human-readable reason.
        """
        passed: list[str] = []
        failed: list[str] = []

        def _stop(check: str, reason: str) -> PolicyDecisionRecord:
            failed.append(check)
            return PolicyDecisionRecord(
                fix_id=str(proposal.fix_id),
                decision=PolicyDecision.STOP,
                policy_version=POLICY_VERSION,
                checks_passed=passed,
                checks_failed=failed,
                reason=reason,
            )

        def _escalate(check: str, reason: str) -> PolicyDecisionRecord:
            failed.append(check)
            return PolicyDecisionRecord(
                fix_id=str(proposal.fix_id),
                decision=PolicyDecision.ESCALATE,
                policy_version=POLICY_VERSION,
                checks_passed=passed,
                checks_failed=failed,
                reason=reason,
            )

        def _ask(check: str, reason: str) -> PolicyDecisionRecord:
            failed.append(check)
            return PolicyDecisionRecord(
                fix_id=str(proposal.fix_id),
                decision=PolicyDecision.ASK,
                policy_version=POLICY_VERSION,
                checks_passed=passed,
                checks_failed=failed,
                reason=reason,
            )

        # 0. Healer tags — checked before everything else
        tags = set(ctx.job_tags)
        if "no-fix" in tags:
            return PolicyDecisionRecord(
                fix_id=str(proposal.fix_id),
                decision=PolicyDecision.STOP,
                policy_version=POLICY_VERSION,
                checks_passed=passed,
                checks_failed=["healer_tag:no-fix"],
                reason="Job tag 'no-fix': diagnose only, no fix applied.",
            )
        if "stop" in tags:
            return PolicyDecisionRecord(
                fix_id=str(proposal.fix_id),
                decision=PolicyDecision.STOP,
                policy_version=POLICY_VERSION,
                checks_passed=passed,
                checks_failed=["healer_tag:stop"],
                reason="Job tag 'stop': abort entire workflow on failure.",
            )
        if "stop-jobs" in tags:
            return PolicyDecisionRecord(
                fix_id=str(proposal.fix_id),
                decision=PolicyDecision.STOP,
                policy_version=POLICY_VERSION,
                checks_passed=passed,
                checks_failed=["healer_tag:stop-jobs"],
                reason="Job tag 'stop-jobs': stop all jobs of the same transformation family.",
            )
        passed.append("healer_tags_ok")

        # 1. Diagnosis confidence
        if diagnosis.confidence < self._threshold:
            return _escalate(
                f"confidence_below_threshold:{diagnosis.confidence:.2f}",
                f"Diagnosis confidence {diagnosis.confidence:.2f} < threshold {self._threshold}.",
            )
        passed.append("confidence_ok")

        # 2. Action is not forbidden
        if proposal.action.value in FORBIDDEN_ACTIONS:
            return _stop(
                f"forbidden_action:{proposal.action.value}",
                f"Action '{proposal.action.value}' modifies scientific logic or executable.",
            )
        passed.append("action_allowed")

        # 3. Scope level check — agent must have sufficient access for this fix
        failure_type = diagnosis.failure_type.value
        if not self._policy.scope_allows(failure_type):
            required = self._policy.get_required_level(failure_type)
            current  = self._policy.scope_level
            fallback = self._policy.get_fallback_decision(failure_type)
            failed.append(f"scope_insufficient:{current}<{required}")
            decision = PolicyDecision.ESCALATE
            try:
                decision = PolicyDecision(fallback)
            except ValueError:
                pass
            if decision == PolicyDecision.ASK:
                reason = (
                    f"Scope '{current}' cannot apply this fix automatically "
                    f"(requires '{required}' access). Fix suggestion provided — apply manually."
                )
            else:
                reason = (
                    f"Agent scope '{current}' does not meet required level '{required}' "
                    f"for {failure_type}. Grant '{required}' access via AGENT_SCOPE_LEVEL."
                )
            return PolicyDecisionRecord(
                fix_id=str(proposal.fix_id),
                decision=decision,
                policy_version=POLICY_VERSION,
                checks_passed=passed,
                checks_failed=failed,
                reason=reason,
            )
        passed.append(f"scope_ok:{self._policy.scope_level}>={self._policy.get_required_level(failure_type)}")

        # 4. Attempt count within limit
        max_attempts = self._policy.get_max_attempts(failure_type)
        if ctx.attempt_number > max_attempts:
            return _stop(
                f"attempt_limit:{ctx.attempt_number}>{max_attempts}",
                f"Max attempts ({max_attempts}) reached for {failure_type}.",
            )
        passed.append("attempt_count_ok")

        # 5. Resource ceilings
        ceiling_violation = self._check_ceilings(proposal, failure_type)
        if ceiling_violation:
            return _stop(ceiling_violation, f"Resource ceiling exceeded: {ceiling_violation}.")
        passed.append("resource_ceilings_ok")

        # 5. No repeated ineffective fix
        for prev in ctx.previous_fixes:
            if prev.action == proposal.action.value and prev.outcome == "INEFFECTIVE":
                return _stop(
                    "repeated_ineffective_fix",
                    f"Fix '{proposal.action.value}' was already applied and marked INEFFECTIVE.",
                )
        passed.append("no_repeated_ineffective_fix")

        # 6. Configuration actually differs (skip for no-change transient retry)
        is_no_change = proposal.action.value == "NO_ACTION_TRANSIENT_RETRY"
        if (
            not is_no_change
            and proposal.old_configuration
            and proposal.old_configuration == proposal.proposed_configuration
        ):
            return _stop(
                "no_configuration_change",
                "Proposed configuration is identical to the current configuration.",
            )
        passed.append("configuration_differs")

        # 7. No scientific logic modification (requires_human_review from diagnosis)
        if diagnosis.requires_human_review:
            return _ask("requires_human_review", "Diagnosis requires human review.")
        passed.append("no_scientific_logic_change")

        # 8. Final decision from policy YAML or approval flag
        policy_decision_str = self._policy.get_decision(failure_type)
        if proposal.requires_approval:
            policy_decision_str = "ASK"

        try:
            decision = PolicyDecision(policy_decision_str)
        except ValueError:
            decision = PolicyDecision.ASK

        return PolicyDecisionRecord(
            fix_id=str(proposal.fix_id),
            decision=decision,
            policy_version=POLICY_VERSION,
            checks_passed=passed,
            checks_failed=failed,
            reason="All policy checks passed.",
        )

    def _check_ceilings(self, proposal: FixProposal, failure_type: str) -> str | None:
        cfg = proposal.proposed_configuration
        ft = failure_type

        max_mem = self._policy.get_ceiling(ft, "maximum_memory_mb")
        if max_mem and cfg.get("memory_mb", 0) > max_mem:
            return f"memory_mb:{cfg['memory_mb']}>{max_mem}"

        max_disk = self._policy.get_ceiling(ft, "maximum_disk_mb")
        if max_disk and cfg.get("disk_mb", 0) > max_disk:
            return f"disk_mb:{cfg['disk_mb']}>{max_disk}"

        max_runtime = self._policy.get_ceiling(ft, "maximum_runtime_seconds")
        if max_runtime and cfg.get("runtime_seconds", 0) > max_runtime:
            return f"runtime_seconds:{cfg['runtime_seconds']}>{max_runtime}"

        return None
