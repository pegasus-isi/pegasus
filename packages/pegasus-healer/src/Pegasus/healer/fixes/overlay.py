from __future__ import annotations

import hashlib
import json

from Pegasus.healer.models.fixes import AppliedOverlay, FixProposal


def _hash(configuration: dict) -> str:
    canonical = json.dumps(configuration, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def build_overlay(proposal: FixProposal) -> AppliedOverlay:
    """
    Build a versioned overlay from a FixProposal.

    Computes SHA-256 hashes of old and new configurations.
    The hash pair is stored in the DB and used to verify that the retry
    consumed the new configuration.
    """
    return AppliedOverlay(
        fix_id=str(proposal.fix_id),
        old_config=proposal.old_configuration,
        new_config=proposal.proposed_configuration,
        hash_before=_hash(proposal.old_configuration),
        hash_after=_hash(proposal.proposed_configuration),
    )
