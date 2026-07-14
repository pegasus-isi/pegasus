from unittest.mock import patch

import pytest

from Pegasus.db.schema import MasterWorkflow
from Pegasus.service._query import InvalidQueryError, query_parse


class TestQueryParse:
    """Legitimate query forms must keep parsing into SQLAlchemy clauses."""

    @pytest.mark.parametrize(
        "clause",
        [
            "r.wf_id == 1",
            "r.wf_id.like(2)",
            "r.wf_id.ilike(2)",
            "r.grid_dn is None",
            "r.wf_id < r.timestamp",
            "not (r.wf_id == 1)",
            "r.wf_id == 1 or (r.wf_id.like(2) and r.grid_dn is None)",
        ],
    )
    def test_valid_queries_parse(self, clause):
        result = query_parse(clause, r=MasterWorkflow)
        assert result  # non-empty clause tuple


class TestQueryParseSecurity:
    """The evaluator must reject reflection/call gadgets used for RCE."""

    @pytest.mark.parametrize(
        "clause",
        [
            # Original published exploit chain.
            "r.__init__.__globals__['__builtins__']['__import__']('os').system('id')",
            # Dunder traversal.
            "r.__class__ == 1",
            "r.wf_id.__class__ == 1",
            # Subscripting is no longer supported.
            "r.wf_id['x'] == 1",
            # Calls other than like/ilike are rejected.
            "r.metadata.create_all()",
            # Bare-name (constructor) call.
            "r() == 1",
            # __call__ bypass attempt.
            "r.wf_id.like.__call__(2)",
        ],
    )
    def test_malicious_queries_rejected(self, clause):
        with pytest.raises(InvalidQueryError):
            query_parse(clause, r=MasterWorkflow)

    def test_no_code_execution_during_parse(self):
        """The exploit must never reach os.system, even before raising."""
        with patch("os.system") as mock_system:
            with pytest.raises(InvalidQueryError):
                query_parse(
                    "r.__init__.__globals__['__builtins__']"
                    "['__import__']('os').system('id')",
                    r=MasterWorkflow,
                )
        mock_system.assert_not_called()
