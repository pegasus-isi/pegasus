from unittest.mock import MagicMock

import pytest

from Pegasus.db.ensembles import EMError, EnsembleStates
from Pegasus.service.ensembles import manager
from Pegasus.service.ensembles.manager import EnsembleProcessor, WorkflowProcessor


@pytest.fixture
def workflow():
    w = MagicMock(submitdir="/sub dir", state="RUNNING")
    w.name = "wf"
    w.get_logfile.return_value = "/log"
    return w


class TestRemove:
    def test_runs_pegasus_remove(self, mocker, workflow):
        runscript = mocker.patch.object(manager, "runscript")
        mocker.patch.object(manager, "get_script_env", return_value={})

        WorkflowProcessor(None, workflow).remove()

        runscript.assert_called_once_with(
            "pegasus-remove '/sub dir' >>/log 2>&1", env={}
        )

    def test_failure(self, mocker, workflow):
        mocker.patch.object(manager, "runscript", side_effect=EMError("rc 1"))
        mocker.patch.object(manager, "get_script_env", return_value={})

        with pytest.raises(EMError, match="pegasus-remove failed, see /log"):
            WorkflowProcessor(None, workflow).remove()


class TestHandleRunning:
    def _processor(self, dao):
        ensemble = MagicMock(state=EnsembleStates.ACTIVE, workflows=[])
        ep = EnsembleProcessor(dao, ensemble)
        ep.Processor = MagicMock()
        ep.Processor.return_value.pending.return_value = False
        ep.Processor.return_value.running.return_value = False
        return ep

    def test_aborted_meanwhile_is_left_alone(self, workflow):
        dao = MagicMock()

        def abort(w):
            w.state = "ABORTED"

        dao.session.refresh.side_effect = abort

        self._processor(dao).handle_running(workflow)

        workflow.set_state.assert_not_called()

    def test_finished(self, workflow):
        dao = MagicMock()
        ep = self._processor(dao)
        ep.Processor.return_value.running_successful.return_value = True

        ep.handle_running(workflow)

        workflow.set_state.assert_called_once_with("SUCCESSFUL")
