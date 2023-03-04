import os
import glob
import logging
import shutil
import tempfile
from collections import defaultdict
from textwrap import dedent

import pytest

from Pegasus.analyzer import AnalyzeDB, AnalyzeFiles, DebugWF, Options
from Pegasus.db import connection

directory = os.path.dirname(__file__)


@pytest.fixture(scope="function")
def AnalyzerDatabase():
    return AnalyzeDB

@pytest.fixture(scope="function")
def AnalyzerFiles():
    return AnalyzeFiles

@pytest.fixture(scope="function")
def AnalyzerDebug():
    return DebugWF

@pytest.fixture(scope="function")
def AnalyzerOptions():
    return Options


class TestAnalyzeDB:
    def test_should_create_AnalyzeDB(AnalyzerDatabase):
        assert AnalyzerDatabase is not None    
    
    
    def test_with_submit_dir(mocker, capsys, AnalyzerDatabase):
        submit_dir = os.path.join(directory, "analyzer_samples_dir/process_wf_success")
        analyze = AnalyzerDatabase(Options(input_dir=submit_dir))
        analyze.analyze_db(None)
        captured = capsys.readouterr()
        captured_output = captured.out.lstrip()
        expected_output = dedent(
                f"""\
                ************************************Summary*************************************

                 Submit Directory   : {submit_dir}
                 Workflow Status    : success
                 Total jobs         :      5 (100.00%)
                 # jobs succeeded   :      5 (100.00%)
                 # jobs failed      :      0 (0.00%)
                 # jobs held        :      0 (0.00%)
                 # jobs unsubmitted :      0 (0.00%)
 
                """
            )
        assert captured_output[captured_output.find("*"):] == expected_output.lstrip()   
