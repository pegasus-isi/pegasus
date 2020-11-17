import importlib
from pathlib import Path

import pytest

from Pegasus.api import *

pegasus_graphviz = importlib.import_module("Pegasus.cli.pegasus-graphviz")


@pytest.fixture(scope="module")
def diamond_wf_file():
    fa = File("f.a")
    fb1 = File("f.b1")
    fb2 = File("f.b2")
    fc1 = File("f.c1")
    fc2 = File("f.c2")
    fd = File("f.d")

    Workflow("blackdiamond").add_jobs(
        Job("preprocess").add_inputs(fa).add_outputs(fb1, fb2),
        Job("findrage").add_inputs(fb1).add_outputs(fc1),
        Job("findrage").add_inputs(fb2).add_outputs(fc2),
        Job("analyze").add_inputs(fc1, fc2).add_outputs(fd),
    ).write("workflow.yml")

    yield "workflow.yml"

    Path("workflow.yml").unlink()


@pytest.fixture(scope="module")
def hierarchical_wf_file():
    analysis_out_file = File("analysis_output")
    h_wf = Workflow("hierarchical-wf")

    analysis_wf_job = SubWorkflow("analysis-wf.yml", is_planned=False).add_outputs(
        analysis_out_file
    )

    sleep_wf_job = SubWorkflow("sleep-wf.yml", is_planned=False)

    ls_job = Job("ls", _id="ls").add_inputs(analysis_out_file).set_stdout("ls_out.txt")

    h_wf.add_jobs(analysis_wf_job, sleep_wf_job, ls_job)
    h_wf.add_dependency(sleep_wf_job, children=[ls_job])

    h_wf.write("h_workflow.yml")

    yield "h_workflow.yml"

    Path("h_workflow.yml").unlink()


class TestEmitDot:
    def test_emit_dot_diamond_wf_yaml_file(self, diamond_wf_file):
        # target dot file
        dot_file = Path("wf.dot")

        # invoke emit_dot on the diamond_wf_file
        dag = pegasus_graphviz.parse_yamlfile(diamond_wf_file, include_files=False)
        dag = pegasus_graphviz.transitivereduction(dag)
        pegasus_graphviz.emit_dot(dag, outfile=str(dot_file))

        with dot_file.open("r") as f:
            result = f.read()

        # check that correct dot file written
        assert result == (
            "digraph dag {\n"
            "    ratio=fill\n"
            '    node [style=filled,color="#444444",fillcolor="#ffed6f"]\n'
            "    edge [arrowhead=normal,arrowsize=1.0]\n\n"
            '    "ID0000001" [shape=ellipse,color="#000000",fillcolor="#1b9e77",label="ID0000001"]\n'
            '    "ID0000002" [shape=ellipse,color="#000000",fillcolor="#d95f02",label="ID0000002"]\n'
            '    "ID0000003" [shape=ellipse,color="#000000",fillcolor="#d95f02",label="ID0000003"]\n'
            '    "ID0000004" [shape=ellipse,color="#000000",fillcolor="#7570b3",label="ID0000004"]\n'
            '    "ID0000001" -> "ID0000002" [color="#000000"]\n'
            '    "ID0000001" -> "ID0000003" [color="#000000"]\n'
            '    "ID0000002" -> "ID0000004" [color="#000000"]\n'
            '    "ID0000003" -> "ID0000004" [color="#000000"]\n'
            "}\n"
        )

        # cleanup
        dot_file.unlink()

    def test_emit_dot_diamond_wf_yaml_file_including_file_nodes(self, diamond_wf_file):
        # target dot file
        dot_file = Path("wf.dot")

        # invoke emit_dot on the diamond wf file, specifying that file nodes
        # be included
        dag = pegasus_graphviz.parse_yamlfile(diamond_wf_file, include_files=True)
        dag = pegasus_graphviz.transitivereduction(dag)
        pegasus_graphviz.emit_dot(dag, outfile=str(dot_file))

        with dot_file.open("r") as f:
            result = f.read()

        # check that correct dot file written
        assert result == (
            "digraph dag {\n"
            "    ratio=fill\n"
            '    node [style=filled,color="#444444",fillcolor="#ffed6f"]\n'
            "    edge [arrowhead=normal,arrowsize=1.0]\n\n"
            '    "ID0000001" [shape=ellipse,color="#000000",fillcolor="#1b9e77",label="ID0000001"]\n'
            '    "ID0000002" [shape=ellipse,color="#000000",fillcolor="#d95f02",label="ID0000002"]\n'
            '    "ID0000003" [shape=ellipse,color="#000000",fillcolor="#d95f02",label="ID0000003"]\n'
            '    "ID0000004" [shape=ellipse,color="#000000",fillcolor="#7570b3",label="ID0000004"]\n'
            '    "f.a" [shape=rect,color="#000000",fillcolor="#ffed6f",label="f.a"]\n'
            '    "f.b1" [shape=rect,color="#000000",fillcolor="#ffed6f",label="f.b1"]\n'
            '    "f.b2" [shape=rect,color="#000000",fillcolor="#ffed6f",label="f.b2"]\n'
            '    "f.c1" [shape=rect,color="#000000",fillcolor="#ffed6f",label="f.c1"]\n'
            '    "f.c2" [shape=rect,color="#000000",fillcolor="#ffed6f",label="f.c2"]\n'
            '    "f.d" [shape=rect,color="#000000",fillcolor="#ffed6f",label="f.d"]\n'
            '    "ID0000001" -> "f.b2" [color="#000000"]\n'
            '    "ID0000001" -> "f.b1" [color="#000000"]\n'
            '    "ID0000002" -> "f.c1" [color="#000000"]\n'
            '    "ID0000003" -> "f.c2" [color="#000000"]\n'
            '    "ID0000004" -> "f.d" [color="#000000"]\n'
            '    "f.a" -> "ID0000001" [color="#000000"]\n'
            '    "f.b1" -> "ID0000002" [color="#000000"]\n'
            '    "f.b2" -> "ID0000003" [color="#000000"]\n'
            '    "f.c1" -> "ID0000004" [color="#000000"]\n'
            '    "f.c2" -> "ID0000004" [color="#000000"]\n'
            "}\n"
        )
        # cleanup
        dot_file.unlink()

    def test_emit_dot_hierarchical_wf_yaml_file(self, hierarchical_wf_file):
        # target dot file
        dot_file = Path("wf.dot")

        # invoke emit_dot on the diamond_wf_file
        dag = pegasus_graphviz.parse_yamlfile(hierarchical_wf_file, include_files=True)
        dag = pegasus_graphviz.transitivereduction(dag)
        pegasus_graphviz.emit_dot(dag, outfile=str(dot_file))

        with dot_file.open("r") as f:
            result = f.read()

        # check that correct dot file written
        assert result == (
            "digraph dag {\n"
            "    ratio=fill\n"
            '    node [style=filled,color="#444444",fillcolor="#ffed6f"]\n'
            "    edge [arrowhead=normal,arrowsize=1.0]\n\n"
            '    "ID0000001" [shape=ellipse,color="#000000",fillcolor="#1b9e77",label="ID0000001"]\n'
            '    "ID0000002" [shape=ellipse,color="#000000",fillcolor="#d95f02",label="ID0000002"]\n'
            '    "analysis-wf.yml" [shape=rect,color="#000000",fillcolor="#ffed6f",label="analysis-wf.yml"]\n'
            '    "analysis_output" [shape=rect,color="#000000",fillcolor="#ffed6f",label="analysis_output"]\n'
            '    "ls" [shape=ellipse,color="#000000",fillcolor="#7570b3",label="ls"]\n'
            '    "ls_out.txt" [shape=rect,color="#000000",fillcolor="#ffed6f",label="ls_out.txt"]\n'
            '    "sleep-wf.yml" [shape=rect,color="#000000",fillcolor="#ffed6f",label="sleep-wf.yml"]\n'
            '    "ID0000001" -> "analysis_output" [color="#000000"]\n'
            '    "ID0000002" -> "ls" [color="#000000"]\n'
            '    "analysis-wf.yml" -> "ID0000001" [color="#000000"]\n'
            '    "analysis_output" -> "ls" [color="#000000"]\n'
            '    "ls" -> "ls_out.txt" [color="#000000"]\n'
            '    "sleep-wf.yml" -> "ID0000002" [color="#000000"]\n'
            "}\n"
        )

        # cleanup
        dot_file.unlink()
