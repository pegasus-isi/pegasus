import importlib
import json
import logging
from collections import namedtuple
from pathlib import Path
from tempfile import NamedTemporaryFile

import cwl_utils.parser_v1_1 as cwl
import pytest

from Pegasus import yaml
from Pegasus.api.errors import DuplicateError
from Pegasus.api.replica_catalog import _ReplicaCatalogEntry, _PFN
from Pegasus.api.writable import _CustomEncoder

# Using import_module because of dashes in pegasus-cwl-converter.py
cwl_converter = importlib.import_module("Pegasus.cli.pegasus-cwl-converter")
build_pegasus_rc = cwl_converter.build_pegasus_rc
build_pegasus_tc = cwl_converter.build_pegasus_tc
build_pegasus_wf = cwl_converter.build_pegasus_wf
collect_files = cwl_converter.collect_files
collect_input_strings = cwl_converter.collect_input_strings
get_basename = cwl_converter.get_basename
get_name = cwl_converter.get_name
load_tr_specs = cwl_converter.load_tr_specs
load_wf_inputs = cwl_converter.load_wf_inputs
main = cwl_converter.main
parse_args = cwl_converter.parse_args


def test_get_basename():
    assert (
        get_basename(
            "file:///Users/ryantanaka/ISI/pegasus/test/core/047-cwl-to-dax/cwl/get_file_sizes.cwl#file_sizes"
        )
        == "file_sizes"
    )


def test_get_name():
    assert (
        get_name(
            "file:///Users/ryantanaka/ISI/pegasus/test/core/047-cwl-to-dax/cwl/workflow.cwl#untar",
            "file:///Users/ryantanaka/ISI/pegasus/test/core/047-cwl-to-dax/cwl/tar.cwl#tar_file",
        )
        == "untar/tar_file"
    )


def test_load_wf_inputs():
    with NamedTemporaryFile("w+") as f:
        yaml.dump({"s1": "abc123"}, f)

        assert load_wf_inputs(f.name) == {"s1": "abc123"}


def test_load_wf_inputs_file_not_found(caplog):
    with pytest.raises(SystemExit):
        load_wf_inputs("xyz0938u4_alkdjfnc.9991acdccegf")

    assert "Unable to find xyz0938u4_alkdjfnc.9991acdccegf" in caplog.text


def test_load_tr_specs():
    tr_specs = {
        "tr_1": {"site": "local", "is_stageable": True},
        "tr_2": {"site": "condorpool", "is_stageable": False},
    }

    with NamedTemporaryFile(mode="w+") as f:
        yaml.dump(tr_specs, f)
        assert load_tr_specs(f.name) == tr_specs


@pytest.mark.parametrize(
    "tr_specs",
    [
        (
            {
                "tr_1": {"sit": "local", "is_stageable": True},
                "tr_2": {"site": "condorpool", "is_stageable": False},
            }
        ),
        (
            {
                "tr_1": {"site": "local", "i_stageable": True},
                "tr_2": {"site": "condorpool", "is_stageable": False},
            }
        ),
    ],
)
def test_load_tr_specs_invalid_format(caplog, tr_specs):
    caplog.set_level(logging.ERROR)

    with NamedTemporaryFile(mode="w+") as f:
        yaml.dump(tr_specs, f)
        with pytest.raises(SystemExit):
            load_tr_specs(f.name)

    assert "Invalid transformation spec file" in caplog.text


def test_load_tr_specs_file_not_found(caplog):
    caplog.set_level(logging.ERROR)
    invalid_file_name = "/bad/file/path/xyz992342"
    with pytest.raises(SystemExit):
        load_tr_specs(invalid_file_name)

    assert (
        "Unable to find transformation spec file: {}".format(invalid_file_name)
        in caplog.text
    )


def test_build_pegasus_tc():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": [],
            "outputs": [],
            "steps": {
                "untar": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/tar",
                        "inputs": [],
                        "outputs": [],
                    },
                    "in": [],
                    "out": [],
                },
                "compile": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/gcc",
                        "inputs": [],
                        "outputs": [],
                    },
                    "in": [],
                    "out": [],
                },
                "get_sizes": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/ryan/get_sizes.sh",
                        "inputs": [],
                        "outputs": [],
                    },
                    "in": [],
                    "out": [],
                },
            },
        }
    )

    tr_specs = {
        "tar": {"site": "condorpool", "is_stageable": False},
        "gcc": {"site": "condorpool", "is_stageable": False},
        # omitting get_sizes as it should be assigned defaults of
        # site: local, is_stageable=True
    }

    tc = build_pegasus_tc(tr_specs, wf)

    assert "None::tar::None" in tc.transformations
    assert "condorpool" in tc.transformations["None::tar::None"].sites
    assert (
        tc.transformations["None::tar::None"].sites["condorpool"].pfn == "/usr/bin/tar"
    )
    assert (
        tc.transformations["None::tar::None"].sites["condorpool"].transformation_type
        == "installed"
    )

    assert "None::gcc::None" in tc.transformations
    assert "condorpool" in tc.transformations["None::gcc::None"].sites
    assert (
        tc.transformations["None::gcc::None"].sites["condorpool"].pfn == "/usr/bin/gcc"
    )
    assert (
        tc.transformations["None::gcc::None"].sites["condorpool"].transformation_type
        == "installed"
    )

    assert "None::get_sizes.sh::None" in tc.transformations
    assert "local" in tc.transformations["None::get_sizes.sh::None"].sites
    assert (
        tc.transformations["None::get_sizes.sh::None"].sites["local"].pfn
        == "/ryan/get_sizes.sh"
    )
    assert (
        tc.transformations["None::get_sizes.sh::None"]
        .sites["local"]
        .transformation_type
        == "stageable"
    )


def test_build_pegasus_tc_command_line_tool_missing_baseCommand():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": [],
            "outputs": [],
            "steps": {
                "untar": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "inputs": [],
                        "outputs": [],
                    },
                    "in": [],
                    "out": [],
                },
            },
        }
    )

    with pytest.raises(ValueError) as e:
        build_pegasus_tc({}, wf)

    assert "requires a 'baseCommand" in str(e)


def test_build_pegasus_tc_command_line_tool_baseCommand_is_not_abspath():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": [],
            "outputs": [],
            "steps": {
                "untar": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "tar",
                        "inputs": [],
                        "outputs": [],
                    },
                    "in": [],
                    "out": [],
                },
            },
        }
    )

    with pytest.raises(ValueError) as e:
        build_pegasus_tc({}, wf)

    assert "must be an absolute path" in str(e)


def test_build_pegasus_tc_duplicate_tr():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": [],
            "outputs": [],
            "steps": {
                "untar1": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/tar",
                        "inputs": [],
                        "outputs": [],
                    },
                    "in": [],
                    "out": [],
                },
                "untar2": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/tar",
                        "inputs": [],
                        "outputs": [],
                    },
                    "in": [],
                    "out": [],
                },
            },
        }
    )

    try:
        build_pegasus_tc({}, wf)
    except DuplicateError:
        pytest.fail("Duplicate error should not have been raised")


def test_build_pegasus_tc_with_containers_using_dockerPull():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": [],
            "outputs": [],
            "steps": {
                "untar": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/tar",
                        "inputs": [],
                        "outputs": [],
                        "requirements": {
                            "DockerRequirement": {"dockerPull": "node:slim"}
                        },
                    },
                    "in": [],
                    "out": [],
                },
            },
        }
    )

    tr_specs = {"tar": {"site": "local", "is_stageable": False}}

    result = json.loads(json.dumps(build_pegasus_tc(tr_specs, wf), cls=_CustomEncoder))

    assert result == {
        "pegasus": "5.0",
        "transformations": [
            {
                "name": "tar",
                "sites": [
                    {
                        "name": "local",
                        "pfn": "/usr/bin/tar",
                        "type": "installed",
                        "container": "node:slim",
                    }
                ],
            }
        ],
        "containers": [
            {
                "name": "node:slim",
                "type": "docker",
                "image": "docker://node:slim",
                "image.site": "local",
            }
        ],
    }


def test_build_pegasus_tc_with_containers_using_dockerLoad():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": [],
            "outputs": [],
            "steps": {
                "untar": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/tar",
                        "inputs": [],
                        "outputs": [],
                        "requirements": {
                            "DockerRequirement": {
                                "dockerLoad": "file:///Users/ryan/docker-image.tar.gz"
                            }
                        },
                    },
                    "in": [],
                    "out": [],
                },
            },
        }
    )

    tr_specs = {"tar": {"site": "local", "is_stageable": False}}

    result = json.loads(json.dumps(build_pegasus_tc(tr_specs, wf), cls=_CustomEncoder))

    assert result == {
        "pegasus": "5.0",
        "transformations": [
            {
                "name": "tar",
                "sites": [
                    {
                        "name": "local",
                        "pfn": "/usr/bin/tar",
                        "type": "installed",
                        "container": "docker-image.tar.gz",
                    }
                ],
            }
        ],
        "containers": [
            {
                "name": "docker-image.tar.gz",
                "type": "docker",
                "image": "file:///Users/ryan/docker-image.tar.gz",
                "image.site": "local",
            }
        ],
    }


def test_build_pegasus_tc_with_duplicate_containers():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": [],
            "outputs": [],
            "steps": {
                "untar": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/tar",
                        "inputs": [],
                        "outputs": [],
                        "requirements": {
                            "DockerRequirement": {
                                "dockerLoad": "file:///Users/ryan/docker-image.tar.gz"
                            }
                        },
                    },
                    "in": [],
                    "out": [],
                },
                "untar2": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/tar",
                        "inputs": [],
                        "outputs": [],
                        "requirements": {
                            "DockerRequirement": {
                                "dockerLoad": "file:///Users/ryan/docker-image.tar.gz"
                            }
                        },
                    },
                    "in": [],
                    "out": [],
                },
            },
        }
    )

    tr_specs = {"tar": {"site": "local", "is_stageable": False}}
    try:
        result = json.loads(
            json.dumps(build_pegasus_tc(tr_specs, wf), cls=_CustomEncoder)
        )
    except DuplicateError:
        pytest.fail("Duplicate error should have been caught.")

    assert result == {
        "pegasus": "5.0",
        "transformations": [
            {
                "name": "tar",
                "sites": [
                    {
                        "name": "local",
                        "pfn": "/usr/bin/tar",
                        "type": "installed",
                        "container": "docker-image.tar.gz",
                    }
                ],
            }
        ],
        "containers": [
            {
                "name": "docker-image.tar.gz",
                "type": "docker",
                "image": "file:///Users/ryan/docker-image.tar.gz",
                "image.site": "local",
            }
        ],
    }


@pytest.mark.parametrize(
    "invalid_field",
    [
        ({"dockerFile": "abc"}),
        ({"dockerImport": "abc"}),
        ({"dockerImageId": "abc"}),
        ({"dockerOutputDirectory": "abc"}),
    ],
)
def test_build_pegasus_tc_with_unsupported_cwl_DockerRequirement_fields(invalid_field):
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": [],
            "outputs": [],
            "steps": {
                "untar": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/tar",
                        "inputs": [],
                        "outputs": [],
                        "requirements": {"DockerRequirement": invalid_field},
                    },
                    "in": [],
                    "out": [],
                },
            },
        }
    )

    tr_specs = {"tar": {"site": "local", "is_stageable": False}}

    with pytest.raises(NotImplementedError) as e:
        build_pegasus_tc(tr_specs, wf)

    assert "Only DockerRequirement." in str(e)


def test_build_pegasus_rc():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": {"input1": "File", "input2": "File"},
            "outputs": [],
            "steps": {},
        }
    )

    wf_inputs = {
        "input1": {"class": "File", "path": "/path/to/input1"},
        "input2": {"class": "File", "path": "/path/to/input2"},
    }

    rc = build_pegasus_rc(wf_inputs, wf)
    assert rc.entries[("input1", False)].lfn == "input1"
    assert rc.entries[("input1", False)].pfns == {_PFN("local", "/path/to/input1")}
    assert rc.entries[("input2", False)].lfn == "input2"
    assert rc.entries[("input2", False)].pfns == {_PFN("local", "/path/to/input2")}


def test_build_pegasus_rc_missing_input(caplog):
    caplog.set_level(logging.ERROR)
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": {"input1": "File", "input2": "File"},
            "outputs": [],
            "steps": {},
        }
    )

    wf_inputs = {
        "input1": {"class": "File", "path": "/path/to/input1"},
    }

    with pytest.raises(SystemExit):
        build_pegasus_rc(wf_inputs, wf)

    assert "Unable to obtain input" in caplog.text


def test_build_pegasus_rc_missing_file_path(caplog):
    caplog.set_level(logging.ERROR)
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": {"input1": "File", "input2": "File"},
            "outputs": [],
            "steps": {},
        }
    )

    wf_inputs = {
        "input1": {"class": "File"},
    }

    with pytest.raises(SystemExit):
        build_pegasus_rc(wf_inputs, wf)

    assert "Unable to obtain a path" in caplog.text


def test_collect_files():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": {"input1": "File", "input2": "string"},
            "outputs": [],
            "steps": {
                "untar": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/tar",
                        "inputs": [],
                        "outputs": {
                            "source_file": {
                                "type": "File",
                                "outputBinding": {"glob": "source_file.cpp"},
                            }
                        },
                    },
                    "in": [],
                    "out": ["source_file"],
                },
                "compile": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/gcc",
                        "inputs": [],
                        "outputs": {
                            "object_file": {
                                "type": "File",
                                "outputBinding": {"glob": "source_file.o"},
                            }
                        },
                    },
                    "in": [],
                    "out": ["object_file"],
                },
            },
        }
    )

    assert collect_files(wf) == {
        "input1": "input1",
        "untar/source_file": "source_file.cpp",
        "compile/object_file": "source_file.o",
    }


def test_collect_files_file_array_input():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": {"input1": "File[]"},
            "outputs": [],
            "steps": {},
        }
    )

    with pytest.raises(NotImplementedError) as e:
        collect_files(wf)

    assert "Support for File[] workflow input" in str(e)


@pytest.mark.parametrize(
    "source_file", [({"type": "File"}), ({"type": "File", "outputBinding": {}})],
)
def test_collect_files_outputBinding_glob_missing(source_file):
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": [],
            "outputs": [],
            "steps": {
                "untar": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/tar",
                        "inputs": [],
                        "outputs": {"source_file": source_file},
                    },
                    "in": [],
                    "out": [],
                },
            },
        }
    )

    with pytest.raises(ValueError) as e:
        collect_files(wf)

    assert "outputBinding.glob" in str(e)


@pytest.mark.parametrize(
    "outputBinding", [({"glob": "*.txt"}), ({"glob": "$(inputs.infile)"})]
)
def test_collect_files_wildcard_in_glob(outputBinding):
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": [],
            "outputs": [],
            "steps": {
                "untar": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/tar",
                        "inputs": {"infile": {"type": "string"}},
                        "outputs": {
                            "source_file": {
                                "type": "File",
                                "outputBinding": outputBinding,
                            }
                        },
                    },
                    "in": [],
                    "out": [],
                },
            },
        }
    )

    with pytest.raises(NotImplementedError) as e:
        collect_files(wf)

    assert "Unable to resolve wildcards" in str(e)


def test_collect_files_file_array_output():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": [],
            "outputs": [],
            "steps": {
                "untar": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/usr/bin/tar",
                        "inputs": {},
                        "outputs": {"source_files": {"type": "File[]",}},
                    },
                    "in": [],
                    "out": [],
                },
            },
        }
    )

    with pytest.raises(NotImplementedError) as e:
        collect_files(wf)

    assert "Support for output types other than File is in development" in str(e)


def test_collect_input_strings():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": {"str1": "string", "str2": "string[]", "f1": "File"},
            "outputs": [],
            "steps": {},
        }
    )

    wf_inputs = {
        "str1": "str1 content",
        "str2": ["str2_0", "str2_1"],
        "f1": {"class": "File"},
    }

    assert collect_input_strings(wf_inputs, wf) == {
        "str1": "str1 content",
        "str2": ["str2_0", "str2_1"],
    }


def test_collect_input_strings_missing_from_input_spec_file(caplog):
    caplog.set_level(logging.ERROR)
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": {"str1": "string", "str2": "string[]", "f1": "File"},
            "outputs": [],
            "steps": {},
        }
    )

    wf_inputs = {
        "str1": "str1 content",
    }

    with pytest.raises(SystemExit):
        collect_input_strings(wf_inputs, wf)

    assert "Unable to obtain input" in caplog.text


def test_build_pegasus_wf():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": {"input1": "File", "input2": "string", "input3": "string[]"},
            "outputs": {"f": {"type": "File", "outputSource": "step2/output_file"}},
            "steps": {
                "step1": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/ryan/write_file",
                        "arguments": ["--arg1", "1"],
                        "inputs": {
                            "input_file": {
                                "type": "File",
                                "inputBinding": {
                                    "prefix": "-c",
                                    "separate": True,
                                    "position": 2,
                                },
                            },
                            "input_string": {
                                "type": "string",
                                "inputBinding": {"position": 0},
                            },
                            "input_string[]": {
                                "type": "string[]",
                                "inputBinding": {
                                    "itemSeparator": ",",
                                    "prefix": "-i=",
                                    "position": 1,
                                },
                            },
                        },
                        "outputs": {
                            "write_file_output1": {
                                "type": "File",
                                "outputBinding": {"glob": "write_file_output1.txt"},
                            },
                            "write_file_output2": {
                                "type": "File",
                                "outputBinding": {"glob": "write_file_output2.txt"},
                            },
                        },
                    },
                    "in": {
                        "input_file": "input1",
                        "input_string": "input2",
                        "input_string[]": "input3",
                    },
                    "out": ["write_file_output1", "write_file_output2"],
                },
                "step2": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/ryan/process_file",
                        "arguments": ["--arg", "2"],
                        "inputs": {
                            "input_file1": {
                                "type": "File",
                                "inputBinding": {"position": 2},
                            },
                            "input_file2": {
                                "type": "File",
                                "inputBinding": {"prefix": "-f=", "separate": False},
                            },
                        },
                        "outputs": {
                            "output_file": {
                                "type": "File",
                                "outputBinding": {"glob": "output_file.txt"},
                            }
                        },
                    },
                    "in": {
                        "input_file1": "step1/write_file_output1",
                        "input_file2": "step1/write_file_output2",
                    },
                    "out": ["output_file"],
                },
            },
        }
    )

    wf_inputs = {
        "input1": {"class": "File", "path": "/path/to/input1"},
        "input2": "input2 is a string",
        "input3": ["input3", "is", "a", "string[]"],
    }

    wf_files = collect_files(wf)
    wf_input_str = collect_input_strings(wf_inputs, wf)

    wf = build_pegasus_wf(wf, wf_files, wf_input_str)
    result = json.loads(json.dumps(wf, cls=_CustomEncoder))
    result["jobs"] = sorted(result["jobs"], key=lambda job: job["id"])

    for job in result["jobs"]:
        job["uses"] = sorted(job["uses"], key=lambda u: u["lfn"])

    expected = {
        "jobDependencies": [],
        "jobs": [
            {
                "arguments": [
                    "--arg1",
                    "1",
                    "input2 is a string",
                    "-i=input3,is,a,string[]",
                    "-c input1",
                ],
                "id": "step1",
                "name": "write_file",
                "type": "job",
                "uses": [
                    {
                        "lfn": "write_file_output1.txt",
                        "registerReplica": True,
                        "stageOut": True,
                        "type": "output",
                    },
                    {
                        "lfn": "write_file_output2.txt",
                        "registerReplica": True,
                        "stageOut": True,
                        "type": "output",
                    },
                    {"lfn": "input1", "type": "input",},
                ],
            },
            {
                "arguments": [
                    "--arg",
                    "2",
                    "-f=write_file_output2.txt",
                    "write_file_output1.txt",
                ],
                "id": "step2",
                "name": "process_file",
                "type": "job",
                "uses": [
                    {"lfn": "write_file_output1.txt", "type": "input"},
                    {"lfn": "write_file_output2.txt", "type": "input"},
                    {
                        "lfn": "output_file.txt",
                        "registerReplica": True,
                        "stageOut": True,
                        "type": "output",
                    },
                ],
            },
        ],
        "name": "cwl-converted-pegasus-workflow",
        "pegasus": "5.0",
    }

    expected["jobs"] = sorted(expected["jobs"], key=lambda job: job["id"])

    for job in expected["jobs"]:
        job["uses"] = sorted(job["uses"], key=lambda u: u["lfn"])

    assert result == expected


def test_build_pegasus_wf_job_contains_non_file_output():
    wf = cwl.load_document(
        {
            "cwlVersion": "v1.1",
            "class": "Workflow",
            "inputs": {},
            "outputs": {},
            "steps": {
                "step1": {
                    "run": {
                        "cwlVersion": "v1.1",
                        "class": "CommandLineTool",
                        "baseCommand": "/command",
                        "inputs": {},
                        "outputs": {"unsupported_output_type": {"type": "File[]",}},
                    },
                    "in": {},
                    "out": ["unsupported_output_type"],
                },
            },
        }
    )

    with pytest.raises(NotImplementedError) as e:
        build_pegasus_wf(wf, {}, {})

    assert "Support for output types other than File" in str(e)


def test_main(mocker):
    Args = namedtuple(
        "Args",
        [
            "cwl_workflow_file_path",
            "workflow_inputs_file_path",
            "transformation_spec_file_path",
            "output_file_path",
            "debug",
        ],
    )

    with NamedTemporaryFile("w+") as cwl_wf_file, NamedTemporaryFile(
        "w+"
    ) as wf_input_spec_file, NamedTemporaryFile("w+") as tr_spec_file:

        # saving path to be used in pegasus wf id
        cwl_wf_file.name

        # return value of patched parse_args()
        converted_wf_file_path = Path.cwd() / "wf.yml"
        args = Args(
            cwl_wf_file.name,
            wf_input_spec_file.name,
            tr_spec_file.name,
            str(converted_wf_file_path),
            True,
        )

        # write cwl_wf_file
        yaml.dump(
            {
                "cwlVersion": "v1.1",
                "class": "Workflow",
                "inputs": {"if": "File"},
                "outputs": {},
                "steps": {
                    "step1": {
                        "run": {
                            "cwlVersion": "v1.1",
                            "class": "CommandLineTool",
                            "baseCommand": "/command",
                            "inputs": {"input_file": {"type": "File",}},
                            "outputs": {
                                "output_file": {
                                    "type": "File",
                                    "outputBinding": {"glob": "output.txt"},
                                }
                            },
                        },
                        "in": {"input_file": "if"},
                        "out": ["output_file"],
                    },
                },
            },
            cwl_wf_file,
        )

        # write wf_input_spec_file
        yaml.dump(
            {"if": {"class": "File", "path": "/path/to/file.txt"}}, wf_input_spec_file
        )

        # write tr_spec_file
        yaml.dump(
            {"command": {"site": "condorpool", "is_stageable": False}}, tr_spec_file
        )

        mocker.patch("Pegasus.cli.pegasus-cwl-converter.parse_args", return_value=args)

        assert main() == 0

    with open(converted_wf_file_path) as f:
        result = yaml.load(f)

    for job in result["jobs"]:
        job["uses"] = sorted(job["uses"], key=lambda u: u["lfn"])

    expected = {
        "jobDependencies": [],
        "jobs": [
            {
                "arguments": [],
                "id": "step1",
                "name": "command",
                "type": "job",
                "uses": [
                    {"lfn": "if", "type": "input",},
                    {
                        "lfn": "output.txt",
                        "registerReplica": True,
                        "stageOut": True,
                        "type": "output",
                    },
                ],
            }
        ],
        "name": "cwl-converted-pegasus-workflow",
        "pegasus": "5.0",
        "replicaCatalog": {
            "replicas": [
                {
                    "lfn": "if", 
                    "pfns": [{"pfn": "/path/to/file.txt", "site": "local"}]
                }
            ]
        },
        "transformationCatalog": {
            "transformations": [
                {
                    "name": "command",
                    "sites": [
                        {"name": "condorpool", "pfn": "/command", "type": "installed"}
                    ],
                }
            ]
        },
    }

    for job in expected["jobs"]:
        job["uses"] = sorted(job["uses"], key=lambda u: u["lfn"])

    assert result == expected

    # cleanup converted file
    converted_wf_file_path.unlink()
