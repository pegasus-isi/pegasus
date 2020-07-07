#!/usr/bin/env python3
import argparse
import logging
import sys
from pathlib import Path, PurePath

from cwl_utils import parser_v1_1 as cwl
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from Pegasus import yaml
from Pegasus.api import (
    Container,
    File,
    Job,
    ReplicaCatalog,
    Transformation,
    TransformationCatalog,
    Workflow,
)
from Pegasus.api.errors import DuplicateError

log = logging.getLogger("logger")

# --- logging ------------------------------------------------------------------
class ColoredFormatter(logging.Formatter):
    # printing debug level logs in yellow
    debug_format_colored = (
        "\u001b[33m%(asctime)s %(levelname)7s:  [%(funcName)s:%(lineno)d] "
        "%(message)s \u001b[0m"
    )

    debug_format = (
        "%(asctime)s %(levelname)7s:  [%(funcName)s:%(lineno)d] " "%(message)s"
    )

    def __init__(self):
        super().__init__("%(asctime)s %(levelname)7s:  %(message)s")

    def format(self, record):

        # save the original format
        fmt_orig = self._style._fmt

        if record.levelno == logging.DEBUG:
            self._style._fmt = ColoredFormatter.debug_format_colored
        else:
            self._style._fmt = ColoredFormatter.debug_format

        # Call the original formatter class to do the grunt work
        result = logging.Formatter.format(self, record)

        # revert format back to original
        self._style._fmt = fmt_orig

        return result


def setup_logger(debug_flag):

    # log to the console
    console = logging.StreamHandler()

    # default log level - make logger/console match
    log.setLevel(logging.INFO)
    console.setLevel(logging.INFO)

    # debug - from command line
    if debug_flag:
        log.setLevel(logging.DEBUG)
        console.setLevel(logging.DEBUG)

    # formatter
    console.setFormatter(ColoredFormatter())
    log.addHandler(console)
    log.debug("Logger has been configured")


# --- args ---------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Converts a cwl workflow into Pegasus's native format.\n"
            "Note that this is a best-effort conversion and minor adjustments may\n"
            "be needed for proper execution."
        )
    )
    parser.add_argument(
        "cwl_workflow_file_path",
        help="Path to the file containing the CWL Workflow class.",
    )
    parser.add_argument(
        "workflow_inputs_file_path", help="YAML file describing the workflow inputs."
    )

    parser.add_argument(
        "transformation_spec_file_path",
        help=(
            "YAML file describing pegasus specific transformation properties.\n"
            "Given in the following format: {<tr name>: {site: <site name>, is_stageable: <bool>}, ...}"
        ),
    )

    parser.add_argument(
        "output_file_path", help="Desired path of the generated DAX file."
    )

    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        dest="debug",
        help="Enables debugging output.",
    )

    return parser.parse_args()


# --- cwl -> pegasus conversion functions --------------------------------------


def get_basename(name):
    return name.split("#")[1]


def get_name(parent_id: str, _id: str) -> str:
    return get_basename(parent_id) + "/" + get_basename(_id)


def load_wf_inputs(input_spec_file_path: str) -> dict:
    try:
        with open(input_spec_file_path) as f:
            wf_inputs = yaml.load(f)

        log.info("Loaded workflow inputs file: {}".format(input_spec_file_path))
    except FileNotFoundError:
        log.exception("Unable to find {}".format(input_spec_file_path))
        sys.exit(1)

    return wf_inputs


def load_tr_specs(tr_specs_file_path: str) -> dict:
    log.info("Validating {}".format(tr_specs_file_path))
    schema = {
        "type": "object",
        "patternProperties": {
            ".+": {
                "type": "object",
                "properties": {
                    "site": {"type": "string"},
                    "is_stageable": {"type": "boolean"},
                },
                "required": ["site", "is_stageable"],
                "additionalPropertes": False,
            }
        },
    }

    try:
        with open(tr_specs_file_path) as f:
            specs = yaml.load(f)

        validate(instance=specs, schema=schema)
    except ValidationError:
        log.exception(
            "Invalid transformation spec file. File should be in the following format:\n"
            "\t\t\t<tr name1>:\n"
            "\t\t\t    site: <site name>\n"
            "\t\t\t    is_stageable: <boolean>\n"
            "\t\t\t<tr name2>:\n"
            "\t\t\t    site: <site name>\n"
            "\t\t\t    is_stageable: <boolean>\n"
            "\t\t\t...\n"
        )
        sys.exit(1)
    except FileNotFoundError:
        log.exception(
            "Unable to find transformation spec file: {}".format(tr_specs_file_path)
        )
        sys.exit(1)

    log.info("Successfully loaded {}".format(tr_specs_file_path))

    return specs


def build_pegasus_tc(tr_specs: dict, cwl_wf: cwl.Workflow) -> TransformationCatalog:
    log.info("Building transformation catalog")
    tc = TransformationCatalog()

    for step in cwl_wf.steps:
        cwl_cmd_ln_tool = (
            cwl.load_document(step.run) if isinstance(step.run, str) else step.run
        )

        if cwl_cmd_ln_tool.baseCommand is None:
            raise ValueError("{} requires a 'baseCommand'".format(cwl_cmd_ln_tool.id))

        tool_path = PurePath(cwl_cmd_ln_tool.baseCommand)

        if not tool_path.is_absolute():
            raise ValueError(
                "{}.baseCommand: {} must be an absolute path".format(
                    cwl_cmd_ln_tool.id, cwl_cmd_ln_tool.baseCommand
                )
            )

        log.debug("baseCommand: {}".format(tool_path))

        # TODO: handle requirements (may not be needed or can manually add them in)
        site = "local"
        is_stageable = True

        try:
            site = tr_specs[tool_path.name]["site"]
            is_stageable = tr_specs[tool_path.name]["is_stageable"]
        except KeyError:
            log.warning(
                "Unable to look up transformation: {} in transformation spec file. Using defaults: site='local', is_stageable=True".format(
                    tool_path.name
                )
            )

        container_name = None
        if cwl_cmd_ln_tool.requirements:
            for req in cwl_cmd_ln_tool.requirements:
                if isinstance(req, cwl.DockerRequirement):
                    """
                    Currently not supported in DockerRequirement:
                    - dockerFile
                    - dockerImport
                    - dockerImageId
                    - dockerOutputDirectory
                    """

                    # assume to be docker because we can't distinguish between
                    # docker and singularity just by image name or url of zipped
                    # file
                    if req.dockerPull:
                        container_name = req.dockerPull

                        # planner won't allow deep lfn (for example: opensicencegrid/osg-el7 is invalid)
                        container_name = container_name.replace("/", "_")
                        image = "docker://" + req.dockerPull
                    elif req.dockerLoad:
                        image = req.dockerLoad
                        container_name = Path(req.dockerLoad).name
                    else:
                        raise NotImplementedError(
                            "Only DockerRequirement.dockerPull and DockerRequirement.dockerLoad currently supported"
                        )

                    try:
                        tc.add_containers(
                            Container(
                                container_name,
                                Container.DOCKER,
                                image,
                                image_site="local",
                            )
                        )
                    except DuplicateError:
                        pass

                    log.info(
                        "Added <Container name={}, container_type='docker', image={}, image_site='local'> from CommandLineTool: {}".format(
                            container_name, image, cwl_cmd_ln_tool.id
                        )
                    )
                    log.warning(
                        "Container types in the transformation catalog will need to be modified if containers are not of type: docker or if image file exists on a site other than 'local'"
                    )

                    #
                    break

        tr = Transformation(
            tool_path.name,
            site=site,
            pfn=str(tool_path),
            is_stageable=is_stageable,
            container=container_name,
        )
        log.debug(
            "tr = Transformation({}, site={}, pfn={}, is_stageable={})".format(
                tool_path.name, site, str(tool_path), is_stageable
            )
        )
        log.info("Adding <Transformation {}>".format(tr.name))

        try:
            tc.add_transformations(tr)
        except DuplicateError:
            log.warning(
                "<Transformation {}> is a duplicate and has already been added.".format(
                    tr.name
                )
            )

    log.info(
        "Building transformation catalog complete. {} transformations, {} containers added.".format(
            len(tc.transformations), len(tc.containers)
        )
    )

    return tc


def build_pegasus_rc(wf_inputs: dict, cwl_wf: cwl.Workflow) -> ReplicaCatalog:
    log.info("Building replica catalog")
    rc = ReplicaCatalog()

    for _input in cwl_wf.inputs:
        if _input.type == "File":
            input_name = get_basename(_input.id)

            try:
                current_wf_inputs = wf_inputs[input_name]
            except KeyError:
                log.exception(
                    "Unable to obtain input: {} from input spec file".format(input_name)
                )
                sys.exit(1)

            try:
                log.info(
                    "Adding replica: site={}, lfn={}, pfn={}".format(
                        "local", input_name, current_wf_inputs["path"]
                    )
                )
                # TODO: what about other sites?
                rc.add_replica("local", input_name, current_wf_inputs["path"])
            except KeyError:
                log.exception(
                    "Unable to obtain a path (pfn) for input file: {} from input spec file".format(
                        input_name
                    )
                )
                sys.exit(1)

    log.info(
        "Building replica catalog complete. {} entries added".format(len(rc.entries))
    )

    return rc


def collect_files(cwl_wf: cwl.Workflow) -> dict:
    log.info("Collecting workflow files")
    wf_files = dict()

    log.info("Parsing input files from workflow inputs")
    for _input in cwl_wf.inputs:
        if _input.type == "File":
            f = get_basename(_input.id)
            wf_files[f] = f
            log.info("Collected input file: {}".format(f))
            log.debug("wf_files[{0}] = {0}".format(f))

        elif (
            isinstance(_input.type, cwl.InputArraySchema)
            and _input.type.items == "File"
        ):

            raise NotImplementedError(
                "Support for File[] workflow input type in development"
            )

    log.info("Parsing output files from each workflow step")
    for step in cwl_wf.steps:
        cwl_cmd_ln_tool = (
            cwl.load_document(step.run) if isinstance(step.run, str) else step.run
        )

        for output in cwl_cmd_ln_tool.outputs:
            if output.type == "File":
                k = get_name(step.id, output.id)

                try:
                    v = output.outputBinding.glob
                except AttributeError:
                    v = None
                finally:
                    if not v:
                        raise ValueError(
                            "outputBinding.glob must be specified (e.g. file1.txt) for {}".format(
                                step.run
                            )
                        )

                if any(c in "*$" for c in v):
                    raise NotImplementedError(
                        "Unable to resolve wildcards in {}".format(v)
                    )

                wf_files[k] = v
                log.info("Collected output file: {}".format(k))
                log.debug("wf_files[{}] = {}".format(k, v))
            else:
                raise NotImplementedError(
                    "Support for output types other than File is in development"
                )

    log.info(
        "Collection of workflow files complete. {} files collected".format(
            len(wf_files)
        )
    )

    return wf_files


def collect_input_strings(wf_inputs: dict, cwl_wf: cwl.Workflow) -> dict:
    log.info("Collecting workflow input strings and string[] from input spec file")
    wf_input_str = dict()

    for _input in cwl_wf.inputs:
        if _input.type == "string" or (
            isinstance(_input.type, cwl.InputArraySchema)
            and _input.type.items == "string"
        ):
            input_name = get_basename(_input.id)

            try:
                wf_input_str[input_name] = wf_inputs[input_name]
                log.info("Collected input string: {}".format(input_name))
                log.debug(
                    "wf_input_str[{}] = {}".format(input_name, wf_inputs[input_name])
                )
            except KeyError:
                log.exception(
                    "Unable to obtain input: {} from input spec file".format(input_name)
                )
                sys.exit(1)

    log.info(
        "Collection of workflow input strings complete. {} string/string[] inputs collected".format(
            len(wf_input_str)
        )
    )

    return wf_input_str


def build_pegasus_wf(
    cwl_wf: cwl.Workflow, wf_files: dict, wf_input_str: dict
) -> Workflow:
    log.info("Building Pegasus workflow")

    wf = Workflow("cwl-converted-pegasus-workflow", infer_dependencies=True)

    for step in cwl_wf.steps:
        step_name = get_basename(step.id)
        log.info("Processing step: {}".format(step_name))
        cwl_cmd_ln_tool = (
            cwl.load_document(step.run) if isinstance(step.run, str) else step.run
        )

        job = Job(PurePath(cwl_cmd_ln_tool.baseCommand).name, _id=get_basename(step.id))

        # collect current step inputs
        log.info("Collecting step inputs from {}".format(step_name))
        step_inputs = dict()
        for _input in step.in_:
            input_id = get_basename(_input.id)

            step_inputs[input_id] = get_basename(_input.source)
            log.debug("step_inputs[{}] = {}".format(input_id, step_inputs[input_id]))

        # add inputs that are of type File
        for _input in cwl_cmd_ln_tool.inputs:
            if _input.type == "File":
                wf_file = File(wf_files[step_inputs[get_name(step.id, _input.id)]])

                job.add_inputs(wf_file)
                log.info("Step: {} added input file: {}".format(step_name, wf_file.lfn))
            """
            # TODO: handle File[] inputs
            elif isinstance(_input.type, cwl.CommandInputArraySchema):
                if _input.type.items == "File":
                    for f in step_inputs[get_name(step.id, _input.id)]:
                        wf_file = File(wf_files[f])

                        job.add_inputs(wf_file)
                        log.info(
                            "Step: {} added input file: {}".format(
                                step_name, wf_file.lfn
                            )
                        )
            """
        # add job outputs that are of type File
        log.info("Collecting step outputs from {}".format(step_name))
        for output in cwl_cmd_ln_tool.outputs:
            if output.type == "File":
                wf_file = File(wf_files[get_name(step.id, output.id)])

                job.add_outputs(wf_file)
                log.info(
                    "Step: {} added output file: {}".format(step_name, wf_file.lfn)
                )
            else:
                raise NotImplementedError(
                    "Support for output types other than File is in development"
                )

        # add job args
        args = (
            cwl_cmd_ln_tool.arguments
            if cwl_cmd_ln_tool.arguments is not None
            else list()
        )

        # args will be added in the order of their assigned inputBinding
        def get_input_binding(_input):
            key = 0
            if hasattr(_input, "inputBinding") and hasattr(
                _input.inputBinding, "position"
            ):
                key = _input.inputBinding.position

            return key if key else 0

        cwl_cmd_ln_tool_inputs = sorted(cwl_cmd_ln_tool.inputs, key=get_input_binding)

        for _input in cwl_cmd_ln_tool_inputs:
            # indicates whether or not input will appear in args
            if _input.inputBinding is not None:
                prefix = _input.inputBinding.prefix
                separate = _input.inputBinding.separate

                current_arg = ""
                if prefix:
                    current_arg += prefix

                if separate:
                    current_arg += " "

                if _input.type == "File":
                    current_arg += wf_files[step_inputs[get_name(step.id, _input.id)]]
                elif _input.type == "string":
                    current_arg += wf_input_str[
                        step_inputs[get_name(step.id, _input.id)]
                    ]

                # TODO: provide better support for array inputs being used in args (see https://www.commonwl.org/user_guide/09-array-inputs/index.html)
                elif isinstance(_input.type, cwl.CommandInputArraySchema):
                    separator = (
                        " "
                        if _input.inputBinding.itemSeparator is None
                        else _input.inputBinding.itemSeparator
                    )

                    if _input.type.items == "File":
                        current_arg += separator.join(
                            wf_files[f]
                            for f in step_inputs[get_name(step.id, _input.id)]
                        )
                    elif _input.type.items == "string":

                        current_arg += separator.join(
                            wf_input_str[step_inputs[get_name(step.id, _input.id)]]
                        )

                args.append(current_arg)

        job.add_args(*args)
        wf.add_jobs(job)

        log.info("Added job: {}".format(step.run))
        log.info("\tcmd: {}".format(job.transformation))
        log.info("\targs: {}".format(job.args))
        log.info("\tinputs: {}".format([f.lfn for f in job.get_inputs()]))
        log.info("\toutputs: {}".format([f.lfn for f in job.get_outputs()]))

    log.info("Building workflow complete. {} jobs added".format(len(wf.jobs)))

    return wf


def main():
    args = parse_args()
    setup_logger(args.debug)

    cwl_wf = cwl.load_document(args.cwl_workflow_file_path)
    log.info("Loaded cwl workflow: {}".format(args.cwl_workflow_file_path))

    wf_inputs = load_wf_inputs(args.workflow_inputs_file_path)
    tr_specs = load_tr_specs(args.transformation_spec_file_path)

    tc = build_pegasus_tc(tr_specs, cwl_wf)
    rc = build_pegasus_rc(wf_inputs, cwl_wf)

    wf_files = collect_files(cwl_wf)
    wf_input_strings = collect_input_strings(wf_inputs, cwl_wf)
    wf = build_pegasus_wf(cwl_wf, wf_files, wf_input_strings)

    wf.add_transformation_catalog(tc)
    wf.add_replica_catalog(rc)

    wf.write(file=args.output_file_path)
    log.info("Workflow written to {}".format(args.output_file_path))

    return 0


if __name__ == "__main__":
    sys.exit(main())
