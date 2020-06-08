import json
from collections import OrderedDict, defaultdict
from enum import Enum
from functools import wraps
from typing import Dict, List

from ._utils import _chained, _get_enum_str
from .errors import DuplicateError, NotFoundError, PegasusError
from .mixins import HookMixin, MetadataMixin, ProfileMixin
from .replica_catalog import File, ReplicaCatalog
from .site_catalog import SiteCatalog
from .transformation_catalog import Transformation, TransformationCatalog
from .writable import Writable, _CustomEncoder, _filter_out_nones

from Pegasus.client._client import from_env

PEGASUS_VERSION = "5.0"

__all__ = ["Job", "SubWorkflow", "Workflow"]


class AbstractJob(HookMixin, ProfileMixin, MetadataMixin):
    """An abstract representation of a workflow job"""

    def __init__(self, _id=None, node_label=None):
        """
        :param _id: a unique id, if None is given then one will be assigned when this job is added to a :py:class:`~Pegasus.api.workflow.Workflow`, defaults to None
        :type _id: str, optional
        :param node_label: a short descriptive label that can be assined to this job, defaults to None
        :type node_label: str, optional
        """
        self._id = _id
        self.node_label = node_label
        self.args = list()
        self.uses = set()

        self.stdout = None
        self.stderr = None
        self.stdin = None

        self.hooks = defaultdict(list)
        self.profiles = defaultdict(dict)
        self.metadata = dict()

    @_chained
    def add_inputs(self, *input_files):
        """Add one or more :py:class:`~Pegasus.api.replica_catalog.File`s as input to this job

        :param input_files: the :py:class:`~Pegasus.api.replica_catalog.File`s to be added as inputs to this job
        :raises DuplicateError: all input files must be unique
        :raises ValueError: job inputs must be of type :py:class:`~Pegasus.api.replica_catalog.File`
        :return: self
        """
        for file in input_files:
            if not isinstance(file, File):
                raise TypeError(
                    "invalid input_file: {file}; input_file(s) must be of type File".format(
                        file=file
                    )
                )

            _input = _Use(file, _LinkType.INPUT, register_replica=None, stage_out=None)
            if _input in self.uses:
                raise DuplicateError(
                    "file: {file} has already been added as input to this job".format(
                        file=file.lfn
                    )
                )

            self.uses.add(_input)

    def get_inputs(self):
        """Get this job's input files

        :return: all input files associated with this job
        :rtype: set
        """
        return {use.file for use in self.uses if use._type == "input"}

    @_chained
    def add_outputs(self, *output_files, stage_out=True, register_replica=True):
        """Add one or more :py:class:`~Pegasus.api.replica_catalog.File`s as outputs to this job. stage_out and register_replica
        will be applied to all files given.

        :param output_files: the :py:class:`~Pegasus.api.replica_catalog.File` s to be added as outputs to this job
        :param stage_out: whether or not to send files back to an output directory, defaults to True
        :type stage_out: bool, optional
        :param register_replica: whether or not to register replica with a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog`, defaults to True
        :type register_replica: bool, optional
        :raises DuplicateError: all output files must be unique
        :raises ValueError: a job output must be of type File
        :return: self
        """
        for file in output_files:
            if not isinstance(file, File):
                raise TypeError(
                    "invalid output_file: {file}; output_file(s) must be of type File".format(
                        file=file
                    )
                )

            output = _Use(
                file,
                _LinkType.OUTPUT,
                stage_out=stage_out,
                register_replica=register_replica,
            )
            if output in self.uses:
                raise DuplicateError(
                    "file: {file} already added as output to this job".format(
                        file=file.lfn
                    )
                )

            self.uses.add(output)

    def get_outputs(self):
        """Get this job's output files

        :return: all output files associated with this job
        :rtype: set
        """
        return {use.file for use in self.uses if use._type == "output"}

    @_chained
    def add_checkpoint(self, checkpoint_file, stage_out=True, register_replica=True):
        """Add an output file of this job as a checkpoint file

        :param checkpoint_file: the :py:class:`~Pegasus.api.replica_catalog.File` to be added as a checkpoint file to this job
        :type checkpoint_file: File
        :param stage_out: whether or not to send files back to an output directory, defaults to True
        :type stage_out: bool, optional
        :param register_replica: whether or not to register replica with a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog`, defaults to True
        :type register_replica: bool, optional
        :raises DuplicateError: all output files must be unique
        :raises ValueError: a job output must be of type File
        :return: self
        """

        if not isinstance(checkpoint_file, File):
            raise TypeError(
                "invalid checkpoint_file: {file}; checkpoint_file must be of type File".format(
                    file=checkpoint_file
                )
            )

        checkpoint = _Use(
            checkpoint_file,
            _LinkType.CHECKPOINT,
            stage_out=stage_out,
            register_replica=register_replica,
        )

        if checkpoint in self.uses:
            raise DuplicateError(
                "file: {file} already added as output to this job".format(
                    file=checkpoint_file.lfn
                )
            )

        self.uses.add(checkpoint)

    @_chained
    def add_args(self, *args):
        """Add arguments to this job. Each argument will be separated by a space.
        Each argument must be either a File, scalar, or str.

        :return: self
        :rtype: AbstractJob
        """
        self.args.extend(args)

    @_chained
    def set_stdin(self, file):
        """Set stdin to a :py:class:`~Pegasus.api.replica_catalog.File`

        :param file: a file that will be read into stdin
        :type file: File or str
        :raises ValueError: file must be of type :py:class:`~Pegasus.api.replica_catalog.File` or str
        :raises DuplicateError: stdin is already set or the given file has already been added as an input to this job
        :return: self
        """
        if not isinstance(file, (File, str)):
            raise TypeError(
                "invalid file: {file}; file must be of type File or str".format(
                    file=file
                )
            )

        if self.stdin is not None:
            raise DuplicateError("stdin has already been set to a file")

        if isinstance(file, str):
            file = File(file)

        self.add_inputs(file)
        self.stdin = file

    def get_stdin(self):
        """Get the :py:class:`~Pegasus.api.replica_catalog.File` being used for stdin

        :return: the stdin file
        :rtype: File
        """
        return self.stdin

    @_chained
    def set_stdout(self, file, stage_out=True, register_replica=True):
        """Set stdout to a :py:class:`~Pegasus.api.replica_catalog.File`

        :param file: a file that stdout will be written to
        :type file: File or str
        :param stage_out: whether or not to send files back to an output directory, defaults to True
        :type stage_out: bool, optional
        :param register_replica: whether or not to register replica with a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog`, defaults to True
        :type register_replica: bool, optional
        :raises ValueError: file must be of type :py:class:`~Pegasus.api.replica_catalog.File` or str
        :raises DuplicateError: stdout is already set or the given file has already been added as an output to this job
        :return: self
        """
        if not isinstance(file, (File, str)):
            raise TypeError(
                "invalid file: {file}; file must be of type File or str".format(
                    file=file
                )
            )

        if self.stdout is not None:
            raise DuplicateError("stdout has already been set to a file")

        if isinstance(file, str):
            file = File(file)

        self.add_outputs(file, stage_out=stage_out, register_replica=register_replica)
        self.stdout = file

    def get_stdout(self):
        """Get the :py:class:`~Pegasus.api.replica_catalog.File` being used for stdout

        :return: the stdout file
        :rtype: File
        """
        return self.stdout

    @_chained
    def set_stderr(self, file, stage_out=True, register_replica=True):
        """Set stderr to a :py:class:`~Pegasus.api.replica_catalog.File`

        :param file: a file that stderr will be written to
        :type file: File or str
        :param stage_out: whether or not to send files back to an output directory, defaults to True
        :type stage_out: bool, optional
        :param register_replica: whether or not to register replica with a :py:class:`~Pegasus.api.replica_catalog.ReplicaCatalog`, defaults to True
        :type register_replica: bool, optional
        :raises ValueError: file must be of type File or str
        :raises DuplicateError: stderr is already set or the given file has already been added as an output to this job
        :return: self
        """
        if not isinstance(file, (File, str)):
            raise TypeError(
                "invalid file: {file}; file must be of type File or str".format(
                    file=file
                )
            )

        if self.stderr is not None:
            raise DuplicateError("stderr has already been set to a file")

        if isinstance(file, str):
            file = File(file)

        self.add_outputs(file, stage_out=stage_out, register_replica=register_replica)
        self.stderr = file

    def get_stderr(self):
        """Get the :py:class:`~Pegasus.api.replica_catalog.File` being used for stderr

        :return: the stderr file
        :rtype: File
        """
        return self.stderr

    def __json__(self):
        return _filter_out_nones(
            {
                "id": self._id,
                "stdin": self.stdin.lfn if self.stdin is not None else None,
                "stdout": self.stdout.lfn if self.stdout is not None else None,
                "stderr": self.stderr.lfn if self.stderr is not None else None,
                "nodeLabel": self.node_label,
                "arguments": [
                    arg.lfn if isinstance(arg, File) else arg for arg in self.args
                ],
                "uses": [use for use in self.uses],
                "profiles": dict(self.profiles) if len(self.profiles) > 0 else None,
                "metadata": self.metadata if len(self.metadata) > 0 else None,
                "hooks": {
                    hook_name: [hook for hook in values]
                    for hook_name, values in self.hooks.items()
                }
                if len(self.hooks) > 0
                else None,
            }
        )


class Job(AbstractJob):
    """A typical workflow Job that executes a :py:class:`~Pegasus.api.transformation_catalog.Transformation`.

    .. code-block:: python

        # Example
        preprocess = (Transformation("preprocess")
                        .add_metadata("size", 2048)
                        .add_site("test-cluster", "/usr/bin/keg", False))

        if1 = File("if1")
        if2 = File("if2")

        of1 = File("of1")
        of2 = File("of2")

        job = (Job(preprocess)
                .add_args("-i", if1, if2, "-o", of1, of2)
                .add_inputs(if1, if2)
                .add_outputs(of1, of2, stage_out=True, register_replica=False))

    """

    def __init__(
        self, transformation, _id=None, node_label=None, namespace=None, version=None,
    ):
        """
        :param transformation: :py:class:`~Pegasus.api.transformation_catalog.Transformation` object or name of the transformation that this job uses
        :type transformation: Transformation or str
        :param _id: a unique id; if none is given then one will be assigned when the job is added by a :py:class:`~Pegasus.api.workflow.Workflow`, defaults to None
        :type _id: str, optional
        :param node_label: a brief job description, defaults to None
        :type node_label: str, optional
        :param namespace: namespace to which the :py:class:`~Pegasus.api.transformation_catalog.Transformation` belongs, defaults to None
        :type namespace: str, optional
        :param version: version of the given :py:class:`~Pegasus.api.transformation_catalog.Transformation`, defaults to None
        :type version: str, optional
        :raises ValueError: transformation must be one of :py:class:`~Pegasus.api.transformation_catalog.Transformation` or str
        """
        if isinstance(transformation, Transformation):
            self.transformation = transformation.name
            self.namespace = transformation.namespace
            self.version = transformation.version
        elif isinstance(transformation, str):
            self.transformation = transformation
            self.namespace = namespace
            self.version = version
        else:
            raise TypeError(
                "invalid transformation: {transformation}; transformation must be of type Transformation or str".format(
                    transformation=transformation
                )
            )

        AbstractJob.__init__(self, _id=_id, node_label=node_label)

    def __json__(self):
        job_json = {
            "type": "job",
            "namespace": self.namespace,
            "version": self.version,
            "name": self.transformation,
        }

        job_json.update(AbstractJob.__json__(self))

        return _filter_out_nones(job_json)


class SubWorkflow(AbstractJob):
    """Job that represents a subworkflow that will be executed as a job."""

    def __init__(self, file, is_planned, _id=None, node_label=None):
        """
        :param file: :py:class:`~Pegasus.api.replica_catalog.File` object or name of the dax file that will be used for this job
        :type file: File or str
        :param is_planned: whether or not this subworkflow has already been planned by the Pegasus planner
        :type is_planned: bool
        :param _id: a unique id; if none is given then one will be assigned when the job is added by a :py:class:`~Pegasus.api.workflow.Workflow`, defaults to None
        :type _id: str, optional
        :param node_label: a brief job description, defaults to None
        :type node_label: str, optional
        :raises ValueError: file must be of type :py:class:`~Pegasus.api.replica_catalog.File` or str
        """
        AbstractJob.__init__(self, _id=_id, node_label=node_label)

        if not isinstance(file, (File, str)):
            raise TypeError(
                "invalid file: {file}; file must be of type File or str".format(
                    file=file
                )
            )

        self.type = "condorWorkflow" if is_planned else "pegasusWorkflow"
        self.file = file if isinstance(file, File) else File(file)

        self.add_inputs(self.file)

    def __json__(self):
        dax_json = {"type": self.type, "file": self.file.lfn}
        dax_json.update(AbstractJob.__json__(self))

        return dax_json


class _LinkType(Enum):
    """Internal class defining link types"""

    INPUT = "input"
    OUTPUT = "output"
    CHECKPOINT = "checkpoint"


class _Use:
    """Internal class used to represent input and output files of a job"""

    def __init__(self, file, link_type, stage_out=True, register_replica=True):
        if not isinstance(file, File):
            raise TypeError(
                "invalid file: {file}; file must be of type File".format(file=file)
            )

        self.file = file

        if not isinstance(link_type, _LinkType):
            raise TypeError(
                "invalid link_type: {link_type}; link_type must one of {enum_str}".format(
                    link_type=link_type, enum_str=_get_enum_str(_LinkType)
                )
            )

        self._type = link_type.value

        self.stage_out = stage_out
        self.register_replica = register_replica

    def __hash__(self):
        return hash(self.file)

    def __eq__(self, other):
        if isinstance(other, _Use):
            return self.file.lfn == other.file.lfn
        raise ValueError("_Use cannot be compared with {}".format(type(other)))

    def __json__(self):
        return _filter_out_nones(
            {
                "lfn": self.file.lfn,
                "metadata": self.file.metadata if len(self.file.metadata) > 0 else None,
                "size": self.file.size,
                "type": self._type,
                "stageOut": self.stage_out,
                "registerReplica": self.register_replica,
            }
        )


class _JobDependency:
    """Internal class used to represent a jobs dependencies within a workflow"""

    def __init__(self, parent_id, children_ids):
        self.parent_id = parent_id
        self.children_ids = children_ids

    def __eq__(self, other):
        if isinstance(other, _JobDependency):
            return (
                self.parent_id == other.parent_id
                and self.children_ids == other.children_ids
            )
        raise ValueError(
            "_JobDependency cannot be compared with {}".format(type(other))
        )

    def __json__(self):
        return {"id": self.parent_id, "children": list(self.children_ids)}


def _needs_client(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if not self._client:
            self._client = from_env()

        f(self, *args, **kwargs)

    return wrapper


def _needs_submit_dir(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if not self._submit_dir:
            raise ValueError(
                "{f} requires a submit directory to be set; Workflow.plan() must be called prior to {f}".format(
                    f=f
                )
            )

        f(self, *args, **kwargs)

    return wrapper


class Workflow(Writable, HookMixin, ProfileMixin, MetadataMixin):
    """Represents multi-step computational steps as a directed
    acyclic graph.

    .. code-block:: python

        # Example
        from pathlib import Path
        from datetime import date

        from Pegasus.api import *

        PEGASUS_LOCATION = "file:///usr/bin/pegasus-keg"

        RUN_ID = "001-black-diamond-vanilla-condor-5.0-api-" + date.today().strftime("%s")

        TOP_DIR = Path(Path.cwd())
        WORK_DIR = TOP_DIR / "work"
        Path.mkdir(WORK_DIR)

        # --- Sites --------------------------------------------------------------------
        LOCAL = "local"
        CONDOR_POOL = "condor-pool"

        shared_scratch_dir = str(WORK_DIR / RUN_ID)
        local_storage_dir = str(WORK_DIR / "outputs" / RUN_ID)

        sc = (SiteCatalog()
                .add_site(
                    Site(LOCAL, arch=Arch.X86_64, os_type=OS.LINUX, os_release="rhel", os_version="7")
                        .add_directory(
                            Directory(Directory.SHARED_SCRATCH, shared_scratch_dir)
                                .add_file_server(FileServer("file://" + shared_scratch_dir, Operation.ALL))
                        ).add_directory(
                            Directory(Directory.LOCAL_STORAGE, local_storage_dir)
                                .add_file_server(FileServer("file://" + local_storage_dir, Operation.ALL))
                        )
                ).add_site(
                    Site(CONDOR_POOL, arch=Arch.X86_64, os_type=OS.LINUX)
                        .add_pegasus_profile(style="condor")
                        .add_condor_profile(universe="vanilla")
                ))

        # --- Replicas -----------------------------------------------------------------
        # create initial input file
        with open("f.a", "w") as f:
            f.write("This is sample input to KEG")

        fa = File("f.a")
        rc = (ReplicaCatalog()
            .add_replica(LOCAL, fa, "file://" + str(TOP_DIR / fa.lfn)))

        # --- Transformations ----------------------------------------------------------
        preprocess = (Transformation("preprocess", namespace="pegasus", version="4.0")
                        .add_site(
                            TransformationSite(
                                CONDOR_POOL,
                                PEGASUS_LOCATION,
                                is_stageable=False,
                                arch=Arch.X86_64,
                                os_type=OS.LINUX)
                        ))

        findrage = (Transformation("findrange", namespace="pegasus", version="4.0")
                        .add_site(
                            TransformationSite(
                                CONDOR_POOL,
                                PEGASUS_LOCATION,
                                is_stageable=False,
                                arch=Arch.X86_64,
                                os_type=OS.LINUX)
                        ))

        analyze = (Transformation("analyze", namespace="pegasus", version="4.0")
                        .add_site(
                            TransformationSite(
                                CONDOR_POOL,
                                PEGASUS_LOCATION,
                                is_stageable=False,
                                arch=Arch.X86_64,
                                os_type=OS.LINUX)
                        ))

        tc = (TransformationCatalog()
                .add_transformations(preprocess, findrage, analyze))

        # --- Workflow -----------------------------------------------------------------
        fb1 = File("f.b1")
        fb2 = File("f.b2")
        fc1 = File("f.c1")
        fc2 = File("f.c2")
        fd = File("f.d")

        (Workflow("blackdiamond")
            .add_jobs(
                Job(preprocess)
                    .add_args("-a", "preprocess", "-T", "60", "-i", fa, "-o", fb1, fb2)
                    .add_inputs(fa)
                    .add_outputs(fb1, fb2),

                Job(findrage)
                    .add_args("-a", "findrange", "-T", "60", "-i", fb1, "-o", fc1)
                    .add_inputs(fb1)
                    .add_outputs(fc1),

                Job(findrage)
                    .add_args("-a", "findrange", "-T", "60", "-i", fb2, "-o", fc2)
                    .add_inputs(fb2)
                    .add_outputs(fc2),

                Job(analyze)
                    .add_args("-a", "analyze", "-T", "60", "-i", fc1, fc2, "-o", fd)
                    .add_inputs(fc1, fc2)
                    .add_outputs(fd)
            ).add_site_catalog(sc)
            .add_replica_catalog(rc)
            .add_transformation_catalog(tc)
            .write())

    """

    _DEFAULT_FILENAME = "workflow.yml"

    def __init__(self, name, infer_dependencies=True):
        """
        :param name: name of the :py:class:`~Pegasus.api.workflow.Workflow`
        :type name: str
        :param infer_dependencies: whether or not to automatically compute job dependencies based on input and output files used by each job, defaults to True
        :type infer_dependencies: bool, optional
        """
        self.name = name
        self.infer_dependencies = infer_dependencies

        # client specific members
        self._submit_dir = None
        self._client = None

        self._path = None

        # sequence unique to this workflow only
        self.sequence = 1

        self.jobs = dict()
        self.dependencies = defaultdict(_JobDependency)

        self.site_catalog = None
        self.transformation_catalog = None
        self.replica_catalog = None

        self.hooks = defaultdict(list)
        self.profiles = defaultdict(dict)
        self.metadata = dict()

    @_chained
    @_needs_client
    def plan(
        self,
        conf: str = None,
        sites: List[str] = None,
        output_sites: List[str] = ["local"],
        staging_sites: Dict[str, str] = None,
        input_dirs: List[str] = None,
        output_dir: str = None,
        dir: str = None,
        relative_dir: str = None,
        cleanup: str = "none",
        verbose: int = 0,
        force: bool = False,
        submit: bool = False,
        **kwargs
    ):
        """Plan the workflow.

        :param conf:  the path to the properties file to use for planning, defaults to None
        :type conf: str, optional
        :param sites: list of execution sites on which to map the workflow, defaults to None
        :type sites: List[str], optional
        :param output_sites: the output sites where the data products during workflow execution are transferred to, defaults to ["local"]
        :type output_sites: List[str], optional
        :param staging_sites: key, value pairs of execution site to staging site mappings, defaults to None
        :type staging_sites: Dict[str,str], optional
        :param input_dirs: comma separated list of optional input directories where the input files reside on submit host, defaults to None
        :type input_dirs: List[str], optional
        :param output_dir: an optional output directory where the output files should be transferred to on submit host, defaults to None
        :type output_dir: str, optional
        :param dir: the directory where to generate the executable workflow, defaults to None
        :type dir: str, optional
        :param relative_dir: the relative directory to the base directory where to generate the concrete workflow, defaults to None
        :type relative_dir: str, optional
        :param cleanup: the cleanup strategy to use. Can be none|inplace|leaf|constraint, defaults to inplace
        :type cleanup: str, optional
        :param verbose: verbosity, defaults to False
        :type verbose: int, optional
        :param force: skip reduction of the workflow, resulting in build style dag, defaults to False
        :type force: bool, optional
        :param submit: submit the executable workflow generated, defaults to False
        :type submit: bool, optional
        """
        # if the workflow has not yet been written to a file and plan is
        # called, write the file to default
        if not self._path:
            # self._path is set by write
            self.write()

        self._submit_dir = self._client.plan(
            self._path,
            conf=conf,
            sites=sites,
            output_sites=output_sites,
            staging_sites=staging_sites,
            input_dirs=input_dirs,
            output_dir=output_dir,
            dir=dir,
            relative_dir=relative_dir,
            cleanup=cleanup,
            verbose=verbose,
            force=force,
            submit=submit,
            **kwargs,
        )._submit_dir

    @_chained
    @_needs_client
    def run(self, verbose: int = 0):
        """Run the planned workflow.

        :param verbose: verbosity, defaults to False
        :type verbose: int, optional
        """
        self._client.run(self._submit_dir, verbose=verbose)

    @_chained
    @_needs_submit_dir
    @_needs_client
    def status(self, long: bool = False, verbose: int = 0):
        """Monitor the workflow by quering Condor and directories.

        :param long: Show all DAG states, including sub-DAGs, default only totals. defaults to False
        :type long: bool, optional
        :param verbose:  verbosity, defaults to False
        :type verbose: int, optional
        """

        self._client.status(self._submit_dir, long=long, verbose=verbose)

    @_chained
    @_needs_submit_dir
    @_needs_client
    def wait(self, delay: int = 2):
        """Displays progress bar to stdout and blocks until the workflow either
        completes or fails.

        :param delay: refresh rate in seconds of the progress bar, defaults to 2
        :type delay: int, optional
        """

        self._client.wait(self._submit_dir, delay=delay)

    @_chained
    @_needs_submit_dir
    @_needs_client
    def remove(self, verbose: int = 0):
        """Removes this workflow that has been planned and submitted.

        :param verbose:  verbosity, defaults to False
        :type verbose: int, optional
        """
        self._client.remove(self._submit_dir, verbose=verbose)

    @_chained
    @_needs_submit_dir
    @_needs_client
    def analyze(self, verbose: int = 0):
        """Debug a workflow.

        :param verbose:  verbosity, defaults to False
        :type verbose: int, optional
        """
        self._client.analyzer(self._submit_dir, verbose=verbose)

    # should wait until wf is done or else we will just get msg:
    # pegasus-monitord still running. Please wait for it to complete.
    @_chained
    @_needs_submit_dir
    @_needs_client
    def statistics(self, verbose: int = 0):
        """Generate statistics about the workflow run.

        :param verbose:  verbosity, defaults to False
        :type verbose: int, optional
        """
        self._client.statistics(self._submit_dir, verbose=verbose)

    @_chained
    def add_jobs(self, *jobs):
        """Add one or more jobs at a time to the Workflow

        :raises DuplicateError: a job with the same id already exists in this workflow
        :return: self
        """
        for job in jobs:
            if job._id is None:
                job._id = self._get_next_job_id()

            if job._id in self.jobs:
                raise DuplicateError(
                    "Job with id {} already added to this workflow".format(job._id)
                )

            self.jobs[job._id] = job

    def get_job(self, _id):
        """Retrieve the job with the given id

        :param _id: id of the job to be retrieved from the Workflow
        :type _id: str
        :raises NotFoundError: a job with the given id does not exist in this workflow
        :return: the job with the given id
        :rtype: Job
        """
        if _id not in self.jobs:
            raise NotFoundError(
                "job with _id={} not found in this workflow".format(_id)
            )

        return self.jobs[_id]

    def _get_next_job_id(self):
        """Get the next job id from a sequence specific to this workflow

        :return: a new unique job id
        :rtype: str
        """
        next_id = None
        while not next_id or next_id in self.jobs:
            next_id = "ID{:07d}".format(self.sequence)
            self.sequence += 1

        return next_id

    @_chained
    def add_site_catalog(self, sc):
        """Add a site catalog to this workflow. The contents fo the site catalog
        will be inlined into the same file as this workflow when it is written
        out.

        :param sc: the SiteCatalog to be added
        :type sc: SiteCatalog
        :raises TypeError: sc must be of type SiteCatalog
        :raises DuplicateError: a SiteCatalog has already been added
        """
        if not isinstance(sc, SiteCatalog):
            raise TypeError(
                "invalid catalog: {}; sc must be of type SiteCatalog".format(sc)
            )

        if self.site_catalog is not None:
            raise DuplicateError(
                "a SiteCatalog has already been added to this workflow"
            )

        self.site_catalog = sc

    @_chained
    def add_replica_catalog(self, rc):
        """Add a replica catalog to this workflow. The contents fo the replica catalog
        will be inlined into the same file as this workflow when it is written
        out.

        :param rc: the ReplicaCatalog to be added
        :type rc: ReplicaCatalog
        :raises TypeError: rc must be of type ReplicaCatalog
        :raises DuplicateError: a ReplicaCatalog has already been added
        """
        if not isinstance(rc, ReplicaCatalog):
            raise TypeError(
                "invalid catalog: {}; rc must be of type ReplicaCatalog".format(rc)
            )

        if self.replica_catalog is not None:
            raise DuplicateError(
                "a ReplicaCatalog has already been added to this workflow"
            )

        self.replica_catalog = rc

    @_chained
    def add_transformation_catalog(self, tc):
        """Add a transformation catalog to this workflow. The contents fo the transformation
        catalog will be inlined into the same file as this workflow when it is written
        out.

        :param tc: the TransformationCatalog to be added
        :type tc: TransformationCatalog
        :raises TypeError: tc must be of type TransformationCatalog
        :raises DuplicateError: a TransformationCatalog has already been added
        """
        if not isinstance(tc, TransformationCatalog):
            raise TypeError(
                "invalid catalog: {}; rc must be of type TransformationCatalog".format(
                    tc
                )
            )

        if self.transformation_catalog is not None:
            raise DuplicateError(
                "a TransformationCatalog has already been added to this workflow"
            )

        self.transformation_catalog = tc

    @_chained
    def add_dependency(self, job, *, parents=[], children=[]):
        """Add parent, child dependencies for a given job. 
        
        .. code-block::python

            # Example 1: set parents of a given job
            wf.add_dependency(job3, parents=[job1, job2])

            # Example 2: set children of a given job
            wf.add_dependency(job1, children=[job2, job3])

            # Example 2 equivalent:
            wf.add_dependency(job1, children=[job2])
            wf.add_dependency(job1, children=[job3])

            # Example 3: set parents and children of a given job
            wf.add_dependency(job3, parents=[job1, job2], children=[job4, job5])


        :param job: the job to which parents and children will be assigned
        :type job: AbstractJob
        :param parents: jobs to be added as parents to this job, defaults to []
        :type parents: list, optional
        :param children: jobs to be added as children of this job, defaults to []
        :type children: list, optional
        :raises ValueError: the given job(s) do not have ids assigned to them
        :raises DuplicateError: a dependency between two jobs already has been added
        """
        # ensure that job, parents, and children are all valid and have ids
        if job._id is None:
            raise ValueError(
                "The given job does not have an id. Either assign one to it upon creation or add the job to this workflow before manually adding its dependencies."
            )

        for parent in parents:
            if parent._id is None:
                raise ValueError(
                    "One of the given parents does not have an id. Either assign one to it upon creation or add the parent job to this workflow before manually adding its dependencies."
                )

        for child in children:
            if child._id is None:
                raise ValueError(
                    "One of the given children does not have an id. Either assign one to it upon creation or add the child job to this workflow before manually adding its dependencies."
                )

        # for each parent, add job as a child
        for parent in parents:
            if parent._id not in self.dependencies:
                self.dependencies[parent._id] = _JobDependency(parent._id, {job._id})
            else:
                if job._id in self.dependencies[parent._id].children_ids:
                    raise DuplicateError(
                        "A dependency already exists between parent id: {} and job id: {}".format(
                            parent._id, job._id
                        )
                    )

                self.dependencies[parent._id].children_ids.add(job._id)

        # for each child, add job as a parent
        if len(children) > 0:
            if job._id not in self.dependencies:
                self.dependencies[job._id] = _JobDependency(job._id, set())

            for child in children:
                if child._id in self.dependencies[job._id].children_ids:
                    raise DuplicateError(
                        "A dependency already exists between job id: {} and child id: {}".format(
                            job._id, child._id
                        )
                    )
                else:
                    self.dependencies[job._id].children_ids.add(child._id)

    def _infer_dependencies(self):
        """Internal function for automatically computing dependencies based on
        Job input and output files. This is called when Workflow.infer_dependencies is
        set to True.
        """

        if self.infer_dependencies:
            mapping = dict()

            """
            create a mapping:
            {
                <filename>: (set(), set())
            }

            where mapping[filename][0] are jobs that use this file as input
            and mapping[filename][1] are jobs that use this file as output
            """
            for _id, job in self.jobs.items():
                if job.stdin:
                    if job.stdin.lfn not in mapping:
                        mapping[job.stdin.lfn] = (set(), set())

                    mapping[job.stdin.lfn][0].add(job)

                if job.stdout:
                    if job.stdout.lfn not in mapping:
                        mapping[job.stdout.lfn] = (set(), set())

                    mapping[job.stdout.lfn][1].add(job)

                if job.stderr:
                    if job.stderr.lfn not in mapping:
                        mapping[job.stderr.lfn] = (set(), set())

                    mapping[job.stderr.lfn][1].add(job)

                """
                for _input in job.inputs:
                    if _input.file.lfn not in mapping:
                        mapping[_input.file.lfn] = (set(), set())

                    mapping[_input.file.lfn][0].add(job)

                for output in job.outputs:
                    if output.file.lfn not in mapping:
                        mapping[output.file.lfn] = (set(), set())

                    mapping[output.file.lfn][1].add(job)
                """
                for io in job.uses:
                    if io.file.lfn not in mapping:
                        mapping[io.file.lfn] = (set(), set())

                    if io._type == _LinkType.INPUT.value:
                        mapping[io.file.lfn][0].add(job)
                    elif io._type == _LinkType.OUTPUT.value:
                        mapping[io.file.lfn][1].add(job)

            """
            Go through the mapping and for each file add dependencies between the
            job producing a file and the jobs consuming the file
            """
            for _, io in mapping.items():
                inputs = io[0]

                if len(io[1]) > 0:
                    # only a single job should produce this file
                    output = io[1].pop()

                    for _input in inputs:
                        try:
                            self.add_dependency(output, children=[_input])
                        except DuplicateError:
                            pass

    @_chained
    def write(self, file=None, _format="yml"):
        """Write this workflow to a file. If no file is given,
        it will written to workflow.yml

        :param file: path or file object (opened in "w" mode) to write to, defaults to None
        :type file: str or file, optional
        :raises PegasusError: Site Catalog and Transformation Catalog must be written as a separate file for hierarchical workflows.
        """

        # if subworkflow jobs exist,  tc and sc cannot be inlined
        has_subworkflow_jobs = False
        for _, job in self.jobs.items():
            if isinstance(job, SubWorkflow):
                has_subworkflow_jobs = True
                break

        if has_subworkflow_jobs:
            if self.site_catalog or self.transformation_catalog:
                raise PegasusError(
                    "Site Catalog and Transformation Catalog must be written as a separate file for hierarchical workflows."
                )

        # default file name
        if file is None:
            file = self._DEFAULT_FILENAME

        self._infer_dependencies()
        Writable.write(self, file, _format=_format)

        # save path so that it can be used by Client.plan()
        if isinstance(file, str):
            self._path = file
        elif hasattr(file, "read"):
            self._path = file.name if hasattr(file, "name") else None

    def __json__(self):
        # remove 'pegasus' from tc, rc, sc as it is not needed when they
        # are included in the Workflow which already contains 'pegasus'
        rc = None
        if self.replica_catalog is not None:
            rc = json.loads(json.dumps(self.replica_catalog, cls=_CustomEncoder))
            del rc["pegasus"]

        tc = None
        if self.transformation_catalog is not None:
            tc = json.loads(json.dumps(self.transformation_catalog, cls=_CustomEncoder))
            del tc["pegasus"]

        sc = None
        if self.site_catalog is not None:
            sc = json.loads(json.dumps(self.site_catalog, cls=_CustomEncoder))
            del sc["pegasus"]

        hooks = None
        if len(self.hooks) > 0:
            hooks = {
                hook_name: [hook for hook in values]
                for hook_name, values in self.hooks.items()
            }

        profiles = None
        if len(self.profiles) > 0:
            profiles = dict(self.profiles)

        metadata = None
        if len(self.metadata) > 0:
            metadata = self.metadata

        return _filter_out_nones(
            OrderedDict(
                [
                    ("pegasus", PEGASUS_VERSION),
                    ("name", self.name),
                    ("hooks", hooks),
                    ("profiles", profiles),
                    ("metadata", metadata),
                    ("siteCatalog", sc),
                    ("replicaCatalog", rc),
                    ("transformationCatalog", tc),
                    ("jobs", [job for _id, job in self.jobs.items()]),
                    (
                        "jobDependencies",
                        [dependency for _id, dependency in self.dependencies.items()],
                    ),
                ]
            )
        )
