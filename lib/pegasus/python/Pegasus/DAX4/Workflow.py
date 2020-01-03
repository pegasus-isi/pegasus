import json
from collections import defaultdict

import yaml

from .Writable import filter_out_nones, FileFormat, Writable
from .Errors import DuplicateError, NotFoundError
from .TransformationCatalog import Transformation, TransformationCatalog
from .ReplicaCatalog import ReplicaCatalog, File
from .SiteCatalog import SiteCatalog
from .Mixins import MetadataMixin, HookMixin, ProfileMixin

PEGASUS_VERSION = "5.0"


class AbstractJob(HookMixin, ProfileMixin, MetadataMixin):
    """An abstract representation of a workflow job"""

    def __init__(self, _id=None, node_label=None):
        """Constructor
        
        :param _id: a unique id, if None is given then one will be assigned when this job is added to a Workflow, defaults to None
        :type _id: str, optional
        :param node_label: a short descriptive label that can be assined to this job, defaults to None
        :type node_label: str, optional
        """
        self._id = _id
        self.node_label = node_label
        self.args = list()
        self.inputs = set()
        self.outputs = set()

        self.stdout = None
        self.stderr = None
        self.stdin = None

        self.hooks = defaultdict(list)
        self.profiles = defaultdict(dict)
        self.metadata = dict()

    def add_inputs(self, *input_files):
        """Add one or more Files as input to this job
        
        :raises DuplicateError: all input files must be unique
        :return: self
        :rtype: AbstractJob
        """
        for file in input_files:
            _input = JobInput(file)
            if _input in self.inputs:
                raise DuplicateError(
                    "file {0} already added as input to this job".format(file.lfn)
                )

            self.inputs.add(_input)

        return self

    def get_inputs(self):
        """Get this job's input files
        
        :return: all input files associated with this job
        :rtype: set
        """
        return {_input.file for _input in self.inputs}

    def add_outputs(self, *output_files, stage_out=True, register_replica=False):
        """Add one or more Files as outputs to this job. stage_out and register_replica
        will be applied to all files given.
        
        :param stage_out: whether or not to send files back to an output directory, defaults to True
        :type stage_out: bool, optional
        :param register_replica: whether or not to register replica with a replica catalog, defaults to False
        :type register_replica: bool, optional
        :raises DuplicateError: all output files must be unique 
        :return: self
        :rtype: AbstractJob
        """
        for file in output_files:
            output = JobOutput(
                file, stage_out=stage_out, register_replica=register_replica
            )
            if output in self.outputs:
                raise DuplicateError(
                    "file {0} already added as output to this job".format(file.lfn)
                )

            self.outputs.add(output)

        return self

    def get_outputs(self):
        """Get this job's output files
        
        :return: all output files associated with this job 
        :rtype: set
        """
        return {output.file for output in self.outputs}

    def add_args(self, *args):
        """Add arguments to this job. Each argument will be separated by a space.
        Each argument must be either a File or a primitive type. 
        
        :return: self
        :rtype: AbstractJob
        """
        self.args.extend(args)

        return self

    def set_stdin(self, file):
        """Set stdin to a file
        
        :param file: a file that will be read into stdin  
        :type file: File|str
        :raises ValueError: file must be of type File or str
        :raises DuplicateError: stdin is already set or the given file has already been added as an input to this job
        :return: self
        :rtype: AbstractJob
        """
        if not isinstance(file, File) and not isinstance(file, str):
            raise ValueError("file must be of type File or str")

        if self.stdin is not None:
            raise DuplicateError("stdin has already been set to a file")

        if isinstance(file, str):
            file = File(file)

        self.add_inputs(file)
        self.stdin = file

        return self

    def get_stdin(self):
        """Get the file being used for stdin
        
        :return: the stdin file
        :rtype: File
        """
        return self.stdin

    def clear_stdin(self):
        """Clear stdin and remove it from the list of inputs 
        
        :return: self
        :rtype: AbstractJob
        """
        if self.stdin != None:
            # stdin file was added as an input, therefore it must be
            # removed from inputs when stdin is cleared
            for _input in self.inputs:
                if _input.file == self.stdin:
                    self.inputs.remove(_input)
                    break

        self.stdin = None

        return self

    def set_stdout(self, file):
        """Set stdout to a file
        
        :param file: a file that stdout will be written to
        :type file: File|str
        :raises ValueError: file must be of type File or str
        :raises DuplicateError: stdout is already set or the given file has already been added as an output to this job 
        :return: self
        :rtype: AbstractJob
        """
        if not isinstance(file, File) and not isinstance(file, str):
            raise ValueError("file must be of type File or str")

        if self.stdout is not None:
            raise DuplicateError("stdout has already been set to a file")

        if isinstance(file, str):
            file = File(file)

        self.add_outputs(file)
        self.stdout = file

        return self

    def get_stdout(self):
        """Get the file being used for stdout
        
        :return: the stdout file
        :rtype: File
        """
        return self.stdout

    def clear_stdout(self):
        """Clear stdout and remove it from the list of outputs
        
        :return: self
        :rtype: AbstractJob
        """
        if self.stdout != None:
            # stdout file was added as an output, therefore it
            # must be removed from outputs when stdout is cleared
            for output in self.outputs:
                if output.file == self.stdout:
                    self.outputs.remove(output)
                    break

        self.stdout = None

        return self

    def set_stderr(self, file):
        """Set stderr to a file 
        
        :param file: a file that stderr will be written to
        :type file: File|str
        :raises ValueError: file must be of type File or str
        :raises DuplicateError: stderr is already set or the given file has already been added as an output to this job 
        :return: self
        :rtype: AbstractJob
        """
        if not isinstance(file, File) and not isinstance(file, str):
            raise ValueError("file must be of type File or str")

        if self.stderr is not None:
            raise DuplicateError("stderr has already been set to a file")

        if isinstance(file, str):
            file = File(file)

        self.add_outputs(file)
        self.stderr = file

        return self

    def get_stderr(self):
        """Get the file being used for stderr
        
        :return: the stderr file 
        :rtype: File
        """
        return self.stderr

    def clear_stderr(self):
        """Clear stderr and remove it from the list of outputs
        
        :return: self
        :rtype: AbstractJob
        """
        if self.stderr != None:
            # stderr file was added as an output, therefore it
            # must be removed from outputs when stderr is cleared
            for output in self.outputs:
                if output.file == self.stderr:
                    self.outputs.remove(output)
                    break

        self.stderr = None

        return self

    def __json__(self):
        raise NotImplementedError()


class Job(AbstractJob):
    def __init__(
        self, transformation, _id=None, node_label=None, namespace=None, version=None,
    ):
        if isinstance(transformation, Transformation):
            self.transformation = transformation.name
        elif isinstance(transformation, str):
            self.transformation = transformation
        else:
            raise ValueError("transformation must be of type Transformation or str")

        self.namespace = namespace
        self.version = version

        AbstractJob.__init__(self, _id=_id, node_label=node_label)

    def __json__(self):
        return filter_out_nones(
            {
                "namespace": self.namespace,
                "version": self.version,
                "name": self.transformation,
                "id": self._id,
                "stdin": self.stdin.__json__() if self.stdin is not None else None,
                "stdout": self.stdout.__json__() if self.stdout is not None else None,
                "stderr": self.stderr.__json__() if self.stderr is not None else None,
                "nodeLabel": self.node_label,
                "arguments": [
                    {"lfn": arg.lfn} if isinstance(arg, File) else arg
                    for arg in self.args
                ],
                "uses": [_input.__json__() for _input in self.inputs]
                + [output.__json__() for output in self.outputs],
                "profiles": dict(self.profiles) if len(self.profiles) > 0 else None,
                "metadata": self.metadata if len(self.metadata) > 0 else None,
                "hooks": dict(self.hooks) if len(self.hooks) > 0 else None,
            }
        )


class JobInput:
    def __init__(self, file):
        self.file = file
        self._type = "input"

    def __hash__(self):
        return hash(self.file)

    def __eq__(self, other):
        if isinstance(other, JobInput):
            return self.file.lfn == other.file.lfn
        raise ValueError("JobInput cannot be compared with {0}".format(type(other)))

    def __json__(self):
        return {"file": self.file.__json__(), "type": self._type}


class JobOutput:
    def __init__(self, file, stage_out=True, register_replica=False):
        self.file = file
        self._type = "output"
        self.stage_out = stage_out
        self.register_replica = register_replica

    def __hash__(self):
        return hash(self.file)

    def __eq__(self, other):
        if isinstance(other, JobOutput):
            return self.file.lfn == other.file.lfn
        raise ValueError("JobOutput cannot be comapred with {0}".format(type(other)))

    def __json__(self):
        return {
            "file": self.file.__json__(),
            "type": self._type,
            "stageOut": self.stage_out,
            "registerReplica": self.register_replica,
        }


class JobDependency:
    def __init__(self, parent_id, children_ids):
        self.parent_id = parent_id
        self.children_ids = children_ids

    def __json__(self):
        return {"id": self.parent_id, "children": list(self.children_ids)}


class DAX(AbstractJob):
    pass


class DAG(AbstractJob):
    pass


class Workflow(Writable, HookMixin, ProfileMixin, MetadataMixin):
    def __init__(self, name, infer_dependencies=False):
        self.name = name
        self.infer_dependencies = infer_dependencies

        self.jobs = dict()
        self.dependencies = defaultdict(JobDependency)

        # sequence unique to this workflow only
        self.sequence = 1

        self.site_catalog = None
        self.transformation_catalog = None
        self.replica_catalog = None

        self.hooks = defaultdict(list)
        self.profiles = defaultdict(dict)
        self.metadata = dict()

    def add_jobs(self, *jobs):
        for job in jobs:
            if job._id == None:
                job._id = self.get_next_job_id()

            if job._id in self.jobs:
                raise DuplicateError("Job with id {0} already exists".format(job._id))

            self.jobs[job._id] = job

        return self

    def get_job(self, _id):
        if _id not in self.jobs:
            raise NotFoundError

        return self.jobs[_id]

    def get_next_job_id(self):
        next_id = None
        while not next_id or next_id in self.jobs:
            next_id = "ID{:07d}".format(self.sequence)
            self.sequence += 1

        return next_id

    def include_catalog(self, catalog):
        if isinstance(catalog, ReplicaCatalog):
            if self.replica_catalog is not None:
                raise ValueError(
                    "a ReplicaCatalog has already been inlined in this Workflow"
                )

            self.replica_catalog = catalog

        elif isinstance(catalog, TransformationCatalog):
            if self.transformation_catalog is not None:
                raise ValueError(
                    "a TransformationCatalog has already been inlined in this Workflow"
                )

            self.transformation_catalog = catalog

        elif isinstance(catalog, SiteCatalog):
            if self.site_catalog is not None:
                raise ValueError(
                    "a SiteCatalog has already been inlined in this Workflow"
                )

            self.site_catalog = catalog

        else:
            raise ValueError("{0} cannot be included in this Workflow".format(catalog))

        return self

    def add_dependency(self, parent, *children):
        children_ids = {child._id for child in children}
        parent_id = parent._id
        if parent_id in self.dependencies:
            if not self.dependencies[parent_id].children_ids.isdisjoint(children_ids):
                raise DuplicateError(
                    "A dependency already exists between parentid: {0} and children_ids: {1}".format(
                        parent_id, children_ids
                    )
                )

            self.dependencies[parent_id].children_ids.update(children_ids)
        else:
            self.dependencies[parent_id] = JobDependency(parent_id, children_ids)

        return self

    def _infer_dependencies(self):
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
                        mapping[job.stderr.lfn][1].add(job)

                for _input in job.inputs:
                    if _input.file.lfn not in mapping:
                        mapping[_input.file.lfn] = (set(), set())

                    mapping[_input.file.lfn][0].add(job)

                for output in job.outputs:
                    if output.file.lfn not in mapping:
                        mapping[output.file.lfn] = (set(), set())

                    mapping[output.file.lfn][1].add(job)

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
                            self.add_dependency(output, _input)
                        except DuplicateError:
                            pass

    def write(self, non_default_filepath="", file_format=FileFormat.YAML):
        """Write this catalog, formatted in YAML, to a file
        
        :param filepath: path to which this catalog will be written, defaults to self.filepath if filepath is "" or None
        :type filepath: str, optional
        """
        self._infer_dependencies()
        Writable.write(
            self, non_default_filepath=non_default_filepath, file_format=file_format
        )

    def __json__(self):
        # remove 'pegasus' from tc, rc, sc as it is not needed when they
        # are included in the Workflow which already contains 'pegasus'
        rc = None
        if self.replica_catalog is not None:
            rc = self.replica_catalog.__json__()
            del rc["pegasus"]

        tc = None
        if self.transformation_catalog is not None:
            tc = self.transformation_catalog.__json__()
            del tc["pegasus"]

        sc = None
        if self.site_catalog is not None:
            sc = self.site_catalog.__json__()
            del sc["pegasus"]

        return filter_out_nones(
            {
                "pegasus": PEGASUS_VERSION,
                "name": self.name,
                "replicaCatalog": rc,
                "transformationCatalog": tc,
                "siteCatalog": sc,
                "jobs": [job.__json__() for _id, job in self.jobs.items()],
                "jobDependencies": [
                    dependency.__json__()
                    for _id, dependency in self.dependencies.items()
                ]
                if len(self.dependencies) > 0
                else None,
                "profiles": dict(self.profiles) if len(self.profiles) > 0 else None,
                "metadata": self.metadata if len(self.metadata) > 0 else None,
                "hooks": dict(self.hooks) if len(self.hooks) > 0 else None,
            }
        )
