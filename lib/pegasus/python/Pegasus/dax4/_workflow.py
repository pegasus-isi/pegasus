import json
from collections import defaultdict

import yaml

from ._writable import _filter_out_nones
from ._writable import FileFormat
from ._writable import Writable
from ._errors import DuplicateError
from ._errors import NotFoundError
from ._transformation_catalog import Transformation
from ._transformation_catalog import TransformationCatalog
from ._replica_catalog import ReplicaCatalog
from ._replica_catalog import File
from ._site_catalog import SiteCatalog
from ._mixins import MetadataMixin
from ._mixins import HookMixin
from ._mixins import ProfileMixin

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
        :raises ValueError: job inputs must be of type File
        :return: self
        :rtype: AbstractJob
        """
        for file in input_files:
            if not isinstance(file, File):
                raise ValueError("a job input must be of type File")

            _input = _JobInput(file)
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
        :raises ValueError: a job output must be of type File 
        :return: self
        :rtype: AbstractJob
        """
        for file in output_files:
            if not isinstance(file, File):
                raise ValueError("a job output must be of type File")

            output = _JobOutput(
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
        :type file: File or str
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
        return _filter_out_nones(
            {
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
                "hooks": {
                    hook_name: [hook.__json__() for hook in values]
                    for hook_name, values in self.hooks.items()
                }
                if len(self.hooks) > 0
                else None,
            }
        )


class Job(AbstractJob):
    """A typical workflow Job that wraps a transformation/executable"""

    def __init__(
        self, transformation, _id=None, node_label=None, namespace=None, version=None,
    ):
        """Constructor
        
        :param transformation: Transformation object or name of the Transformation that this job uses
        :type transformation: Transformation|str
        :param _id: a unique id; if none is given then one will be assigned when the job is added by a Workflow, defaults to None
        :type _id: str, optional
        :param node_label: a brief job description, defaults to None
        :type node_label: str, optional
        :param namespace: namespace to which the transformation belongs, defaults to None
        :type namespace: str, optional
        :param version: version of the given transformation, defaults to None
        :type version: str, optional
        :raises ValueError: transformation must be one of Transformation or str
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
            raise ValueError("transformation must be of type Transformation or str")

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


class DAX(AbstractJob):
    """This type of job represents a sub-DAX that will be planned and executed
    by the workflow"""

    def __init__(self, file, _id=None, node_label=None):
        """Constructor
        
        :param file: File object or name of the dax file that will be used for this job
        :type file: File|str
        :param _id: a unique id; if none is given then one will be assigned when the job is added by a Workflow, defaults to None
        :type _id: str, optional
        :param node_label: a brief job description, defaults to None
        :type node_label: str, optional
        :raises ValueError: file must be of type File or str
        """
        AbstractJob.__init__(self, _id=_id, node_label=node_label)

        if not isinstance(file, File) and not isinstance(file, str):
            raise ValueError("file must be of type File or str")

        if isinstance(file, File):
            self.file = file
        else:
            self.file = File(file)

        self.add_inputs(self.file)

    def __json__(self):
        dax_json = {"type": "dax", "file": self.file.lfn}
        dax_json.update(AbstractJob.__json__(self))

        return dax_json


class DAG(AbstractJob):
    """This type of job represents a sub-DAG that will be executed by this 
    workflow"""

    def __init__(self, file, _id=None, node_label=None):
        """Constructor
        
        :param file: File object or name of the dag file that will be used for this job
        :type file: File|str
        :param _id: a unique id; if none is given then one will be assigned when the job is added by a Workflow, defaults to None
        :type _id: str, optional
        :param node_label: a brief job description, defaults to None
        :type node_label: str, optional
        :raises ValueError: file must be of type File or str
        """
        AbstractJob.__init__(self, _id=_id, node_label=node_label)

        if not isinstance(file, File) and not isinstance(file, str):
            raise ValueError("file must be of type File or str")

        if isinstance(file, File):
            self.file = file
        else:
            self.file = File(file)

        self.add_inputs(self.file)

    def __json__(self):
        dag_json = {"type": "dag", "file": self.file.lfn}
        dag_json.update(AbstractJob.__json__(self))

        return dag_json


class _JobInput:
    """Internal class used to represent a job's input"""

    def __init__(self, file):
        self.file = file
        self._type = "input"

    def __hash__(self):
        return hash(self.file)

    def __eq__(self, other):
        if isinstance(other, _JobInput):
            return self.file.lfn == other.file.lfn
        raise ValueError("_JobInput cannot be compared with {0}".format(type(other)))

    def __json__(self):
        return {"file": self.file.__json__(), "type": self._type}


class _JobOutput:
    """Internal class used to represent a job's output"""

    def __init__(self, file, stage_out=True, register_replica=False):
        self.file = file
        self._type = "output"
        self.stage_out = stage_out
        self.register_replica = register_replica

    def __hash__(self):
        return hash(self.file)

    def __eq__(self, other):
        if isinstance(other, _JobOutput):
            return self.file.lfn == other.file.lfn
        raise ValueError("__JobOutput cannot be comapred with {0}".format(type(other)))

    def __json__(self):
        return {
            "file": self.file.__json__(),
            "type": self._type,
            "stageOut": self.stage_out,
            "registerReplica": self.register_replica,
        }


class JobDependency:
    """Internal class used to represent a job's dependencies"""

    def __init__(self, parent_id, children_ids):
        self.parent_id = parent_id
        self.children_ids = children_ids

    def __eq__(self, other):
        if isinstance(other, JobDependency):
            return (
                self.parent_id == other.parent_id
                and self.children_ids == other.children_ids
            )
        raise ValueError(
            "JobDependency cannot be compared with {0}".format(type(other))
        )

    def __json__(self):
        return {"id": self.parent_id, "children": list(self.children_ids)}


class Workflow(Writable, HookMixin, ProfileMixin, MetadataMixin):
    """ Main abastraction for representing multi-step computational steps as a directed
    acyclic graph. """

    def __init__(self, name, infer_dependencies=False):
        """Constructor
        
        :param name: name of the Workflow
        :type name: str
        :param infer_dependencies: whether or not to automatically compute job dependencies based on input and output files used by each job, defaults to False
        :type infer_dependencies: bool, optional
        """
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
        """Add one or more jobs at a time to the Workflow
        
        :raises DuplicateError: a job with the same id already exists in this workflow 
        :return: self
        :rtype: Workflow
        """
        for job in jobs:
            if job._id == None:
                job._id = self._get_next_job_id()

            if job._id in self.jobs:
                raise DuplicateError("Job with id {0} already exists".format(job._id))

            self.jobs[job._id] = job

        return self

    def get_job(self, _id):
        """Retrieve the job with the given id
        
        :param _id: id of the job to be retrieved from the Workflow
        :type _id: str
        :raises NotFoundError: a job with the given id does not exist in this Workflow
        :return: the Job with the given id
        :rtype: Job
        """
        if _id not in self.jobs:
            raise NotFoundError("job with _id={0} not found".format(_id))

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

    def include_catalog(self, catalog):
        """Inline the given catalog into this Workflow. When a Workflow is written 
        to a file, if a catalog has been included, then the contents of the catalog
        will appear on the same file as the Workflow. 
        
        :param catalog: the catalog to be included
        :type catalog: SiteCatalog|TransformationCatalog|ReplicaCatalog
        :raises ValueError: a ReplicaCatalog has already been included
        :raises ValueError: a TransformationCatalog has already been included
        :raises ValueError: a SiteCatalog has already been included
        :raises ValueError: a type other than the aformentioned catalogs was given
        :return: self
        :rtype: Workflow
        """
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
        """Manually specify a dependency between one job to one or more other jobs
        
        :param parent: parent job
        :type parent: Job
        :param children: one or more child jobs
        :raises DuplicateError: a dependency has already been added between the parent job and one of the child jobs
        :return: self
        :rtype: Workflow
        """
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

        return _filter_out_nones(
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
                "hooks": {
                    hook_name: [hook.__json__() for hook in values]
                    for hook_name, values in self.hooks.items()
                }
                if len(self.hooks) > 0
                else None,
            }
        )
