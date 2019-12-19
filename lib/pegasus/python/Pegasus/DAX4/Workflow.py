import json
from collections import defaultdict

import yaml

from .Encoding import filter_out_nones, FileFormat, CustomEncoder
from .Errors import DuplicateError, NotFoundError
from .TransformationCatalog import Transformation
from .ReplicaCatalog import File

PEGASUS_VERSION = "5.0"

# TODO: metadata
# TODO: hooks
# TODO: profiles
# TODO: maybe throw job arguments back up here because they were part of
# abstract job in the schema OR ask KARAN if they need to be in abstract job
# or job
# TODO: it would be useful to be able to say job1.add_inputs(job0.get_outputs())
class AbstractJob:
    def __init__(self, _id=None, node_label=None):
        self._id = _id
        self.node_label = node_label
        self.args = list()
        self.inputs = set()
        self.outputs = set()

        self.stdout = None
        self.stderr = None
        self.stdin = None

        # self.hooks = defaultdict(dict)
        # self.profiles = defaultdict(dict)
        # self.metadata = dict()

    def add_inputs(self, *input_files):
        for file in input_files:
            self.inputs.add(JobInput(file))

        return self

    def add_outputs(self, *output_files, stage_out=True, register_replica=False):
        for file in output_files:
            self.outputs.add(
                JobOutput(file, stage_out=stage_out, register_replica=register_replica)
            )

        return self

    def add_args(self, *args):
        self.args.extend(args)

        return self

    def set_stdout(self, file, stage_out=True, register_replica=False):
        if not isinstance(file, str):
            file = File(file)

        self.add_outputs(file, stage_out=stage_out, register_replica=register_replica)

    def __json__(self):
        raise NotImplementedError()


class Job(AbstractJob):
    def __init__(
        self, transformation, _id=None, node_label=None, namespace=None, version=None,
    ):
        self.namespace = None
        self.version = None
        self.transformation = transformation

        AbstractJob.__init__(self, _id=_id, node_label=node_label)

    def __json__(self):
        return filter_out_nones(
            {
                "namespace": self.namespace,
                "version": self.version,
                "name": self.transformation.name,
                "id": self._id,
                "nodeLabel": self.node_label,
                "arguments": [
                    arg if isinstance(arg, File) else arg for arg in self.args
                ],
                "uses": [_input.__json__() for _input in self.inputs]
                + [output.__json__() for output in self.outputs],
            }
        )


class JobInput:
    def __init__(self, file):
        self.file = file
        self._type = "input"

    def __hash__(self):
        return hash(self.file)

    def __json__(self):
        return {"type": self._type, "lfn": self.file.lfn}


class JobOutput:
    def __init__(self, file, stage_out=True, register_replica=False):
        self.file = file
        self._type = "output"
        self.stage_out = stage_out
        self.register_replica = register_replica

    def __hash__(self):
        return hash(self.file)

    def __json__(self):
        return {
            "lfn": self.file.lfn,
            "type": self._type,
            "stageOut": self.stage_out,
            "registerReplica": self.register_replica,
        }


class JobDependency:
    parent_id: str
    children_ids: set

    def __init__(self, parent_id, children_ids):
        self.parent_id = parent_id
        self.children_ids = children_ids

    def __json__(self):
        return {"id": self.parent_id, "children": list(self.children_ids)}


class DAX(AbstractJob):
    pass


class DAG(AbstractJob):
    pass


# TODO: profiles
# TODO: hooks
# TODO: metadata


class Workflow:
    def __init__(self, name, infer_dependencies=False, default_filepath="Workflow"):

        self.name = name
        self.infer_dependencies = infer_dependencies
        self.default_filepath = default_filepath

        self.jobs = dict()
        self.dependencies = defaultdict(JobDependency)

        # sequence unique to this workflow only
        self.sequence = 1

        """
        TBD, its a touchy subject...
        self.site_catalog = None
        self.transformation_catalog = None
        self.replica_catalog = None
        """

    def add_job(self, job):
        if job._id == None:
            job._id = self.get_next_job_id()

        if job._id in self.jobs:
            raise DuplicateError("Job with id {0} already exists".format(job._id))

        self.jobs[job._id] = job

        return self

    def get_next_job_id(self):
        next_id = None
        while not next_id or next_id in self.jobs:
            next_id = "ID{:07d}".format(self.sequence)
            self.sequence += 1

        return next_id

    """
    TBD, its a touchy subject...
    def add_site_catalog(self, site_catalog):
        pass

    def add_replica_catalog(self, replica_catalog):
        pass

    def add_transformation_catalog(self, transformation_catalog):
        pass
    """

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
            for filename, io in mapping.items():
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
        if not isinstance(file_format, FileFormat):
            raise ValueError("invalid file format {}".format(file_format))

        self._infer_dependencies()

        path = self.default_filepath
        if non_default_filepath != "":
            path = non_default_filepath
        else:
            if file_format == FileFormat.YAML:
                path = ".".join([self.default_filepath, FileFormat.YAML.value])
            elif file_format == FileFormat.JSON:
                path = ".".join([self.default_filepath, FileFormat.JSON.value])

        with open(path, "w") as file:
            if file_format == FileFormat.YAML:
                yaml.dump(CustomEncoder().default(self), file)
            elif file_format == FileFormat.JSON:
                json.dump(self, file, cls=CustomEncoder, indent=4)

    def __json__(self):
        # TODO: remove 'pegasus' from tc, rc, sc

        return filter_out_nones(
            {
                "pegasus": PEGASUS_VERSION,
                "name": self.name,
                "jobs": [job.__json__() for _id, job in self.jobs.items()],
                "jobDependencies": [
                    dependency.__json__()
                    for _id, dependency in self.dependencies.items()
                ]
                if len(self.dependencies) > 0
                else None,
            }
        )
