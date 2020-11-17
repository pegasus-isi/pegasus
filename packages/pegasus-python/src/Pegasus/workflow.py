from collections import defaultdict
from io import StringIO
from typing import TextIO

from Pegasus import yaml
from Pegasus.api.errors import PegasusError
from Pegasus.api.replica_catalog import File
from Pegasus.api.workflow import (
    Job,
    SubWorkflow,
    Workflow,
    _JobDependency,
    _LinkType,
    _Use,
)
from Pegasus.replica_catalog import _to_rc
from Pegasus.site_catalog import _to_sc
from Pegasus.transformation_catalog import _to_tc

"""
:mod:`workflow` exposes an API to serialize and deserialize Pegasus's workflow file.

Basic Usage::

    >>> from Pegasus import workflow
    >>> workflow.loads("... ")
    ...

    >>> print(workflow.dumps( ... ))
    ' ... '

.. moduleauthor:: Ryan Tanaka <tanaka@isi.edu>
.. moduleauthor:: Rajiv Mayani <mayani@isi.edu>
"""


__all__ = (
    "load",
    "loads",
    "dump",
    "dumps",
)


def _to_wf(d: dict) -> Workflow:
    """Convert dict to Workflow

    :param d: Workflow represented as a dict
    :type d: dict
    :raises PegasusError: encountered error parsing
    :return: a Workflow object based on d
    :rtype: Workflow
    """

    try:
        #
        wf = Workflow(d["name"], infer_dependencies=False)

        # add rc
        if "replicaCatalog" in d:
            wf.replica_catalog = _to_rc(d["replicaCatalog"])

        # add tc
        if "transformationCatalog" in d:
            wf.transformation_catalog = _to_tc(d["transformationCatalog"])

        # add sc
        if "siteCatalog" in d:
            wf.site_catalog = _to_sc(d["siteCatalog"])

        # add jobs
        for j in d["jobs"]:
            # create appropriate job based on type
            if j["type"] == "job":
                job = Job(
                    j["name"],
                    _id=j["id"],
                    node_label=j.get("nodeLabel"),
                    namespace=j.get("namespace"),
                    version=j.get("version"),
                )
            elif j["type"] in {"pegasusWorkflow", "condorWorkflow"}:
                f = File(j["file"])

                is_planned = False if j["type"] == "pegasusWorkflow" else True

                job = SubWorkflow(
                    f, is_planned, _id=j["id"], node_label=j.get("nodeLabel")
                )

            else:
                raise ValueError

            # add args
            args = list()
            for a in j["arguments"]:
                args.append(a)

            job.args = args

            # add uses
            uses = set()
            for u in j["uses"]:
                f = File(u["lfn"], size=u.get("size"))
                try:
                    f.metadata = u["metadata"]
                except KeyError:
                    pass

                uses.add(
                    _Use(
                        f,
                        getattr(_LinkType, u["type"].upper()),
                        stage_out=u.get("stageOut"),
                        register_replica=u.get("registerReplica"),
                        bypass_staging=u.get("bypass"),
                    )
                )

            job.uses = uses

            # set stdin
            if "stdin" in j:
                for u in job.uses:
                    if u.file.lfn == j["stdin"]:
                        job.stdin = u.file
                        break

            # set stdout
            if "stdout" in j:
                for u in job.uses:
                    if u.file.lfn == j["stdout"]:
                        job.stdout = u.file
                        break

            # set stderr
            if "stderr" in j:
                for u in job.uses:
                    if u.file.lfn == j["stderr"]:
                        job.stderr = u.file
                        break

            # add profiles
            if j.get("profiles"):
                job.profiles = defaultdict(dict, j.get("profiles"))

            # add metadata
            if j.get("metadata"):
                job.metadata = j.get("metadata")

            # add hooks
            if j.get("hooks"):
                job.hooks = defaultdict(list, j.get("hooks"))

            # add job to wf
            wf.add_jobs(job)

        # add dependencies
        if d.get("jobDependencies"):
            dependencies = defaultdict(_JobDependency)
            for item in d.get("jobDependencies"):
                dependencies[item["id"]] = _JobDependency(
                    item["id"], {child for child in item["children"]}
                )

            wf.dependencies = dependencies

        # add profiles
        if d.get("profiles"):
            wf.profiles = defaultdict(dict, d.get("profiles"))

        # add metadata
        if d.get("metadata"):
            wf.metadata = d.get("metadata")

        # add hooks
        if d.get("hooks"):
            wf.hooks = defaultdict(list, d.get("hooks"))

        return wf
    except (KeyError, ValueError):
        raise PegasusError("error parsing {}".format(d))


def load(fp: TextIO, *args, **kwargs) -> Workflow:
    """
    Deserialize ``fp`` (a ``.read()``-supporting file-like object containing a Workflow document) to a :py:class:`~Pegasus.api.workflow.Workflow` object.

    :param fp: file like object to load from
    :type fp: TextIO
    :return: deserialized Workflow object
    :rtype: Workflow
    """
    return _to_wf(yaml.load(fp))


def loads(s: str, *args, **kwargs) -> Workflow:
    """
    Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance containing a Workflow document) to a :py:class:`~Pegasus.api.workflow.Workflow` object.

    :param s: string to load from
    :type s: str
    :return: deserialized Workflow object
    :rtype: Workflow
    """
    return _to_wf(yaml.load(s))


def dump(obj: Workflow, fp: TextIO, _format="yml", *args, **kwargs) -> None:
    """
    Serialize ``obj`` as a :py:class:`~Pegasus.api.worklfow.Workflow` formatted stream to ``fp`` (a ``.write()``-supporting file-like object).

    :param obj: Workflow to serialize
    :type obj: Workflow
    :param fp: file like object to serialize to
    :type fp: TextIO
    :param _format: format to write to if fp does not have an extension; can be one of ["yml" | "yaml" | "json"], defaults to "yml"
    :type _format: str
    :rtype: NoReturn
    """
    obj.write(fp, _format=_format)


def dumps(obj: Workflow, _format="yml", *args, **kwargs) -> str:
    """
    Serialize ``obj`` to a :py:class:`~Pegasus.api.workflow.Workflow` formatted ``str``.

    :param obj: Workflow to serialize
    :type obj: Workflow
    :param _format: format to write to if fp does not have an extension; can be one of ["yml" | "yaml" | "json"], defaults to "yml"
    :type _format: str
    :return: Workflow serialized as a string
    :rtype: str
    """
    with StringIO() as s:
        obj.write(s, _format=_format)
        s.seek(0)
        return s.read()
