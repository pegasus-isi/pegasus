from enum import Enum
from functools import partialmethod
from functools import wraps

from .errors import DuplicateError
from .errors import NotFoundError
from ._utils import _get_enum_str

# --- metadata -----------------------------------------------------------------
class MetadataMixin:
    """Derived class can have metadata assigned to it as key value pairs."""

    def add_metadata(self, *args, **kwargs):
        """Add metadata key value pairs to this object

        .. code-block:: python

            # Example 1
            job.add_metadata({"key1": "value1"})

            # Example 2
            job.add_metadata(key1="value1, key2="value2")

        :param args: dictionary of key value pair to add as metadata
        :type args: dict, optional
        :raises TypeError: each arg in args must be a dict
        :return: self
        """

        # values will be converted to str; metadata schema requires str values
        for arg in args:
            if isinstance(arg, dict):
                self.metadata.update({key: str(value) for key, value in arg.items()})
            else:
                raise TypeError("{arg} must be of type dict".format(arg=arg))

        for key, value in kwargs.items():
            self.metadata[key] = str(value)

        return self


# --- hooks --------------------------------------------------------------------
class EventType(Enum):
    """Event type on which a hook will be triggered"""

    NEVER = "never"
    START = "start"
    ERROR = "error"
    SUCCESS = "success"
    END = "end"
    ALL = "all"


class HookMixin:
    """Derived class can have hooks assigned to it. This currently supports
    shell hooks. The supported hooks are triggered when some event,
    specified by :py:class:`~Pegasus.dax4.mixins.EventType`, takes place.
    """

    def add_shell_hook(self, event_type, cmd):
        # TODO: consider making event_type either an event type or an actual ShellHook
        """Add a shell hook. The given command will be executed by the shell
        when the specified :py:class:`~Pegasus.dax4.mixins.EventType` takes
        place.

        .. code-block:: python

            # Example
            wf.add_shell_hook(EventType.START, "echo 'hello'")
        
        :param event_type: an event type defined in :py:class:`~Pegasus.dax4.mixins.EventType`
        :type event_type: EventType
        :param cmd: shell command
        :type cmd: str
        :raises ValueError: event_type must be one of :py:class:`~Pegasus.dax4.mixins.EventType`
        :return: self
        """
        if not isinstance(event_type, EventType):
            raise ValueError("event_type must be one of EventType")

        self.hooks[_ShellHook.__hook_type__].append(_ShellHook(event_type, cmd))

        return self


class _Hook:
    """Base class that specific hook types will inherit from"""

    def __init__(self, event_type):
        """Constructor
        
        :param event_type: an event type defined in :py:class:`~Pegasus.dax4.mixins.EventType`
        :type event_type: EventType
        :raises ValueError: event_type must be of type :py:class:`~Pegasus.dax4.mixins.EventType`
        """
        if not isinstance(event_type, EventType):
            raise ValueError("event_type must be one of EventType")

        self.on = event_type.value

    # TODO: def get/set event type


# TODO: make this public
class _ShellHook(_Hook):
    """A hook that executes a shell command"""

    __hook_type__ = "shell"

    def __init__(self, event_type, cmd):
        """Constructor
        
        :param event_type: an event type defined in :py:class:`~Pegasus.dax4.mixins.EventType`
        :type event_type: EventType
        :param cmd: shell command
        :type cmd: str
        """
        _Hook.__init__(self, event_type)
        self.cmd = cmd

    def __json__(self):
        return {"_on": self.on, "cmd": self.cmd}


# --- profiles -----------------------------------------------------------------
class Namespace(Enum):
    """Profile Namespace values recognized by Pegasus"""

    PEGASUS = "pegasus"
    CONDOR = "condor"
    DAGMAN = "dagman"
    ENV = "env"
    HINTS = "hints"
    GLOBUS = "globus"
    SELECTOR = "selector"
    STAT = "stat"


def _profiles(ns, **map_p):
    def wrap(f):
        @wraps(f)
        def wrapped_f(self, *a, **kw):
            # map of the actual property name to a value
            # for example, "abort_dag" would become {"ABORT-DAG": some_value}
            new_kw = {}
            for k, v in kw.items():
                if v is not None and k in map_p:
                    if isinstance(map_p[k], str):
                        new_kw[map_p[k]] = v
                    elif isinstance(map_p[k], (tuple, list)):
                        # for some properties, we can pass a long a function as well
                        # that can be used to do thing such as convert from one unit
                        nk, vf = map_p[k]
                        new_kw[nk] = vf(v)

            # calling this will verify kwargs
            f(self, **kw)

            return ProfileMixin._add_profiles(self, ns, **new_kw)

        return wrapped_f

    return wrap


class ProfileMixin:
    '''
    """Derived class can have profiles assigned to it"""

    def add_profile(self, namespace, key, value):
        """Add a profile to this object
        
        .. code-block:: python

            # Example 1
            preprocess = (
                Transformation("preprocess")
                    .add_profile(Namespace.GLOBUS, "maxtime", 2)
                    .add_profile(Namespace.DAGMAN, "retry", 3)
            )

            # Example 2
            job = (
                Job(preprocess)
                    .add_profile(Namespace.ENV, "FOO", "bar")
            )

        :param namespace: a namespace defined in :py:class:`~Pegasus.dax4.mixins.Namespace`
        :type namespace: Namespace
        :param key: key
        :type key: str
        :param value: value
        :type value: str
        :raises ValueError: namespace must be one of :py:class:`~Pegasus.dax4.mixins.Namespace`
        :raises DuplicateError: profiles must be unique
        :return: self
        """
        if not isinstance(namespace, Namespace):
            raise ValueError("namespace must be one of Namespace")

        if namespace.value in self.profiles:
            if key in self.profiles[namespace.value]:
                raise DuplicateError(
                    "Duplicate profile with namespace: {0}, key: {1}, value: {2}".format(
                        namespace.value, key, value
                    )
                )

        self.profiles[namespace.value][key] = value

        return self
    '''

    def _add_profiles(self, ns, key=None, value=None, **kw):
        if not isinstance(ns, Namespace):
            raise TypeError(
                "invalid ns: {ns}; ns should be one of {enum_str}".format(
                    ns=ns, enum_str=_get_enum_str(Namespace)
                )
            )

        ns = ns.value

        # add profile(s)
        if key and value:
            self.profiles[ns].update({key: value})
        else:
            self.profiles[ns].update(kw)

        return self

    add_env = partialmethod(_add_profiles, Namespace.ENV)

    @_profiles(
        Namespace.GLOBUS,
        count="count",
        job_type="jobtype",
        max_cpu_time="maxcputime",
        max_memory="maxmemory",
        max_time="maxtime",
        max_wall_time="maxwalltime",
        min_memory="minmemory",
        project="project",
        queue="queue",
    )
    def add_globus(
        self,
        *,
        count: int = None,
        job_type: str = None,
        max_cpu_time: int = None,
        max_memory: int = None,
        max_time: int = None,
        max_wall_time: int = None,
        min_memory: int = None,
        project: str = None,
        queue: str = None,
    ):
        """[summary]
        
        :param count: [description], defaults to None
        :type count: int, optional
        :param job_type: [description], defaults to None
        :type job_type: str, optional
        :param max_cpu_time: [description], defaults to None
        :type max_cpu_time: int, optional
        :param max_memory: [description], defaults to None
        :type max_memory: int, optional
        :param max_time: [description], defaults to None
        :type max_time: int, optional
        :param max_wall_time: [description], defaults to None
        :type max_wall_time: int, optional
        :param min_memory: [description], defaults to None
        :type min_memory: int, optional
        :param project: [description], defaults to None
        :type project: str, optional
        :param queue: [description], defaults to None
        :type queue: str, optional
        """
        ...

    @_profiles(
        Namespace.CONDOR,
        universe="universe",
        periodic_release="periodic_release",
        periodic_remove="periodic_remove",
        filesystem_domain="filesystemdomain",
        stream_error="stream_error",
        stream_output="stream_output",
        priority="priority",
        request_cpus="request_cpus",
        request_gpus="request_gpus",
        request_memory="request_memory",
        request_disk="request_disk",
    )
    def add_condor(
        self,
        *,
        universe: str = None,
        periodic_release: str = None,
        periodic_remove: str = None,
        filesystem_domain: str = None,
        stream_error: bool = None,
        stream_output: bool = None,
        priority: str = None,
        request_cpus: str = None,
        request_gpus: str = None,
        request_memory: str = None,
        request_disk: str = None,
    ):
        """[summary]
        
        :param universe: [description], defaults to None
        :type universe: str, optional
        :param periodic_release: [description], defaults to None
        :type periodic_release: str, optional
        :param periodic_remove: [description], defaults to None
        :type periodic_remove: str, optional
        :param filesystem_domain: [description], defaults to None
        :type filesystem_domain: str, optional
        :param stream_error: [description], defaults to None
        :type stream_error: bool, optional
        :param stream_output: [description], defaults to None
        :type stream_output: bool, optional
        :param priority: [description], defaults to None
        :type priority: str, optional
        :param request_cpus: [description], defaults to None
        :type request_cpus: str, optional
        :param request_memory: [description], defaults to None
        :type request_memory: str, optional
        :param request_disk: [description], defaults to None
        :type request_disk: str, optional
        """
        ...

    @_profiles(
        Namespace.PEGASUS,
        clusters_num="clusters.num",
        clusters_size="clusters.size",
        job_aggregator="job.aggregator",
        grid_start="gridstart",
        grid_start_path="gridstart.path",
        grid_start_arguments="gridstart.arguments",
        stagein_clusters="stagein.clusters",
        stagein_local_clusters="stagein.local.clusters",
        stagein_remove_clusters="stagein.remove.clusters",
        stageout_clusters="stageout.clusters",
        stageout_local_clusters="stageout.local.clusters",
        stageout_remote_clusters="stageout.remote.clusters",
        group="group",
        change_dir="change.dir",
        create_dir="create.dir",
        transfer_proxy="transfer.proxy",
        style="style",
        pmc_request_memory="pmc_request_memory",
        pmc_request_cpus="pmc_request_cpus",
        pmc_priority="pmc_priority",
        pmc_task_arguments="pmc_task_arguments",
        exitcode_failure_msg="exitcode.failuremsg",
        exitcode_success_msg="exitcode.successmsg",
        checkpoint_time="checkpoint_time",
        max_walltime="maxwalltime",
        glite_arguments="glite.arguments",
        auxillary_local="auxillary.local",
        condor_arguments_quote="condor.arguments.quote",
        runtime="runtime",
        clusters_max_runtime="clusters.maxruntime",
        cores="cores",
        nodes="nodes",
        ppn="ppn",
        memory="memory",
        diskspace="diskspace",
    )
    def add_pegasus(
        self,
        *,
        clusters_num: int = None,
        clusters_size: int = None,
        job_aggregator: int = None,
        grid_start: int = None,
        grid_start_path: str = None,
        grid_start_arguments: str = None,
        stagein_clusters: int = None,
        stagein_local_clusters: int = None,
        stagein_remove_clusters: int = None,
        stageout_clusters: int = None,
        stageout_local_clusters: int = None,
        stageout_remote_clusters: int = None,
        group: str = None,
        change_dir: bool = None,
        create_dir: bool = None,
        transfer_proxy: bool = None,
        style: str = None,
        pmc_request_memory: int = None,
        pmc_request_cpus: int = None,
        pmc_priority: int = None,
        pmc_task_arguments: str = None,
        exitcode_failure_msg: str = None,
        exitcode_success_msg: str = None,
        checkpoint_time: int = None,
        max_walltime: int = None,
        glite_arguments: str = None,
        auxillary_local: bool = None,
        condor_arguments_quote: bool = None,
        runtime: str = None,
        clusters_max_runtime: int = None,
        cores: int = None,
        nodes: int = None,
        ppn: int = None,
        memory: int = None,
        diskspace: int = None,
    ):
        """[summary]
        
        :param clusters_num: [description], defaults to None
        :type clusters_num: int, optional
        :param clusters_size: [description], defaults to None
        :type clusters_size: int, optional
        :param job_aggregator: [description], defaults to None
        :type job_aggregator: int, optional
        :param grid_start: [description], defaults to None
        :type grid_start: int, optional
        :param grid_start_path: [description], defaults to None
        :type grid_start_path: str, optional
        :param grid_start_arguments: [description], defaults to None
        :type grid_start_arguments: str, optional
        :param stagein_clusters: [description], defaults to None
        :type stagein_clusters: int, optional
        :param stagein_local_clusters: [description], defaults to None
        :type stagein_local_clusters: int, optional
        :param stagein_remove_clusters: [description], defaults to None
        :type stagein_remove_clusters: int, optional
        :param stageout_clusters: [description], defaults to None
        :type stageout_clusters: int, optional
        :param stageout_local_clusters: [description], defaults to None
        :type stageout_local_clusters: int, optional
        :param stageout_remote_clusters: [description], defaults to None
        :type stageout_remote_clusters: int, optional
        :param group: [description], defaults to None
        :type group: str, optional
        :param change_dir: [description], defaults to None
        :type change_dir: bool, optional
        :param create_dir: [description], defaults to None
        :type create_dir: bool, optional
        :param transfer_proxy: [description], defaults to None
        :type transfer_proxy: bool, optional
        :param style: [description], defaults to None
        :type style: str, optional
        :param pmc_request_memory: [description], defaults to None
        :type pmc_request_memory: int, optional
        :param pmc_request_cpus: [description], defaults to None
        :type pmc_request_cpus: int, optional
        :param pmc_priority: [description], defaults to None
        :type pmc_priority: int, optional
        :param pmc_task_arguments: [description], defaults to None
        :type pmc_task_arguments: str, optional
        :param exitcode_failure_msg: [description], defaults to None
        :type exitcode_failure_msg: str, optional
        :param exitcode_success_msg: [description], defaults to None
        :type exitcode_success_msg: str, optional
        :param checkpoint_time: [description], defaults to None
        :type checkpoint_time: int, optional
        :param max_walltime: [description], defaults to None
        :type max_walltime: int, optional
        :param glite_arguments: [description], defaults to None
        :type glite_arguments: str, optional
        :param auxillary_local: [description], defaults to None
        :type auxillary_local: bool, optional
        :param condor_arguments_quote: [description], defaults to None
        :type condor_arguments_quote: bool, optional
        """
        ...

    @_profiles(
        Namespace.HINTS,
        execution_site="execution.site",
        pfn="pfn",
        grid_job_type="grid.jobtype",
    )
    def add_hint(
        self, *, execution_site: str = None, pfn: str = None, grid_job_type: str = None
    ):
        """[summary]
        
        :param execution_site: [description], defaults to None
        :type execution_site: str, optional
        :param pfn: [description], defaults to None
        :type pfn: str, optional
        :param grid_job_type: [description], defaults to None
        :type grid_job_type: str, optional
        """
        ...

    def add_dagman(
        self,
        *,
        pre: str = None,
        pre_arguments: str = None,
        post: str = None,
        post_path: str = None,
        post_arguments: str = None,
        retry: int = None,
        category: str = None,
        priority: int = None,
        abort_dag_on: str = None,
        max_pre: str = None,
        max_post: str = None,
        max_jobs: str = None,
        max_idle: str = None,
        max_jobs_category: str = None,
        max_jobs_category_value: str = None,
        post_scope: str = None,
    ):
        """[summary]
        
        :param pre: [description], defaults to None
        :type pre: str, optional
        :param pre_arguments: [description], defaults to None
        :type pre_arguments: str, optional
        :param post: [description], defaults to None
        :type post: str, optional
        :param post_path: [description], defaults to None
        :type post_path: str, optional
        :param post_arguments: [description], defaults to None
        :type post_arguments: str, optional
        :param retry: [description], defaults to None
        :type retry: int, optional
        :param category: [description], defaults to None
        :type category: str, optional
        :param priority: [description], defaults to None
        :type priority: int, optional
        :param abort_dag_on: [description], defaults to None
        :type abort_dag_on: str, optional
        :param max_pre: [description], defaults to None
        :type max_pre: str, optional
        :param max_post: [description], defaults to None
        :type max_post: str, optional
        :param max_jobs: [description], defaults to None
        :type max_jobs: str, optional
        :param max_idle: [description], defaults to None
        :type max_idle: str, optional
        :param max_jobs_category: [description], defaults to None
        :type max_jobs_category: str, optional
        :param max_jobs_category_value: [description], defaults to None
        :type max_jobs_category_value: str, optional
        :param post_scope: [description], defaults to None
        :type post_scope: str, optional
        :return: [description]
        :rtype: [type]
        """
        map_p = {
            "pre": "PRE",
            "pre_arguments": "PRE.ARGUMENTS",
            "post": "POST",
            "post_path": "post.path",
            "post_arguments": "POST.ARGUMENTS",
            "retry": "RETRY",
            "category": "CATEGORY",
            "priority": "PRIORITY",
            "abort_dag_on": "ABORT-DAG-ON",
            "max_pre": "MAXPRE",
            "max_post": "MAXPOST",
            "max_jobs": "MAXJOBS",
            "max_idle": "MAXIDLE",
            "post_scope": "POST.SCOPE",
        }

        kw = locals()
        new_kw = {}
        for k, v in kw.items():
            if v is not None and k in map_p:
                new_kw[map_p[k]] = v

        # set the key: post.path.[value of dagman.post]
        if post_path and post:
            new_kw["post.path.{POST}".format(POST=kw["post"])] = new_kw["post.path"]
            try:
                del new_kw["post.path"]
            except KeyError:
                pass

        # set the key: [CATEGORY-NAME].MAXJOBS
        if max_jobs_category and max_jobs_category_value:
            new_kw["{CATEGORY}.MAXJOBS".format(CATEGORY=kw["max_jobs_category"])] = kw[
                "max_jobs_category_value"
            ]

        return self._add_profiles(Namespace.DAGMAN, **new_kw)

