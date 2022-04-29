import re
from collections import OrderedDict
from enum import Enum
from functools import partialmethod, wraps
from pathlib import Path
from typing import Dict, Optional, Union

from ._utils import _chained, _get_enum_str


# --- metadata -----------------------------------------------------------------
class MetadataMixin:
    """Derived class can have metadata assigned to it as key value pairs."""

    @_chained
    def add_metadata(self, *args: Dict[str, Union[str, int, float, bool]], **kwargs):
        """
        add_metadata(self, *args: Dict[str, Union[str, int, float, bool]], **kwargs)
        Add metadata key value pairs to this object

        .. code-block:: python

            # Example 1
            job.add_metadata({"key1": "value1"})

            # Example 2
            job.add_metadata(key1="value1, key2="value2")

        :param args: dictionary of key value pair to add as metadata
        :type args: Dict[str, Union[str, int, float, bool]]
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
    specified by :py:class:`~Pegasus.api.mixins.EventType`, takes place.
    """

    @_chained
    def add_shell_hook(self, event_type: EventType, cmd: str):
        """
        add_shell_hook(self, event_type: EventType, cmd: str)
        Add a shell hook. The given command will be executed by the shell
        when the specified :py:class:`~Pegasus.api.mixins.EventType` takes
        place.

        .. code-block:: python

            # Example
            wf.add_shell_hook(EventType.START, "echo 'hello'")

        :param event_type: an event type defined in :py:class:`~Pegasus.api.mixins.EventType`
        :type event_type: EventType
        :param cmd: shell command
        :type cmd: str
        :raises ValueError: event_type must be one of :py:class:`~Pegasus.api.mixins.EventType`
        :return: self
        """
        # TODO: consider making event_type either an event type or an actual ShellHook
        if not isinstance(event_type, EventType):
            raise ValueError("event_type must be one of EventType")

        self.hooks[_ShellHook.__hook_type__].append(_ShellHook(event_type, cmd))


class _Hook:
    """Base class that specific hook types will inherit from"""

    def __init__(self, event_type):
        """Constructor

        :param event_type: an event type defined in :py:class:`~Pegasus.api.mixins.EventType`
        :type event_type: EventType
        :raises ValueError: event_type must be of type :py:class:`~Pegasus.api.mixins.EventType`
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

        :param event_type: an event type defined in :py:class:`~Pegasus.api.mixins.EventType`
        :type event_type: EventType
        :param cmd: shell command
        :type cmd: str
        """
        _Hook.__init__(self, event_type)
        self.cmd = cmd

    def __json__(self):
        return OrderedDict([("_on", self.on), ("cmd", self.cmd)])


# --- profiles -----------------------------------------------------------------
class Namespace(Enum):
    """Profile Namespace values recognized by Pegasus"""

    PEGASUS = "pegasus"
    CONDOR = "condor"
    DAGMAN = "dagman"
    ENV = "env"
    GLOBUS = "globus"
    SELECTOR = "selector"
    STAT = "stat"


def _profiles(ns, **map_p):
    """Internal decorator that enables the use of kw args in functions like
    ProfileMixin.add_condor_profile() and ProfileMixin.add_dagman_profile().
    A handful of profile keys contain "." or "-", and so those profile keys cannot
    be used for kw args. By providing a mapping of legal key names to actual key names,
    adding profiles becomes more natural. For example we can have the following:

    .. code-block:: python

        # Example
        @_profiles(
            Namespace.DAGMAN,
            pre_args="PRE.ARGUMENTS"
        )
        def add_dagman_profile(self, pre_args: str = None):
            ...

    This way, available Profile keys will appear in an IDE and we can use kw args
    for keys that would be invalid as python variable names.

    :param ns: namespace
    :type ns: Namespace
    """

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

            return ProfileMixin.add_profiles(self, ns, **new_kw)

        return wrapped_f

    return wrap


def to_mb(value: str) -> int:
    """Convert the given value to MB

    :param value: str formatted as str formatted as :code:`'<int> [MB | GB | TB | PB | EB | ZB | YB]'`
    :type value: str
    :raises ValueError: invalid format
    :return: value in MB
    :rtype: int
    """
    try:
        value = str(value).strip()
        m = re.match(r"^\s*(\d+([Ee][+-]?\d+)?)\s*([MmGgPpEeZzYy][Bb])?\s*$", value)

        if not m:
            raise ValueError

        # float(..) is required as int("1E1") raises a ValueError
        amt = int(float(m.group(1)))
        unit = m.group(3).lower() if m.group(3) else None

        if unit is None:
            return amt
        else:
            _bytes = {
                "mb": 1 << 20,
                "gb": 1 << 30,
                "tb": 1 << 40,
                "pb": 1 << 50,
                "eb": 1 << 60,
                "zb": 1 << 70,
                "yb": 1 << 80,
            }

            return int((amt * _bytes[unit]) / _bytes["mb"])
    except Exception:
        raise ValueError(
            "value: {} should be a str formatted as '<int> [MB | GB | TB | PB | EB | ZB | YB]'".format(
                value
            )
        )


class ProfileMixin:
    @_chained
    def add_profiles(
        self,
        ns: Namespace,
        key: Optional[str] = None,
        value: Optional[Union[str, int, float, bool, Path]] = None,
        **kw
    ):
        r"""
        add_profiles(self, ns: Namespace, key: Optional[str] = None, value: Optional[str, int, float, bool, Path] = None, **kw)
        Add a profile.

        If key and value are given, then :code:`**kw` are ignored and :code:`{Namespace::key : value}`
        is added. Else :code:`**kw` is added. When the value of "key" is not a valid python
        variable name, the usage in Example #1 should be used, else follow the usage
        shown in Example #2.

        .. code-block:: python

            # Example 1
            job.add_profiles(Namespace.DAGMAN, key="pre.arguments", value="-i f1")

            # Example 2
            job.add_profiles(Namespace.ENV, JAVA_HOME="/usr/bin/java", USER="ryan")

        For :py:meth:`~Pegasus.api.mixins.ProfileMixin.add_globus_profiles`, :py:meth:`~Pegasus.api.mixins.ProfileMixin.add_condor_profiles`,
        :py:meth:`~Pegasus.api.mixins.ProfileMixin.add_dagman_profiles`, :py:meth:`~Pegasus.api.mixins.ProfileMixin.add_selector_profiles`,
        and :py:meth:`~Pegasus.api.mixins.ProfileMixin.add_pegasus_profiles`, if a profile key
        you are trying to use is not listed as a key word argument, use this
        function to add the profile.

        :raises TypeError: namespace must be one of Namespace
        :return: self
        """

        if not isinstance(ns, Namespace):
            raise TypeError(
                "invalid ns: {ns}; ns should be one of {enum_str}".format(
                    ns=ns, enum_str=_get_enum_str(Namespace)
                )
            )

        ns = ns.value

        # add profile(s)
        if key and value:
            if isinstance(value, Path) or type(value) == bool:
                # convert pathlib.Path and bool to str
                value = str(value)

            self.profiles[ns].update({key: value})
        else:
            for k, v in kw.items():
                if isinstance(v, Path) or type(v) == bool:
                    # convert pathlib.Path, bool, to str
                    kw[k] = str(v)

            self.profiles[ns].update(kw)

    #: Add environment variable(s)
    add_env = partialmethod(add_profiles, Namespace.ENV)

    @_profiles(
        Namespace.GLOBUS,
        count="count",
        job_type="jobtype",
        max_cpu_time="maxcputime",
        max_memory=("maxmemory", to_mb),
        max_time="maxtime",
        max_wall_time="maxwalltime",
        min_memory=("minmemory", to_mb),
        project="project",
        queue="queue",
    )
    def add_globus_profile(
        self,
        *,
        count: int = None,
        job_type: str = None,
        max_cpu_time: int = None,
        max_memory: str = None,
        max_time: int = None,
        max_wall_time: int = None,
        min_memory: int = None,
        project: str = None,
        queue: str = None
    ):
        """Add Globus profile(s).

        The globus profile namespace encapsulates Globus resource specification
        language (RSL) instructions. The RSL configures settings and behavior of
        the remote scheduling system.

        :param count: the number of times an executable is started, defaults to None
        :type count: int, optional
        :param job_type: specifies how the job manager should start the remote job. While Pegasus defaults to single, use mpi when running MPI jobs., defaults to None
        :type job_type: str, optional
        :param max_cpu_time: the max CPU time in minutes for a single execution of a job, defaults to None
        :type max_cpu_time: int, optional
        :param max_memory: the maximum memory in MB required for the job. Given as a str formatted as '<int> [MB | GB | TB | PB | EB]', defaults to None
        :type max_memory: str, optional
        :param max_time: the maximum time or walltime in minutes for a single execution of a job, defaults to None
        :type max_time: int, optional
        :param max_wall_time: the maximum walltime in minutes for a single execution of a job, defaults to None
        :type max_wall_time: int, optional
        :param min_memory: the minumum amount of memory required for this job. Given as a str formatted as '<int> [MB | GB | TB | PB | EB]', defaults to None
        :type min_memory: str, optional
        :param project: associates an account with a job at the remote end, defaults to None
        :type project: str, optional
        :param queue: the remote queue in which the job should be run. Used when remote scheduler is PBS that supports queues, defaults to None
        :type queue: str, optional
        :return: self
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
        request_memory=("request_memory", to_mb),
        request_disk=("request_disk", to_mb),
        requirements="requirements",
        should_transfer_files="should_transfer_files",
        when_to_transfer_output="when_to_transfer_output",
        condor_collector="condor_collector",
        grid_resource="grid_resource",
        cream_attributes="cream_attributes",
    )
    def add_condor_profile(
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
        requirements: str = None,
        should_transfer_files: str = None,
        when_to_transfer_output: str = None,
        condor_collector: str = None,
        grid_resource: str = None,
        cream_attributes: str = None
    ):
        """Add Condor profile(s).

        The condor profiles permit to add or overwrite instructions in the Condor submit file.

        :param universe: Pegasus defaults to either globus or scheduler universes. Set to standard for compute jobs that require standard universe. Set to vanilla to run natively in a condor pool, or to run on resources grabbed via condor glidein, defaults to None
        :type universe: str, optional
        :param periodic_release: is the number of times job is released back to the queue if it goes to HOLD, e.g. due to Globus errors. Pegasus defaults to 3, defaults to None
        :type periodic_release: str, optional
        :param periodic_remove: is the number of times a job is allowed to get into HOLD state before being removed from the queue. Pegasus defaults to 3, defaults to None
        :type periodic_remove: str, optional
        :param filesystem_domain: Useful for Condor glide-ins to pin a job to a remote site, defaults to None
        :type filesystem_domain: str, optional
        :param stream_error: boolean to turn on the streaming of the stderr of the remote job back to submit host, defaults to None
        :type stream_error: bool, optional
        :param stream_output: boolean to turn on the streaming of the stdout of the remote job back to submit host, defaults to None
        :type stream_output: bool, optional
        :param priority: integer value to assign the priority of a job. Higher value means higher priority. The priorities are only applied for vanilla / standard/ local universe jobs. Determines the order in which a users own jobs are executed, defaults to None
        :type priority: str, optional
        :param request_cpus: Number of CPU's a job requires, defaults to None
        :type request_cpus: str, optional
        :param request_gpus: Number of GPU's a job requires, defaults to None
        :type request_gpus: str, optional
        :param request_memory: Amount of memory a job requires. Given as a str formatted as '<int> [MB | GB | TB | PB | EB]', defaults to None
        :type request_memory: str, optional
        :param request_disk: Amount of disk a job requires. Given as a str formatted as '<int> [MB | GB | TB | PB | EB]', defaults to None
        :type request_disk: str, optional
        :param requirements: a job requirements expression such as :code:`"(CUDACapability >= 1.2) && $(requirements:True)"`
        :type requirements: str, optional
        :param should_transfer_files: Used to define if HTCondor should transfer files to and from the remote machine where the job runs, Given as :code:`"YES"`, :code:`"NO"`, or :code:`"IF_NEEDED"`
        :type should_transfer_file: str, optional
        :param when_to_transfer_output: Given as a one of :code:`"ON_EXIT"`, :code:`"ON_EXIT_OR_EVICT"`, or :code:`"ON_SUCCESS"`
        :type when_to_transfer_output: str, optional
        :param condor_collector: Specify the condor collector to use (e.g :code:`"ccg-testing999.isi.edu"`)
        :type condor_collector: str, optional
        :param grid_resource: Specify a grid resource such as :code:`"batch pbs"`
        :type grid_resource: str, optional,
        :param cream_attributes: Additional cream attributes (e.g. :code:`"key1=value1;key2=value2"`)
        :type cream_attributes: str, optional
        :return: self
        """
        ...

    @_profiles(
        Namespace.PEGASUS,
        clusters_num="clusters.num",
        clusters_size="clusters.size",
        job_aggregator="job.aggregator",
        job_aggregator_arguments="job.aggregator.arguments",
        grid_start="gridstart",
        grid_start_path="gridstart.path",
        grid_start_arguments="gridstart.arguments",
        grid_start_launcher="gridstart.launcher",
        grid_start_launcher_arguments="gridstart.launcher.arguments",
        stagein_clusters="stagein.clusters",
        stagein_local_clusters="stagein.local.clusters",
        stagein_remote_clusters="stagein.remote.clusters",
        stageout_clusters="stageout.clusters",
        stageout_local_clusters="stageout.local.clusters",
        stageout_remote_clusters="stageout.remote.clusters",
        group="group",
        change_dir="change.dir",
        create_dir="create.dir",
        transfer_proxy="transfer.proxy",
        style="style",
        pmc_request_memory=("pmc_request_memory", to_mb),
        pmc_request_cpus="pmc_request_cpus",
        pmc_priority="pmc_priority",
        pmc_task_arguments="pmc_task_arguments",
        exitcode_failure_msg="exitcode.failuremsg",
        exitcode_success_msg="exitcode.successmsg",
        checkpoint_time="checkpoint.time",
        max_walltime="maxwalltime",
        glite_arguments="glite.arguments",
        auxillary_local="auxillary.local",
        condor_arguments_quote="condor.arguments.quote",
        runtime="runtime",
        clusters_max_runtime="clusters.maxruntime",
        cores="cores",
        gpus="gpus",
        nodes="nodes",
        ppn="ppn",
        memory=("memory", to_mb),
        diskspace=("diskspace", to_mb),
        data_configuration="data.configuration",
        queue="queue",
        project="project",
        boto_config="BOTO_CONFIG",
        container_arguments="container.arguments",
        label="label",
        pegasus_lite_env_source="pegasus_lite_env_source",
        SSH_PRIVATE_KEY="SSH_PRIVATE_KEY",
        relative_submit_dir="relative.submit.dir",
    )
    def add_pegasus_profile(
        self,
        *,
        clusters_num: int = None,
        clusters_size: int = None,
        job_aggregator: int = None,
        job_aggregator_arguments: str = None,
        grid_start: int = None,
        grid_start_path: str = None,
        grid_start_arguments: str = None,
        grid_start_launcher: str = None,
        grid_start_launcher_arguments: str = None,
        stagein_clusters: int = None,
        stagein_local_clusters: int = None,
        stagein_remote_clusters: int = None,
        stageout_clusters: int = None,
        stageout_local_clusters: int = None,
        stageout_remote_clusters: int = None,
        group: str = None,
        change_dir: bool = None,
        create_dir: bool = None,
        transfer_proxy: bool = None,
        style: str = None,
        pmc_request_memory: str = None,
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
        gpus: int = None,
        nodes: int = None,
        ppn: int = None,
        memory: str = None,
        diskspace: str = None,
        data_configuration=None,
        queue: str = None,
        project: str = None,
        boto_config: str = None,
        container_arguments: str = None,
        label: str = None,
        pegasus_lite_env_source: Union[str, Path] = None,
        SSH_PRIVATE_KEY: str = None,
        relative_submit_dir: Union[str, Path] = None
    ):
        """Add Pegasus profile(s).

        :param clusters_num: Determines the total number of clusters per level, jobs are evenly spread across clusters (see `Pegasus Clustering Guide <https://pegasus.isi.edu/documentation/job_clustering.php#horizontal_clustering>`_ for more information), defaults to None
        :type clusters_num: int, optional
        :param clusters_size: Determines the number of jobs in each cluster (see `Pegasus Clustering Guide <https://pegasus.isi.edu/documentation/job_clustering.php#horizontal_clustering>`_ for more information), defaults to None
        :type clusters_size: int, optional
        :param job_aggregator: Indicates the clustering executable that is used to run the clustered job on the remote site, defaults to None
        :type job_aggregator: int, optional
        :param job_aggregator_arguments: Additional arguments with which a clustering executable should be invoked, defaults to None
        :type job_aggregator_arguments: str, optional
        :param grid_start: Determines the executable for launching a job (see `docs <https://pegasus.isi.edu/documentation/profiles.php#hints_profiles>`_ for more information), defaults to None
        :type grid_start: int, optional
        :param grid_start_path: Sets the path to the gridstart . This profile is best set in the Site Catalog, defaults to None
        :type grid_start_path: str, optional
        :param grid_start_arguments: Sets the arguments with which GridStart is used to launch a job on the remote site, defaults to None
        :type grid_start_arguments: str, optional
        :param grid_start_launcher: specifies the path to the executable to launch kickstart, defaults to None,
        :type grid_start_launcher: str, optional
        :param grid_start_launcher_arguments: the arguments to the launcher executable if any
        :type grid_start_launcher_arguments: str, optional
        :param stagein_clusters: This key determines the maximum number of stage-in jobs that are can executed locally or remotely per compute site per workflow. This is used to configure the BalancedCluster Transfer Refiner, which is the Default Refiner used in Pegasus. This profile is best set in the Site Catalog or in the Properties file, defaults to None
        :type stagein_clusters: int, optional
        :param stagein_local_clusters: This key provides finer grained control in determining the number of stage-in jobs that are executed locally and are responsible for staging data to a particular remote site. This profile is best set in the Site Catalog or in the Properties file, defaults to None
        :type stagein_local_clusters: int, optional
        :param stagein_remote_clusters: This key provides finer grained control in determining the number of stage-in jobs that are executed remotely on the remote site and are responsible for staging data to it. This profile is best set in the Site Catalog or in the Properties file, defaults to None
        :type stagein_remote_clusters: int, optional
        :param stageout_clusters: This key determines the maximum number of stage-out jobs that are can executed locally or remotely per compute site per workflow. This is used to configure the BalancedCluster Transfer Refiner, , which is the Default Refiner used in Pegasus, defaults to None
        :type stageout_clusters: int, optional
        :param stageout_local_clusters: This key provides finer grained control in determining the number of stage-out jobs that are executed locally and are responsible for staging data from a particular remote site. This profile is best set in the Site Catalog or in the Properties file, defaults to None
        :type stageout_local_clusters: int, optional
        :param stageout_remote_clusters: This key provides finer grained control in determining the number of stage-out jobs that are executed remotely on the remote site and are responsible for staging data from it. This profile is best set in the Site Catalog or in the Properties file, defaults to None
        :type stageout_remote_clusters: int, optional
        :param group: Tags a job with an arbitrary group identifier. The group site selector makes use of the tag, defaults to None
        :type group: str, optional
        :param change_dir: If true, tells kickstart to change into the remote working directory. Kickstart itself is executed in whichever directory the remote scheduling system chose for the job, defaults to None
        :type change_dir: bool, optional
        :param create_dir: If true, tells kickstart to create the the remote working directory before changing into the remote working directory. Kickstart itself is executed in whichever directory the remote scheduling system chose for the job, defaults to None
        :type create_dir: bool, optional
        :param transfer_proxy: If true, tells Pegasus to explicitly transfer the proxy for transfer jobs to the remote site. This is useful, when you want to use a full proxy at the remote end, instead of the limited proxy that is transferred by CondorG, defaults to None
        :type transfer_proxy: bool, optional
        :param style: Sets the condor submit file style. If set to globus, submit file generated refers to CondorG job submissions. If set to condor, submit file generated refers to direct Condor submission to the local Condor pool. It applies for glidein, where nodes from remote grid sites are glided into the local condor pool. The default style that is applied is globus, defaults to None
        :type style: str, optional
        :param pmc_request_memory: This key is used to set the -m option for pegasus-mpi-cluster. It specifies the amount of memory in MB that a job requires. This profile is usually set in the DAX for each job. Given as a str formatted as '<int> [MB | GB | TB | PB | EB]', defaults to None
        :type pmc_request_memory: str, optional
        :param pmc_request_cpus: This key is used to set the -c option for pegasus-mpi-cluster. It specifies the number of cpu's that a job requires. This profile is usually set in the DAX for each job, defaults to None
        :type pmc_request_cpus: int, optional
        :param pmc_priority: This key is used to set the -p option for pegasus-mpi-cluster. It specifies the priority for a job . This profile is usually set in the DAX for each job. Negative values are allowed for priorities, defaults to None
        :type pmc_priority: int, optional
        :param pmc_task_arguments: The key is used to pass any extra arguments to the PMC task during the planning time. They are added to the very end of the argument string constructed for the task in the PMC file. Hence, allows for overriding of any argument constructed by the planner for any particular task in the PMC job, defaults to None
        :type pmc_task_arguments: str, optional
        :param exitcode_failure_msg: The message string that pegasus-exitcode searches for in the stdout and stderr of the job to flag failures, defaults to None
        :type exitcode_failure_msg: str, optional
        :param exitcode_success_msg: The message string that pegasus-exitcode searches for in the stdout and stderr of the job to determine whether a job logged it's success message or not. Note this value is used to check for whether a job failed or not i.e if this profile is specified, and pegasus-exitcode DOES NOT find the string in the job stdout or stderr, the job is flagged as failed. The complete rules for determining failure are described in the man page for pegasus-exitcode, defaults to None
        :type exitcode_success_msg: str, optional
        :param checkpoint_time: the expected time in minutes for a job after which it should be sent a TERM signal to generate a job checkpoint file, defaults to None
        :type checkpoint_time: int, optional
        :param max_walltime: the maximum walltime in minutes for a single execution of a job, defaults to None
        :type max_walltime: int, optional
        :param glite_arguments: specifies the extra arguments that must appear in the local PBS generated script for a job, when running workflows on a local cluster with submissions through Glite. This is useful when you want to pass through special options to underlying LRMS such as PBS e.g. you can set value -l walltime=01:23:45 -l nodes=2 to specify your job's resource requirements, defaults to None
        :type glite_arguments: str, optional
        :param auxillary_local: indicates whether auxillary jobs associated with a compute site X, can be run on local site. This CAN ONLY be specified as a profile in the site catalog and should be set when the compute site filesystem is accessible locally on the submit host, defaults to None
        :type auxillary_local: bool, optional
        :param condor_arguments_quote: indicates whether condor quoting rules should be applied for writing out the arguments key in the condor submit file. By default it is true unless the job is schedule to a glite style site. The value is automatically set to false for glite style sites, as condor quoting is broken in batch_gahp, defaults to None
        :type condor_arguments_quote: bool, optional
        :param runtime: Specifies the expected runtime of a job in seconds, defaults to None
        :type runtime: str, optional
        :param clusters_max_runtime: Specifies the maximum runtime of a job, defaults to None
        :type clusters_max_runtime: int, optional
        :param cores: The total number of cores required for a job. This is also used for accounting purposes in the database while generating statistics. It corresponds to the multiplier_factor in the job_instance table, defaults to None
        :type cores: int, optional
        :param gpus: The total number of gpus required for a job, defaults to None
        :type gpus: int, optional
        :param nodes: Indicates the number of nodes a job requires, defaults to None
        :type nodes: int, optional
        :param ppn: Indicates the number of processors per node. This profile is best set in the Site Catalog and usually set when running workflows with MPI jobs, defaults to None
        :type ppn: int, optional
        :param memory: Indicates the maximum memory a job requires. Given as a str formatted as '<int> [MB | GB | TB | PB | EB]', defaults to None
        :type memory: str, optional
        :param diskspace: Indicates the maximum diskspace a job requires in MB. Given as a str formatted as '<int> [MB | GB | TB | PB | EB]', defaults to None
        :type diskspace: int, optional
        :param data_configuration: Indicates the data configuration setup. Can be one of "sharedfs", "condorio", or "nonsharedfs", defaults to None
        :type data_configuration: str, optional
        :param queue: This specifies the queue for the job. (e.g. :code:`"normal"`)
        :type queue: str, optional
        :param project: Causes the job time to be charged to or associated with a particular project/account. This is not used for SGE.
        :type project: str, optional
        :param boto_config: Specified which :code:`.boto` file to use (e.g. :code:`"/home/myuser/.boto"`)
        :type boto_config: str, optional
        :param container_arguments: additional cli arguments that will be added to the :code:`docker container run` or :code:`singularity exec` command
        :type container_arguments: str, optional
        :param label: associate a label to a job; used for label based clustering
        :type label: str, optional
        :param pegasus_lite_env_source: specify a path on the submit host to indicate the file that needs to be sourced, defaults to None
        :type pegasus_lite_env_source: Union[str, Path], optional
        :param SSH_PRIVATE_KEY: path to the ssh private key which will be used in this workflow (it is recommended that a special set of keys are created specifically for workflow use)
        :type SSH_PRIVATE_KEY: str, optional
        :param relative_submit_dir: specify the relative directory a job's submit files are written to, defaults to None
        :type relative_submit_dir: Union[str, Path], optional
        """
        ...

    @_profiles(
        Namespace.SELECTOR,
        execution_site="execution.site",
        pfn="pfn",
        grid_job_type="grid.jobtype",
    )
    def add_selector_profile(
        self,
        *,
        execution_site: str = None,
        pfn: Union[str, Path] = None,
        grid_job_type: str = None
    ):
        """Add Selector(s).

        The Selector namespace allows users to override the beahvior of the Workflow
        Mapper during site selection. This gives you finer grained control over
        where a job executes and what executable it refers to.

        :param execution_site: the execution site where a job should be executed, defaults to None
        :type execution_site: str, optional
        :param pfn: the physical file name to the main executable that a job refers to. Overrides any entries specified in the transformation catalog, defaults to None
        :type pfn: Union[str, Path], optional
        :param grid_job_type: This profile is usually used to ensure that a compute job executes on another job manager (see `docs <https://pegasus.isi.edu/documentation/profiles.php#hints_profiles>`_ for more information), defaults to None
        :type grid_job_type: str, optional
        :return: self
        """
        ...

    @_profiles(
        Namespace.DAGMAN,
        pre="PRE",
        pre_arguments="PRE.ARGUMENTS",
        post="POST",
        post_arguments="POST.ARGUMENTS",
        retry="RETRY",
        category="CATEGORY",
        priority="PRIORITY",
        abort_dag_on="ABORT-DAG-ON",
        max_pre="MAXPRE",
        max_post="MAXPOST",
        max_jobs="MAXJOBS",
        max_idle="MAXIDLE",
        post_scope="POST.SCOPE",
    )
    def add_dagman_profile(
        self,
        *,
        pre: Union[str, Path] = None,
        pre_arguments: str = None,
        post: str = None,
        post_arguments: str = None,
        retry: str = None,
        category: str = None,
        priority: str = None,
        abort_dag_on: str = None,
        max_pre: str = None,
        max_post: str = None,
        max_jobs: str = None,
        max_idle: str = None,
        post_scope: str = None
    ):
        """Add Dagman profile(s).

        Note, to add the profile keys "post.path.[value of dagman.post]" or
        ["category-name].maxjobs", use the following:

        .. code-block:: python

            job.add_profiles(Namespace.DAGMAN, key="post.path.<value of dagman.post>", value=<value string>)
            job.add_profiles(Namespace.DAGMAN, key="<category-name>.maxjobs", value=<value string>)

        :param pre: is the path to the pre-script. DAGMan executes the pre-script before it runs the job, defaults to None
        :type pre: Union[str, Path], optional
        :param pre_arguments: are command-line arguments for the pre-script, if any, defaults to None
        :type pre_arguments: str, optional
        :param post: is the postscript type/mode that a user wants to associate with a job (see `docs <https://pegasus.isi.edu/documentation/profiles.php>`_ for more information), defaults to None
        :type post: str, optional
        :param post_arguments: are the command line arguments for the post script, if any, defaults to None
        :type post_arguments: str, optional
        :param retry: is the number of times DAGMan retries the full job cycle from pre-script through post-script, if failure was detected, defaults to None
        :type retry: int, optional
        :param category: the DAGMan category the job belongs to, defaults to None
        :type category: str, optional
        :param priority: the priority to apply to a job. DAGMan uses this to select what jobs to release when MAXJOBS is enforced for the DAG, defaults to None
        :type priority: int, optional
        :param abort_dag_on: The ABORT-DAG-ON key word provides a way to abort the entire DAG if a given node returns a specific exit code (AbortExitValue). The syntax for the value of the key is AbortExitValue [RETURN DAGReturnValue] . When a DAG aborts, by default it exits with the node return value that caused the abort. This can be changed by using the optional RETURN key word along with specifying the desired DAGReturnValue, defaults to None
        :type abort_dag_on: str, optional
        :param max_pre: sets the maximum number of PRE scripts within the DAG that may be running at one time, defaults to None
        :type max_pre: str, optional
        :param max_post: sets the maximum number of POST scripts within the DAG that may be running at one time, defaults to None
        :type max_post: str, optional
        :param max_jobs: sets the maximum number of jobs within the DAG that will be submitted to Condor at one time, defaults to None
        :type max_jobs: str, optional
        :param max_idle: Sets the maximum number of idle jobs allowed before HTCondor DAGMan stops submitting more jobs. Once idle jobs start to run, HTCondor DAGMan will resume submitting jobs. If the option is omitted, the number of idle jobs is unlimited, defaults to None
        :type max_idle: str, optional
        :param post_scope: can be "all", "none" or "essential" (see `docs <https://pegasus.isi.edu/documentation/profiles.php>`_ for more information), defaults to None
        :type post_scope: str, optional
        :return: self
        """
        ...

    # initially had add_<namespace>_profile for 5.0 release and in order not not
    # break existing workflows, these functions have aliases that are plural
    add_globus_profiles = add_globus_profile
    add_pegasus_profiles = add_pegasus_profile
    add_dagman_profiles = add_dagman_profile
    add_condor_profiles = add_condor_profile
    add_selector_profiles = add_selector_profile
