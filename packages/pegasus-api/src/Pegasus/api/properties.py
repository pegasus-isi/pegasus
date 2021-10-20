import logging
from configparser import DEFAULTSECT, ConfigParser
from io import StringIO
from typing import Optional, TextIO, Union

__all__ = ["Properties"]

log = logging.getLogger(__name__)


class Properties:
    """Write Pegasus properties to a file.

    .. code-block:: python

        # Example
        props = Properties()
        props["globus.maxtime"] = 900
        props["globus.maxwalltime"] = 1000
        props["dagman.retry"] = 4

        props.write()

    """

    _pattern_props = (
        # variable property keys
        "pegasus.file.cleanup.constraint.*.maxspace",
        "pegasus.log.*",
        "pegasus.metrics.app.*",
        "pegasus.transfer.*.remote.sites",
        "pegasus.transfer.*.impl",
        "pegasus.selector.site.env.*",
        "pegasus.selector.regex.rank.*",
        "pegasus.selector.replica.*.prefer.stagein.sites",
        "pegasus.selector.replica.*.ignore.stagein.sites",
        "pegasus.catalog.replica.output.*",
        "pegasus.catalog.*.timeout",
        "pegasus.catalog.replica.db.*",
        "env.*",
        "dagman.*",
        "condor.*",
        "globus.*",
    )

    _props = (
        "pegasus.mode",
        "pegasus.home.datadir",
        "pegasus.home.sysconfdir",
        "pegasus.home.sharedstatedir",
        "pegasus.home.localstatedir",
        "pegasus.dir.submit.logs",
        "pegasus.dir.useTimestamp",
        "pegasus.dir.exec",
        "pegasus.dir.submit.mapper",
        "pegasus.dir.staging.mapper",
        "pegasus.dir.storage.mapper",
        "pegasus.dir.storage.deep",
        "pegasus.dir.create.strategy",
        "pegasus.schema.dax",
        "pegasus.schema.sc",
        "pegasus.schema.ivr",
        "pegasus.catalog.replica",
        "pegasus.catalog.replica.file",
        "pegasus.catalog.replica.chunk.size",
        "pegasus.catalog.replica.cache.asrc",
        "pegasus.catalog.replica.dax.asrc",
        "pegasus.catalog.site",
        "pegasus.catalog.site.file",
        "pegasus.catalog.transformation",
        "pegasus.catalog.transformation.file",
        "pegasus.selector.replica",
        "pegasus.selector.site",
        "pegasus.selector.site.path",
        "pegasus.selector.site.timeout",
        "pegasus.selector.site.keep.tmp",
        "pegasus.data.configuration",
        "pegasus.transfer.bypass.input.staging",
        "pegasus.transfer.arguments",
        "pegasus.transfer.threads",
        "pegasus.transfer.lite.arguments",
        "pegasus.transfer.worker.package",
        "pegasus.transfer.worker.package.autodownload",
        "pegasus.transfer.worker.package.strict",
        "pegasus.transfer.links",
        "pegasus.transfer.staging.delimiter",
        "pegasus.transfer.disable.chmod.sites",
        "pegasus.transfer.setup.source.base.url",
        "pegasus.monitord.events",
        "pegasus.catalog.workflow.url",
        "pegasus.catalog.workflow.amqp.events",
        "pegasus.catalog.workflow.amqp.url",
        "pegasus.catalog.master.url",
        "pegasus.monitord.output",
        "pegasus.dashboard.output",
        "pegasus.monitord.notifications",
        "pegasus.monitord.notifications.max",
        "pegasus.monitord.notifications.timeout",
        "pegasus.monitord.stdout.disable.parsing",
        "pegasus.monitord.encoding",
        "pegasus.monitord.arguments",
        "pegasus.clusterer.job.aggregator",
        "pegasus.clusterer.job.aggregator.arguments",
        "pegasus.clusterer.job.aggregator.seqexec.log",
        "pegasus.clusterer.job.aggregator.seqexec.firstjobfail",
        "pegasus.clusterer.allow.single",
        "pegasus.clusterer.label.key",
        "pegasus.clusterer.preference",
        "pegasus.log.manager",
        "pegasus.log.manager.formatter",
        "pegasus.log.memory.usage",
        "pegasus.metrics.app",
        "pegasus.file.cleanup.strategy",
        "pegasus.file.cleanup.impl",
        "pegasus.file.cleanup.clusters.num",
        "pegasus.file.cleanup.clusters.size",
        "pegasus.file.cleanup.scope",
        "pegasus.file.cleanup.constraint.deferstageins",
        "pegasus.file.cleanup.constraint.csv",
        "pegasus.aws.account",
        "pegasus.aws.region",
        "pegasus.aws.batch.job_definition",
        "pegasus.aws.batch.compute_environment",
        "pegasus.aws.batch.job_queue",
        "pegasus.aws.batch.s3_bucket",
        "pegasus.code.generator",
        "pegasus.condor.concurrency.limits",
        "pegasus.register",
        "pegasus.register.deep",
        "pegasus.data.reuse.scope",
        "pegasus.catalog.transformation.mapper",
        "pegasus.selector.transformation",
        "pegasus.parser.dax.preserver.linebreaks",
        "pegasus.parser.dax.data.dependencies",
        "pegasus.integrity.checking",
        "env.PEGASUS_HOME",
        "env.GLOBUS_LOCATION",
        "env.LD_LIBRARY_PATH",
        "globus.count",
        "globus.jobtype",
        "globus.maxcputime",
        "globus.maxmemory",
        "globus.maxtime",
        "globus.maxwalltime",
        "globus.minmemory",
        "globus.project",
        "globus.queue",
        "condor.universe",
        "condor.periodic_release",
        "condor.periodic_remove",
        "condor.filesystemdomain",
        "condor.stream_error",
        "condor.stream_output",
        "condor.priority",
        "condor.request_cpus",
        "condor.request_gpus",
        "condor.request_memory",
        "condor.request_disk",
        "dagman.pre",
        "dagman.pre.arguments",
        "dagman.post",
        "dagman.post.path",
        "dagman.post.arguments",
        "dagman.retry",
        "dagman.category",
        "dagman.priority",
        "dagman.abort-dag-on",
        "dagman.maxpre",
        "dagman.maxpost",
        "dagman.maxjobs",
        "dagman.maxidle",
        "dagman.post.scope",
        "pegasus.cluster.num",
        "pegasus.clusters.size",
        "pegasus.job.aggregator",
        "pegasus.gridstart",
        "pegasus.gridstart.path",
        "pegasus.gridstart.arguments",
        "pegasus.gridstart.launcher",
        "pegasus.gridstart.launcher.arguments",
        "pegasus.stagein.clusters",
        "pegasus.stagein.local.clusters",
        "pegasus.stagein.remote.clusters",
        "pegasus.stageout.clusters",
        "pegasus.stageout.local.clusters",
        "pegasus.stageout.remote.clusters",
        "pegasus.group",
        "pegasus.change.dir",
        "pegasus.create.dir",
        "pegasus.transfer.proxy",
        "pegasus.style",
        "pegasus.pmc_request_memory",
        "pegasus.pmc_request_cpus",
        "pegasus.pmc_priority",
        "pegasus.pmc_task_arguments",
        "pegasus.exitcode.failuremsg",
        "pegasus.exitcode.successmsg",
        "pegasus.checkpoint.time",
        "pegasus.maxwalltime",
        "pegasus.glite.arguments",
        "pegasus.condor.arguments.quote",
        "pegasus.runtime",
        "pegasus.clusters.maxruntime",
        "pegasus.cores",
        "pegasus.nodes",
        "pegasus.ppn",
        "pegasus.memory",
        "pegasus.diskspace",
        "selector.execution.site",
        "selector.pfn",
        "selector.grid.jobtype",
        # Database
        "pegasus.catalog.replica.db.password",
        "pegasus.catalog.replica.db.user",
        "pegasus.catalog.replica.db.url",
        "pegasus.catalog.replica.db.driver",
        *_pattern_props,
    )

    _cfg_header_len = len("[{}]\n".format(DEFAULTSECT))

    @staticmethod
    def ls(prop: Optional[str] = None):
        """List property keys. Refer to
        `Configuration docs <https://pegasus.isi.edu/documentation/configuration.php>`_
        for additional information. If :code:`prop` is given, all properties
        containing prop will be printed, else all properties will be printed.

        .. code-block:: python

            # Example
            >>> P.ls("request")
            condor.request_cpus
            condor.request_disk
            condor.request_gpus
            condor.request_memory
            pegasus.pmc_request_cpus
            pegasus.pmc_request_memory

        :param prop: properties containing "prop" will be listed in alphabetical order, defaults to None
        :type prop: Optional[str]
        """
        if prop:
            to_print = list()
            for p in Properties._props:
                if prop in p:
                    to_print.append(p)

            to_print.sort()
            print(*to_print, sep="\n")
        else:
            print(*sorted(Properties._props), sep="\n")

    def __init__(self):
        self._conf = ConfigParser()
        # preserve case for keys
        self._conf.optionxform = str
        self._conf[DEFAULTSECT] = {}

    def __setitem__(self, k, v):
        self._conf[DEFAULTSECT][k] = self._escape(v)

        if not self._check_key(k):
            log.warning(
                "Unrecognized property key: '{}' has been set to '{}'".format(
                    k, self._escape(v)
                )
            )

    def __getitem__(self, k):
        return self._conf[DEFAULTSECT][k]

    def __delitem__(self, k):
        self._conf.remove_option(DEFAULTSECT, k)

    @classmethod
    def _check_key(cls, k) -> bool:
        """Check if the key :code:`k` is a valid Pegasus property."""
        rv = False
        if k in cls._props:
            rv = True
        else:
            for p in cls._pattern_props:
                _ = p.split("*")
                if k.startswith(_[0]) and k.endswith(_[1]) and len(k) >= len(p):
                    rv = True
                    break

        return rv

    @staticmethod
    def _escape(v):
        """Escape value :code:`v`."""
        if isinstance(v, str):
            return v
        else:
            return str(v)

    def write(self, file: Optional[Union[str, TextIO]] = None):
        """Write these properties to a file. If :code:`file` is not given, these
        properties are written to :code:`./pegasus.properties`

        .. code-block:: python

            # Example 1
            props.write()

            # Example 2
            with open("conf", "w") as f:
                props.write(f)

        :param file: file path or file object where properties will be written to, defaults to None
        :type file: Optional[Union[str, TextIO]]
        :raises TypeError: file must be of type str or file like object
        """
        with StringIO() as sio:
            self._conf.write(sio)

            # write without header
            props = sio.getvalue()[Properties._cfg_header_len :]

        # default file
        if file is None:
            file = "pegasus.properties"

        if isinstance(file, str):
            with open(file, "w") as f:
                f.write(props)
        elif hasattr(file, "read"):
            file.write(props)
        else:
            raise TypeError(
                "invalid file: {}; file must be of type str or file like object".format(
                    file
                )
            )
