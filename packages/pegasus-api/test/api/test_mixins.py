from collections import defaultdict

import pytest

from Pegasus.api.mixins import (
    EventType,
    HookMixin,
    MetadataMixin,
    Namespace,
    ProfileMixin,
    _Hook,
    _ShellHook,
    to_mb,
)


@pytest.fixture(scope="function")
def md_mixin_obj():
    def _metadata_mixin_obj():
        class MetadataMixinObj(MetadataMixin):
            def __init__(self):
                self.metadata = dict()

        return MetadataMixinObj()

    return _metadata_mixin_obj()


class TestMetadataMixin:
    def test_add_metadata_method_1(self, md_mixin_obj):
        assert id(md_mixin_obj.add_metadata({"key": "value"})) == id(md_mixin_obj)
        assert md_mixin_obj.metadata["key"] == "value"

    def test_add_metadata_method_2(self, md_mixin_obj):
        assert id(md_mixin_obj.add_metadata(key1="value1", key2="value2")) == id(
            md_mixin_obj
        )
        assert md_mixin_obj.metadata["key1"] == "value1"
        assert md_mixin_obj.metadata["key2"] == "value2"

    def test_add_invalid_metadata(self, md_mixin_obj):
        with pytest.raises(TypeError) as e:
            md_mixin_obj.add_metadata([1, 2])

        assert "[1, 2]" in str(e)

    def test_chaining(self, md_mixin_obj):
        assert md_mixin_obj.add_metadata(key1="value").add_metadata(key2="value")

        assert len(md_mixin_obj.metadata) == 2
        assert id(md_mixin_obj) == id(md_mixin_obj.add_metadata(key3="value"))


class Test_Hook:
    def test_valid_hook(self):
        _Hook(EventType.START)

    def test_invalid_hook(self):
        with pytest.raises(ValueError):
            _Hook("123")


class Test_ShellHook:
    def test_valid_shell_hook(self):
        _ShellHook(EventType.START, "some command")

    def test_invalid_shell_hook(self):
        with pytest.raises(ValueError):
            _ShellHook("123", "some command")

    def test_tojson(self):
        hook = _ShellHook(EventType.START, "some command")
        assert hook.__json__() == {"_on": "start", "cmd": "some command"}


@pytest.fixture(scope="function")
def hook_mixin_obj():
    def _hook_mixin_obj():
        class HookMixinObj(HookMixin):
            def __init__(self):
                self.hooks = defaultdict(list)

        return HookMixinObj()

    return _hook_mixin_obj()


class TestHookMixin:
    def test_add_shell_hook(self, hook_mixin_obj):
        cmd = "some command"
        hook_mixin_obj.add_shell_hook(EventType.START, cmd)
        assert "shell" in hook_mixin_obj.hooks

        added_shell_hook = hook_mixin_obj.hooks["shell"][0]

        assert added_shell_hook.on == EventType.START.value
        assert added_shell_hook.cmd == cmd

    def test_add_invalid_shell_hook(self, hook_mixin_obj):
        with pytest.raises(ValueError):
            hook_mixin_obj.add_shell_hook("123", "some command")

    def test_chaining(self, hook_mixin_obj):
        (
            hook_mixin_obj.add_shell_hook(
                EventType.START, "some command"
            ).add_shell_hook(EventType.START, "another command")
        )

        assert "shell" in hook_mixin_obj.hooks

        added_shell_hook_1 = hook_mixin_obj.hooks["shell"][0]
        assert added_shell_hook_1.on == EventType.START.value
        assert added_shell_hook_1.cmd == "some command"

        added_shell_hook_2 = hook_mixin_obj.hooks["shell"][1]
        assert added_shell_hook_2.on == EventType.START.value
        assert added_shell_hook_2.cmd == "another command"

        assert id(hook_mixin_obj) == id(
            hook_mixin_obj.add_shell_hook(EventType.START, "some command")
        )


@pytest.fixture(scope="function")
def obj():
    def _profile_mixin_obj():
        class ProfileMixinObj(ProfileMixin):
            def __init__(self):
                self.profiles = defaultdict(dict)

        return ProfileMixinObj()

    return _profile_mixin_obj()


class TestProfileMixin:
    def test_add_valid_profile(self, obj):
        assert id(
            obj.add_profiles(Namespace.ENV, key="ABC-123.45/", value="value")
        ) == id(obj)
        assert dict(obj.profiles) == {"env": {"ABC-123.45/": "value"}}

    def test_add_valid_profiles(self, obj):
        assert id(obj.add_profiles(Namespace.ENV, ENV1="env1", ENV2="env2",)) == id(obj)

        assert dict(obj.profiles) == {"env": {"ENV1": "env1", "ENV2": "env2"}}

    def test_add_invalid_profile(self, obj):
        with pytest.raises(TypeError) as e:
            obj.add_profiles("ns")

        assert "invalid ns: ns" in str(e)

    def test_add_env(self, obj):
        assert id(obj.add_env(ENV1="env1", ENV2="env2")) == id(obj)
        assert dict(obj.profiles) == {"env": {"ENV1": "env1", "ENV2": "env2"}}

    def test_add_globus_profile(self, obj):
        assert id(
            obj.add_globus_profile(
                count=1,
                job_type="single",
                max_cpu_time=2,
                max_memory="3",
                max_time=4,
                max_wall_time=5,
                min_memory="2 GB",
                project="abc",
                queue="queue",
            )
        ) == id(obj)

        assert dict(obj.profiles) == {
            "globus": {
                "count": 1,
                "jobtype": "single",
                "maxcputime": 2,
                "maxmemory": 3,
                "maxtime": 4,
                "maxwalltime": 5,
                "minmemory": 2048,
                "project": "abc",
                "queue": "queue",
            }
        }

    def test_add_globus_profile_invalid_profile(self, obj):
        with pytest.raises(TypeError) as e:
            obj.add_globus_profile(aa=1)

        assert "add_globus_profile() got an unexpected" in str(e)

    def test_add_dagman_profile(self, obj):
        assert id(
            obj.add_dagman_profile(
                pre="pre",
                pre_arguments="pre_args",
                post="post",
                post_arguments="post_args",
                retry=1,
                category="cat",
                priority="prio",
                abort_dag_on="abrt",
                max_pre="mp",
                max_post="mp",
                max_jobs="mj",
                max_idle="mi",
                post_scope="ps",
            )
        ) == id(obj)

        assert dict(obj.profiles) == {
            "dagman": {
                "PRE": "pre",
                "PRE.ARGUMENTS": "pre_args",
                "POST": "post",
                "POST.ARGUMENTS": "post_args",
                "RETRY": 1,
                "CATEGORY": "cat",
                "PRIORITY": "prio",
                "ABORT-DAG-ON": "abrt",
                "MAXPRE": "mp",
                "MAXPOST": "mp",
                "MAXJOBS": "mj",
                "MAXIDLE": "mi",
                "POST.SCOPE": "ps",
            }
        }

    def test_add_dagman_profile_invalid_profile(self, obj):
        with pytest.raises(TypeError) as e:
            obj.add_dagman_profile(aa=1)

        assert "add_dagman_profile() got an unexpected" in str(e)

    def test_add_condor_profile(self, obj):
        assert id(
            obj.add_condor_profile(
                universe="un",
                periodic_release="pr",
                periodic_remove="pr",
                filesystem_domain="fsd",
                stream_error="se",
                stream_output="so",
                priority="prio",
                request_cpus="rc",
                request_gpus="rg",
                request_memory="100 MB",
                request_disk="200 MB",
                requirements="(CUDACapability >= 1.2) && $(requirements:True)",
                should_transfer_files="YES",
                when_to_transfer_output="ON_EXIT",
                condor_collector="ccg-testing999.isi.edu",
                grid_resource="batch pbs",
                cream_attributes="key1=value1",
            )
        ) == id(obj)

        assert dict(obj.profiles) == {
            "condor": {
                "universe": "un",
                "periodic_release": "pr",
                "periodic_remove": "pr",
                "filesystemdomain": "fsd",
                "stream_error": "se",
                "stream_output": "so",
                "priority": "prio",
                "request_cpus": "rc",
                "request_gpus": "rg",
                "request_memory": 100,
                "request_disk": 200,
                "requirements": "(CUDACapability >= 1.2) && $(requirements:True)",
                "should_transfer_files": "YES",
                "when_to_transfer_output": "ON_EXIT",
                "condor_collector": "ccg-testing999.isi.edu",
                "grid_resource": "batch pbs",
                "cream_attributes": "key1=value1",
            }
        }

    def test_add_condor_profile_invalid_profile(self, obj):
        with pytest.raises(TypeError) as e:
            obj.add_condor_profile(aa=1)

        assert "add_condor_profile() got an unexpected" in str(e)

    def test_add_pegasus_profile(self, obj):
        assert id(
            obj.add_pegasus_profile(
                clusters_num="clusters.num",
                clusters_size="clusters.size",
                job_aggregator="job.aggregator",
                grid_start="gridstart",
                grid_start_path="gridstart.path",
                grid_start_arguments="gridstart.arguments",
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
                pmc_request_memory="512",
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
                memory="2 GB",
                diskspace="1 GB",
                data_configuration="condorio",
                queue="normal",
                project="-A project_name",
                boto_config="/home/myuser/.boto",
                container_arguments="--shm-size",
            )
        ) == id(obj)

        assert dict(obj.profiles) == {
            "pegasus": {
                "clusters.num": "clusters.num",
                "clusters.size": "clusters.size",
                "job.aggregator": "job.aggregator",
                "gridstart": "gridstart",
                "gridstart.path": "gridstart.path",
                "gridstart.arguments": "gridstart.arguments",
                "stagein.clusters": "stagein.clusters",
                "stagein.local.clusters": "stagein.local.clusters",
                "stagein.remote.clusters": "stagein.remote.clusters",
                "stageout.clusters": "stageout.clusters",
                "stageout.local.clusters": "stageout.local.clusters",
                "stageout.remote.clusters": "stageout.remote.clusters",
                "group": "group",
                "change.dir": "change.dir",
                "create.dir": "create.dir",
                "transfer.proxy": "transfer.proxy",
                "style": "style",
                "pmc_request_memory": 512,
                "pmc_request_cpus": "pmc_request_cpus",
                "pmc_priority": "pmc_priority",
                "pmc_task_arguments": "pmc_task_arguments",
                "exitcode.failuremsg": "exitcode.failuremsg",
                "exitcode.successmsg": "exitcode.successmsg",
                "checkpoint_time": "checkpoint_time",
                "maxwalltime": "maxwalltime",
                "glite.arguments": "glite.arguments",
                "auxillary.local": "auxillary.local",
                "condor.arguments.quote": "condor.arguments.quote",
                "runtime": "runtime",
                "clusters.maxruntime": "clusters.maxruntime",
                "cores": "cores",
                "nodes": "nodes",
                "ppn": "ppn",
                "memory": 2048,
                "diskspace": 1024,
                "data.configuration": "condorio",
                "queue": "normal",
                "project": "-A project_name",
                "BOTO_CONFIG": "/home/myuser/.boto",
                "container.arguments": "--shm-size",
            }
        }

    def test_add_pegasus_profile_invalid_profile(self, obj):
        with pytest.raises(TypeError) as e:
            obj.add_pegasus_profile(aa=1)

        assert "add_pegasus_profile() got an unexpected" in str(e)

    def test_add_selector_profile(self, obj):
        assert id(
            obj.add_selector_profile(
                execution_site="condor-pool", pfn="/tmp", grid_job_type="compute"
            )
        ) == id(obj)

        assert dict(obj.profiles) == {
            "selector": {
                "execution.site": "condor-pool",
                "pfn": "/tmp",
                "grid.jobtype": "compute",
            }
        }

    def test_add_selector_profile_invalid_profile(self, obj):
        with pytest.raises(TypeError) as e:
            obj.add_selector_profile(aa=1)

        assert "add_selector_profile() got an unexpected" in str(e)


@pytest.mark.parametrize(
    "value, expected",
    [("0", 0), (1, 1), ("1", 1), ("2 MB", 2), ("2 GB", 2048), ("10 GB", 10240)],
)
def test_to_mb(value, expected):
    assert to_mb(value) == expected


@pytest.mark.parametrize("value", [("abc MB"), ("MB"), ("1 KB")])
def test_to_mb_invalid_input(value):
    with pytest.raises(ValueError) as e:
        to_mb(value)

    assert "value: {} should be a str".format(value) in str(e)
