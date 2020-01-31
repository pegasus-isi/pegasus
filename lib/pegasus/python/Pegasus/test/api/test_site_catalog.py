import os
import json
from tempfile import NamedTemporaryFile

import pytest
import yaml
from jsonschema import validate

from Pegasus.dax4.site_catalog import Arch
from Pegasus.dax4.site_catalog import OS
from Pegasus.dax4.site_catalog import Operation
from Pegasus.dax4.site_catalog import FileServer
from Pegasus.dax4.site_catalog import Directory
from Pegasus.dax4.site_catalog import Grid
from Pegasus.dax4.site_catalog import SupportedJobs
from Pegasus.dax4.site_catalog import Scheduler
from Pegasus.dax4.site_catalog import Site
from Pegasus.dax4.site_catalog import SiteCatalog
from Pegasus.dax4.site_catalog import PEGASUS_VERSION
from Pegasus.dax4.mixins import Namespace
from Pegasus.dax4.errors import DuplicateError
from Pegasus.dax4.writable import _CustomEncoder


class TestFileServer:
    def test_valid_file_server(self):
        assert FileServer("url", Operation.PUT)

    def test_invlaid_file_server(self):
        with pytest.raises(TypeError) as e:
            FileServer("url", "put")

        assert "invalid operation_type: put" in str(e)

    def test_tojson_with_profiles(self, convert_yaml_schemas_to_json, load_schema):
        file_server = FileServer("url", Operation.PUT).add_env(SOME_ENV="1")

        result = json.loads(json.dumps(file_server, cls=_CustomEncoder))

        expected = {
            "url": "url",
            "operation": "put",
            "profiles": {"env": {"SOME_ENV": "1"}},
        }

        file_server_schema = load_schema("sc-5.0.json")["$defs"]["fileServer"]
        validate(instance=result, schema=file_server_schema)

        assert result == expected


class TestDirectory:
    def test_valid_directory(self):
        assert Directory(Directory.LOCAL_SCRATCH, "/path")

    def test_invalid_directory(self):
        with pytest.raises(TypeError) as e:
            Directory("invalid type", "/path")

        assert "invalid directory_type: invalid type" in str(e)

    def test_add_valid_file_server(self):
        d = Directory(Directory.LOCAL_SCRATCH, "/path")
        assert d.add_file_server(FileServer("url", Operation.PUT))

    def test_add_invalid_file_server(self):
        with pytest.raises(TypeError) as e:
            d = Directory(Directory.LOCAL_SCRATCH, "/path")
            d.add_file_server(123)

            assert "invalid file_server: 123" in str(e)

    def test_chaining(self):
        a = Directory(Directory.LOCAL_SCRATCH, "/path")
        b = a.add_file_server(FileServer("url", Operation.PUT)).add_file_server(
            FileServer("url", Operation.GET)
        )

        assert id(a) == id(b)

    def test_tojson(self):
        directory = Directory(Directory.LOCAL_SCRATCH, "/path").add_file_server(
            FileServer("url", Operation.PUT)
        )

        result = json.loads(json.dumps(directory, cls=_CustomEncoder))

        expected = {
            "type": "localScratch",
            "path": "/path",
            "fileServers": [{"url": "url", "operation": "put"}],
        }

        assert result == expected


class TestGrid:
    def test_valid_grid(self):
        assert Grid(
            Grid.GT5,
            "smarty.isi.edu/jobmanager-pbs",
            Scheduler.PBS,
            SupportedJobs.AUXILLARY,
        )

    @pytest.mark.parametrize(
        "grid_type, contact, scheduler_type, job_type, invalid_var",
        [
            (
                "badgridtype",
                "contact",
                Scheduler.PBS,
                SupportedJobs.AUXILLARY,
                "grid_type",
            ),
            (
                Grid.PBS,
                "contact",
                "badschedulertype",
                SupportedJobs.AUXILLARY,
                "scheduler_type",
            ),
            (Grid.PBS, "contact", Scheduler.PBS, "badjobtype", "job_type"),
        ],
    )
    def test_invalid_grid(
        self, grid_type, contact, scheduler_type, job_type, invalid_var
    ):
        with pytest.raises(TypeError) as e:
            Grid(
                grid_type, contact, scheduler_type, job_type,
            )

        assert "invalid {invalid_var}: {value}".format(
            invalid_var=invalid_var, value=locals()[invalid_var]
        ) in str(e)

    def test_tojson(self, convert_yaml_schemas_to_json, load_schema):
        grid = Grid(
            Grid.GT5,
            "smarty.isi.edu/jobmanager-pbs",
            Scheduler.PBS,
            SupportedJobs.AUXILLARY,
            free_mem="123",
            total_mem="1230",
            max_count="10",
            max_cpu_time="100",
            running_jobs=10,
            jobs_in_queue=10,
            idle_nodes=1,
            total_nodes=10,
        )

        result = json.loads(json.dumps(grid, cls=_CustomEncoder))

        expected = {
            "type": "gt5",
            "contact": "smarty.isi.edu/jobmanager-pbs",
            "scheduler": "pbs",
            "jobtype": "auxillary",
            "freeMem": "123",
            "totalMem": "1230",
            "maxCount": "10",
            "maxCPUTime": "100",
            "runningJobs": 10,
            "jobsInQueue": 10,
            "idleNodes": 1,
            "totalNodes": 10,
        }

        grid_schema = load_schema("sc-5.0.json")["$defs"]["grid"]
        validate(instance=result, schema=grid_schema)

        assert result == expected


class TestSite:
    def test_valid_site(self):
        assert Site(
            "site",
            arch=Arch.X86_64,
            os_type=OS.LINUX,
            os_release="release",
            os_version="1.1.1",
            glibc="123",
        )

    @pytest.mark.parametrize(
        "name, arch, os_type, invalid_var",
        [
            ("site", "badarch", OS.LINUX, "arch"),
            ("site", Arch.X86_64, "badostype", "os_type"),
        ],
    )
    def test_invalid_site(self, name, arch, os_type, invalid_var):
        with pytest.raises(TypeError) as e:
            Site(name, arch=arch, os_type=os_type)

        assert "invalid {invalid_var}: {value}".format(
            invalid_var=invalid_var, value=locals()[invalid_var]
        ) in str(e)

    def test_add_valid_directory(self):
        site = Site("s")
        site.add_directory(Directory(Directory.LOCAL_SCRATCH, "/path"))
        site.add_directory(Directory(Directory.LOCAL_STORAGE, "/path"))

        assert len(site.directories) == 2

    def test_add_invalid_directory(self):
        with pytest.raises(TypeError) as e:
            site = Site("s")
            site.add_directory("baddirectory")

        assert "invalid directory: baddirectory" in str(e)

    def test_add_valid_grid(self):
        site = Site("s")
        site.add_grid(
            Grid(
                Grid.GT5,
                "smarty.isi.edu/jobmanager-pbs",
                Scheduler.PBS,
                job_type=SupportedJobs.AUXILLARY,
            )
        )
        site.add_grid(
            Grid(
                Grid.GT5,
                "smarty.isi.edu/jobmanager-pbs",
                Scheduler.PBS,
                job_type=SupportedJobs.COMPUTE,
            )
        )

        assert len(site.grids) == 2

    def test_add_invalid_grid(self):
        with pytest.raises(TypeError) as e:
            site = Site("s")
            site.add_grid("badgrid")

        assert "invalid grid: badgrid" in str(e)

    def test_chaining(self):
        site = Site("s")
        a = site.add_directory(Directory(Directory.LOCAL_SCRATCH, "/path"))
        b = site.add_grid(
            Grid(
                Grid.GT5,
                "smarty.isi.edu/jobmanager-pbs",
                Scheduler.PBS,
                job_type=SupportedJobs.AUXILLARY,
            )
        )

        assert id(a) == id(b)

    def test_tojson_with_profiles(self):
        site = Site(
            "s",
            arch=Arch.X86_64,
            os_type=OS.LINUX,
            os_release="release",
            os_version="1.2.3",
            glibc="1",
        )
        site.add_directory(
            Directory(Directory.LOCAL_SCRATCH, "/path").add_file_server(
                FileServer("url", Operation.GET)
            )
        )
        site.add_grid(
            Grid(
                Grid.GT5,
                "smarty.isi.edu/jobmanager-pbs",
                Scheduler.PBS,
                job_type=SupportedJobs.AUXILLARY,
            )
        )
        site.add_env(JAVA_HOME="/usr/bin/java")

        result = json.loads(json.dumps(site, cls=_CustomEncoder))

        expected = {
            "name": "s",
            "arch": "x86_64",
            "os.type": "linux",
            "os.release": "release",
            "os.version": "1.2.3",
            "glibc": "1",
            "directories": [
                {
                    "type": "localScratch",
                    "path": "/path",
                    "fileServers": [{"url": "url", "operation": "get"}],
                }
            ],
            "grids": [
                {
                    "type": "gt5",
                    "contact": "smarty.isi.edu/jobmanager-pbs",
                    "scheduler": "pbs",
                    "jobtype": "auxillary",
                }
            ],
            "profiles": {"env": {"JAVA_HOME": "/usr/bin/java"}},
        }

        assert result == expected


@pytest.fixture
def expected_json():
    return {
        "pegasus": PEGASUS_VERSION,
        "sites": [
            {
                "name": "local",
                "arch": "x86_64",
                "os.type": "linux",
                "directories": [
                    {
                        "type": "sharedScratch",
                        "path": "/tmp/workflows/scratch",
                        "fileServers": [
                            {
                                "url": "file:///tmp/workflows/scratch",
                                "operation": "all",
                            }
                        ],
                    },
                    {
                        "type": "localStorage",
                        "path": "/tmp/workflows/outputs",
                        "fileServers": [
                            {
                                "url": "file:///tmp/workflows/outputs",
                                "operation": "all",
                            }
                        ],
                    },
                ],
            },
            {
                "name": "condor_pool",
                "arch": "x86_64",
                "os.type": "linux",
                "directories": [
                    {
                        "type": "sharedScratch",
                        "path": "/lustre",
                        "fileServers": [
                            {
                                "url": "gsiftp://smarty.isi.edu/lustre",
                                "operation": "all",
                            }
                        ],
                    }
                ],
                "grids": [
                    {
                        "type": "gt5",
                        "contact": "smarty.isi.edu/jobmanager-pbs",
                        "scheduler": "pbs",
                        "jobtype": "auxillary",
                    },
                    {
                        "type": "gt5",
                        "contact": "smarty.isi.edu/jobmanager-pbs",
                        "scheduler": "pbs",
                        "jobtype": "compute",
                    },
                ],
                "profiles": {"env": {"JAVA_HOME": "/usr/bin/java"}},
            },
            {
                "name": "staging_site",
                "arch": "x86_64",
                "os.type": "linux",
                "directories": [
                    {
                        "type": "sharedScratch",
                        "path": "/data",
                        "fileServers": [
                            {"url": "scp://obelix.isi.edu/data", "operation": "put",},
                            {"url": "http://obelix.isi.edu/data", "operation": "get",},
                        ],
                    }
                ],
            },
        ],
    }


class TestSiteCatalog:
    def test_add_valid_site(self):
        sc = SiteCatalog()
        assert sc.add_site(Site("local"))

    def test_add_invalid_site(self):
        with pytest.raises(TypeError) as e:
            sc = SiteCatalog()
            sc.add_site("badsite")

        assert "invalid site: badsite" in str(e)

    def test_add_duplicate_site(self):
        sc = SiteCatalog()
        sc.add_site(Site("local"))
        with pytest.raises(DuplicateError):
            sc.add_site(Site("local"))

    def test_chaining(self):
        sc = SiteCatalog()
        a = sc.add_site(Site("local"))
        b = sc.add_site(Site("condor_pool"))

        assert id(a) == id(b)

    def test_tojson(self, convert_yaml_schemas_to_json, load_schema, expected_json):
        sc = (
            SiteCatalog()
            .add_site(
                Site("local", arch=Arch.X86_64, os_type=OS.LINUX)
                .add_directory(
                    Directory(
                        Directory.SHARED_SCRATCH, "/tmp/workflows/scratch"
                    ).add_file_server(
                        FileServer("file:///tmp/workflows/scratch", Operation.ALL)
                    )
                )
                .add_directory(
                    Directory(
                        Directory.LOCAL_STORAGE, "/tmp/workflows/outputs"
                    ).add_file_server(
                        FileServer("file:///tmp/workflows/outputs", Operation.ALL)
                    )
                )
            )
            .add_site(
                Site("condor_pool", arch=Arch.X86_64, os_type=OS.LINUX)
                .add_directory(
                    Directory(Directory.SHARED_SCRATCH, "/lustre").add_file_server(
                        FileServer("gsiftp://smarty.isi.edu/lustre", Operation.ALL)
                    )
                )
                .add_grid(
                    Grid(
                        Grid.GT5,
                        "smarty.isi.edu/jobmanager-pbs",
                        Scheduler.PBS,
                        job_type=SupportedJobs.AUXILLARY,
                    )
                )
                .add_grid(
                    Grid(
                        Grid.GT5,
                        "smarty.isi.edu/jobmanager-pbs",
                        Scheduler.PBS,
                        job_type=SupportedJobs.COMPUTE,
                    )
                )
                .add_env(JAVA_HOME="/usr/bin/java")
            )
            .add_site(
                Site("staging_site", arch=Arch.X86_64, os_type=OS.LINUX).add_directory(
                    Directory(Directory.SHARED_SCRATCH, "/data")
                    .add_file_server(
                        FileServer("scp://obelix.isi.edu/data", Operation.PUT)
                    )
                    .add_file_server(
                        FileServer("http://obelix.isi.edu/data", Operation.GET)
                    )
                )
            )
        )

        result = json.loads(json.dumps(sc, cls=_CustomEncoder))

        sc_schema = load_schema("sc-5.0.json")
        validate(instance=result, schema=sc_schema)

        assert result == expected_json

    @pytest.mark.parametrize(
        "_format, loader", [("json", json.load), ("yml", yaml.safe_load)]
    )
    def test_write(self, expected_json, _format, loader):
        sc = (
            SiteCatalog()
            .add_site(
                Site("local", arch=Arch.X86_64, os_type=OS.LINUX)
                .add_directory(
                    Directory(
                        Directory.SHARED_SCRATCH, "/tmp/workflows/scratch"
                    ).add_file_server(
                        FileServer("file:///tmp/workflows/scratch", Operation.ALL)
                    )
                )
                .add_directory(
                    Directory(
                        Directory.LOCAL_STORAGE, "/tmp/workflows/outputs"
                    ).add_file_server(
                        FileServer("file:///tmp/workflows/outputs", Operation.ALL)
                    )
                )
            )
            .add_site(
                Site("condor_pool", arch=Arch.X86_64, os_type=OS.LINUX)
                .add_directory(
                    Directory(Directory.SHARED_SCRATCH, "/lustre").add_file_server(
                        FileServer("gsiftp://smarty.isi.edu/lustre", Operation.ALL)
                    )
                )
                .add_grid(
                    Grid(
                        Grid.GT5,
                        "smarty.isi.edu/jobmanager-pbs",
                        Scheduler.PBS,
                        job_type=SupportedJobs.AUXILLARY,
                    )
                )
                .add_grid(
                    Grid(
                        Grid.GT5,
                        "smarty.isi.edu/jobmanager-pbs",
                        Scheduler.PBS,
                        job_type=SupportedJobs.COMPUTE,
                    )
                )
                .add_env(JAVA_HOME="/usr/bin/java")
            )
            .add_site(
                Site("staging_site", arch=Arch.X86_64, os_type=OS.LINUX).add_directory(
                    Directory(Directory.SHARED_SCRATCH, "/data")
                    .add_file_server(
                        FileServer("scp://obelix.isi.edu/data", Operation.PUT)
                    )
                    .add_file_server(
                        FileServer("http://obelix.isi.edu/data", Operation.GET)
                    )
                )
            )
        )

        with NamedTemporaryFile(mode="r+") as f:
            sc.write(f, _format=_format)
            f.seek(0)
            result = loader(f)

        assert result == expected_json

