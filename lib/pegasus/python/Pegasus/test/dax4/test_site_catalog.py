import os
import json

import pytest
from jsonschema import validate

from Pegasus.dax4.site_catalog import Arch
from Pegasus.dax4.site_catalog import OSType
from Pegasus.dax4.site_catalog import OperationType
from Pegasus.dax4.site_catalog import FileServer
from Pegasus.dax4.site_catalog import DirectoryType
from Pegasus.dax4.site_catalog import Directory
from Pegasus.dax4.site_catalog import GridType
from Pegasus.dax4.site_catalog import Grid
from Pegasus.dax4.site_catalog import SchedulerType
from Pegasus.dax4.site_catalog import JobType
from Pegasus.dax4.site_catalog import Site
from Pegasus.dax4.site_catalog import SiteCatalog
from Pegasus.dax4.site_catalog import PEGASUS_VERSION
from Pegasus.dax4.mixins import Namespace
from Pegasus.dax4.errors import DuplicateError
from Pegasus.dax4.writable import FileFormat


class TestFileServer:
    def test_valid_file_server(self):
        FileServer("url", OperationType.PUT)

    def test_invlaid_file_server(self):
        with pytest.raises(ValueError):
            FileServer("url", "put")

    def test_tojson_with_profiles(self, convert_yaml_schemas_to_json, load_schema):
        result = (
            FileServer("url", OperationType.PUT)
            .add_profile(Namespace.ENV, "key", "value")
            .__json__()
        )

        expected = {
            "url": "url",
            "operation": "put",
            "profiles": {"env": {"key": "value"}},
        }

        file_server_schema = load_schema("sc-5.0.json")["$defs"]["fileServer"]
        validate(instance=result, schema=file_server_schema)

        assert result == expected


class TestDirectory:
    def test_valid_directory(self):
        Directory(DirectoryType.LOCAL_SCRATCH, "/path")

    def test_invalid_directory(self):
        with pytest.raises(ValueError):
            Directory("invalid type", "/path")

    def test_add_valid_file_server(self):
        d = Directory(DirectoryType.LOCAL_SCRATCH, "/path")
        d.add_file_server(FileServer("url", OperationType.PUT))

    def test_add_invalid_file_server(self):
        d = Directory(DirectoryType.LOCAL_SCRATCH, "/path")
        with pytest.raises(ValueError):
            d.add_file_server(123)

    def test_chaining(self):
        a = Directory(DirectoryType.LOCAL_SCRATCH, "/path")
        b = a.add_file_server(FileServer("url", OperationType.PUT)).add_file_server(
            FileServer("url", OperationType.GET)
        )

        assert id(a) == id(b)

    def test_tojson(self):
        result = (
            Directory(DirectoryType.LOCAL_SCRATCH, "/path")
            .add_file_server(FileServer("url", OperationType.PUT))
            .__json__()
        )

        expected = {
            "type": "localScratch",
            "path": "/path",
            "fileServers": [FileServer("url", OperationType.PUT).__json__()],
        }

        assert result == expected


class TestGrid:
    def test_valid_grid(self):
        Grid(
            GridType.GT5,
            "smarty.isi.edu/jobmanager-pbs",
            SchedulerType.PBS,
            JobType.AUXILLARY,
        )

    @pytest.mark.parametrize(
        "grid_type, contact, scheduler_type, job_type",
        [
            ("badgridtype", "contact", SchedulerType.PBS, JobType.AUXILLARY),
            (GridType.PBS, "contact", "badschedulertype", JobType.AUXILLARY),
            (GridType.PBS, "contact", SchedulerType.PBS, "badjobtype"),
        ],
    )
    def test_invalid_grid(self, grid_type, contact, scheduler_type, job_type):
        with pytest.raises(ValueError):
            Grid(
                grid_type, contact, scheduler_type, job_type,
            )

    def test_tojson(self, convert_yaml_schemas_to_json, load_schema):
        result = Grid(
            GridType.GT5,
            "smarty.isi.edu/jobmanager-pbs",
            SchedulerType.PBS,
            JobType.AUXILLARY,
            free_mem="123",
            total_mem="1230",
            max_count="10",
            max_cpu_time="100",
            running_jobs=10,
            jobs_in_queue=10,
            idle_nodes=1,
            total_nodes=10,
        ).__json__()

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
        Site(
            "site",
            arch=Arch.X86_64,
            os_type=OSType.LINUX,
            os_release="release",
            os_version="1.1.1",
            glibc="123",
        )

    @pytest.mark.parametrize(
        "name, arch, os_type",
        [("site", "badarch", OSType.LINUX), ("site", Arch.X86_64, "badostype")],
    )
    def test_invalid_site(self, name, arch, os_type):
        with pytest.raises(ValueError):
            Site(name, arch=arch, os_type=os_type)

    def test_add_valid_directory(self):
        site = Site("s")
        site.add_directory(Directory(DirectoryType.LOCAL_SCRATCH, "/path"))
        site.add_directory(Directory(DirectoryType.LOCAL_STORAGE, "/path"))

        assert len(site.directories) == 2

    def test_add_invalid_directory(self):
        site = Site("s")
        with pytest.raises(ValueError):
            site.add_directory("baddirectory")

    def test_add_valid_grid(self):
        site = Site("s")
        site.add_grid(
            Grid(
                GridType.GT5,
                "smarty.isi.edu/jobmanager-pbs",
                SchedulerType.PBS,
                job_type=JobType.AUXILLARY,
            )
        )
        site.add_grid(
            Grid(
                GridType.GT5,
                "smarty.isi.edu/jobmanager-pbs",
                SchedulerType.PBS,
                job_type=JobType.COMPUTE,
            )
        )

        assert len(site.grids) == 2

    def test_add_invalid_grid(self):
        site = Site("s")
        with pytest.raises(ValueError):
            site.add_grid("badgrid")

    def test_chaining(self):
        site = Site("s")
        a = site.add_directory(Directory(DirectoryType.LOCAL_SCRATCH, "/path"))
        b = site.add_grid(
            Grid(
                GridType.GT5,
                "smarty.isi.edu/jobmanager-pbs",
                SchedulerType.PBS,
                job_type=JobType.AUXILLARY,
            )
        )

        assert id(a) == id(b)

    def test_tojson_with_profiles(self):
        site = Site(
            "s",
            arch=Arch.X86_64,
            os_type=OSType.LINUX,
            os_release="release",
            os_version="1.2.3",
            glibc="1",
        )
        site.add_directory(Directory(DirectoryType.LOCAL_SCRATCH, "/path"))
        site.add_grid(
            Grid(
                GridType.GT5,
                "smarty.isi.edu/jobmanager-pbs",
                SchedulerType.PBS,
                job_type=JobType.AUXILLARY,
            )
        )
        site.add_profile(Namespace.ENV, "JAVA_HOME", "/usr/bin/java")

        result = site.__json__()

        expected = {
            "name": "s",
            "arch": "x86_64",
            "os.type": "linux",
            "os.release": "release",
            "os.version": "1.2.3",
            "glibc": "1",
            "directories": [d.__json__() for d in site.directories],
            "grids": [g.__json__() for g in site.grids],
            "profiles": {"env": {"JAVA_HOME": "/usr/bin/java"}},
        }

        assert result == expected


class TestSiteCatalog:
    def test_add_valid_site(self):
        sc = SiteCatalog()
        sc.add_site(Site("local"))

    def test_add_invalid_site(self):
        sc = SiteCatalog()
        with pytest.raises(ValueError):
            sc.add_site("badsite")

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

    def test_tojson(self, convert_yaml_schemas_to_json, load_schema):
        sc = (
            SiteCatalog()
            .add_site(
                Site("local", arch=Arch.X86_64, os_type=OSType.LINUX)
                .add_directory(
                    Directory(
                        DirectoryType.SHARED_SCRATCH, "/tmp/workflows/scratch"
                    ).add_file_server(
                        FileServer("file:///tmp/workflows/scratch", OperationType.ALL)
                    )
                )
                .add_directory(
                    Directory(
                        DirectoryType.LOCAL_STORAGE, "/tmp/workflows/outputs"
                    ).add_file_server(
                        FileServer("file:///tmp/workflows/outputs", OperationType.ALL)
                    )
                )
            )
            .add_site(
                Site("condor_pool", arch=Arch.X86_64, os_type=OSType.LINUX)
                .add_directory(
                    Directory(DirectoryType.SHARED_SCRATCH, "/lustre").add_file_server(
                        FileServer("gsiftp://smarty.isi.edu/lustre", OperationType.ALL)
                    )
                )
                .add_grid(
                    Grid(
                        GridType.GT5,
                        "smarty.isi.edu/jobmanager-pbs",
                        SchedulerType.PBS,
                        job_type=JobType.AUXILLARY,
                    )
                )
                .add_grid(
                    Grid(
                        GridType.GT5,
                        "smarty.isi.edu/jobmanager-pbs",
                        SchedulerType.PBS,
                        job_type=JobType.COMPUTE,
                    )
                )
                .add_profile(Namespace.ENV, "JAVA_HOME", "/usr/bin/javap")
            )
            .add_site(
                Site(
                    "staging_site", arch=Arch.X86_64, os_type=OSType.LINUX
                ).add_directory(
                    Directory(DirectoryType.SHARED_SCRATCH, "/data")
                    .add_file_server(
                        FileServer("scp://obelix.isi.edu/data", OperationType.PUT)
                    )
                    .add_file_server(
                        FileServer("http://obelix.isi.edu/data", OperationType.GET)
                    )
                )
            )
        )

        result = sc.__json__()

        expected = {
            "pegasus": PEGASUS_VERSION,
            "sites": [s.__json__() for _, s in sc.sites.items()],
        }

        sc_schema = load_schema("sc-5.0.json")
        validate(instance=result, schema=sc_schema)

        assert result == expected

    def test_write(self):
        sc = (
            SiteCatalog()
            .add_site(
                Site("local", arch=Arch.X86_64, os_type=OSType.LINUX)
                .add_directory(
                    Directory(
                        DirectoryType.SHARED_SCRATCH, "/tmp/workflows/scratch"
                    ).add_file_server(
                        FileServer("file:///tmp/workflows/scratch", OperationType.ALL)
                    )
                )
                .add_directory(
                    Directory(
                        DirectoryType.LOCAL_STORAGE, "/tmp/workflows/outputs"
                    ).add_file_server(
                        FileServer("file:///tmp/workflows/outputs", OperationType.ALL)
                    )
                )
            )
            .add_site(
                Site("condor_pool", arch=Arch.X86_64, os_type=OSType.LINUX)
                .add_directory(
                    Directory(DirectoryType.SHARED_SCRATCH, "/lustre").add_file_server(
                        FileServer("gsiftp://smarty.isi.edu/lustre", OperationType.ALL)
                    )
                )
                .add_grid(
                    Grid(
                        GridType.GT5,
                        "smarty.isi.edu/jobmanager-pbs",
                        SchedulerType.PBS,
                        job_type=JobType.AUXILLARY,
                    )
                )
                .add_grid(
                    Grid(
                        GridType.GT5,
                        "smarty.isi.edu/jobmanager-pbs",
                        SchedulerType.PBS,
                        job_type=JobType.COMPUTE,
                    )
                )
                .add_profile(Namespace.ENV, "JAVA_HOME", "/usr/bin/javap")
            )
            .add_site(
                Site(
                    "staging_site", arch=Arch.X86_64, os_type=OSType.LINUX
                ).add_directory(
                    Directory(DirectoryType.SHARED_SCRATCH, "/data")
                    .add_file_server(
                        FileServer("scp://obelix.isi.edu/data", OperationType.PUT)
                    )
                    .add_file_server(
                        FileServer("http://obelix.isi.edu/data", OperationType.GET)
                    )
                )
            )
        )

        expected = {
            "pegasus": PEGASUS_VERSION,
            "sites": [s.__json__() for _, s in sc.sites.items()],
        }

        test_output_filename = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "SiteCatalogTestOutput.json"
        )

        sc.write(non_default_filepath=test_output_filename, file_format=FileFormat.JSON)

        with open(test_output_filename, "r") as f:
            result = json.load(f)

        assert result == expected

        # cleanup
        os.remove(test_output_filename)
