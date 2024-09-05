#!/usr/bin/env python3
import argparse
import configparser
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from Pegasus.api import *

logging.basicConfig(level=logging.DEBUG)


def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description="Runtime Cluster Test Workflow")

    parser.add_argument(
        "pegasus_keg_path",
        help="abs path to pegasus-keg install (e.g '/usr/bin/pegasus-keg')",
        metavar="PEGASUS_KEG_PATH",
    )

    parser.add_argument(
        "config_dir",
        help="name of test config dir (e.g. 'runtime-condorio', 'runtime-nonsharedfs'",
    )

    return parser.parse_args(args)


def write_sc(top_dir: Path, run_id: str):
    # get pegasus version
    cp = subprocess.run(
        ["pegasus-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if cp.returncode != 0:
        raise RuntimeError(
            f"unable to call pegasus-version: {cp.stderr.decode().strip()}"
        )

    REMOTE_PEGASUS_HOME = "/scitech/shared/scratch-90-days/bamboo/installs/pegasus-{}".format(
        cp.stdout.decode().strip()
    )

    sc = SiteCatalog()

    # --- cartman-data site ----------------------------------------------------
    cartman_data = Site(name="cartman-data", arch=Arch.X86_64, os_type=OS.LINUX)
    cartman_data.add_directories(
        Directory(
            Directory.SHARED_SCRATCH,
            top_dir / "staging-site/scratch",
        ).add_file_servers(
            FileServer(
                "webdavs://workflow.isi.edu/webdav/scratch-90-days/" + str(top_dir / "staging-site/scratch"),
                Operation.ALL,
            )
        )
    )
    cartman_data.add_env(PEGASUS_HOME=REMOTE_PEGASUS_HOME)
    sc.add_sites(cartman_data)

    # --- condorpool site ------------------------------------------------------
    condorpool = Site(name="condorpool", arch=Arch.X86_64, os_type=OS.LINUX)
    condorpool.add_condor_profile(universe="vanilla")
    condorpool.add_pegasus_profile(style="condor")
    sc.add_sites(condorpool)

    # --- sharedfs site --------------------------------------------------------
    sharedfs = Site(name="sharedfs", arch=Arch.X86_64, os_type=OS.LINUX)
    sharedfs_dir1 = Directory(
        Directory.SHARED_STORAGE,
        Path("/scitech/shared/scratch-90-days")
        / os.getenv("USER")
        / "storage/black-diamond-output"
        / run_id,
    )
    sharedfs_dir1.add_file_servers(
        FileServer(
            "file://"
            + str(
                Path("/scitech/shared/scratch-90-days")
                / os.getenv("USER")
                / "storage/black-diamond-output"
                / run_id
            ),
            Operation.ALL,
        )
    )
    sharedfs.add_directories(sharedfs_dir1)

    sharedfs_dir2 = Directory(
        Directory.SHARED_SCRATCH,
        Path("/scitech/shared/scratch-90-days") / os.getenv("USER") / "scratch" / run_id,
    )
    sharedfs_dir2.add_file_servers(
        FileServer(
            "file://"
            + str(
                Path("/scitech/shared/scratch-90-days") / os.getenv("USER") / "scratch" / run_id
            ),
            Operation.ALL,
        )
    )
    sharedfs.add_directories(sharedfs_dir2)
    sharedfs.add_env(PEGASUS_HOME=REMOTE_PEGASUS_HOME)
    sharedfs.add_condor_profile(
        should_transfer_files="Yes",
        universe="vanilla",
        when_to_transfer_output="ON_EXIT",
    )
    sharedfs.add_pegasus_profile(style="condor")
    sc.add_sites(sharedfs)

    # --- local site -----------------------------------------------------------
    local_site_url = config.get("all", "local_site_url", fallback="")

    local = Site(name="local", arch=Arch.X86_64, os_type=OS.LINUX)
    local_dir1 = Directory(Directory.SHARED_STORAGE, top_dir / "outputs")
    local_dir1.add_file_servers(
        FileServer(local_site_url + str(top_dir / "outputs"), Operation.ALL)
    )
    local.add_directories(local_dir1)

    local_dir2 = Directory(Directory.SHARED_SCRATCH, top_dir / "work")
    local_dir2.add_file_servers(
        FileServer(local_site_url + str(top_dir / "work"), Operation.ALL)
    )
    local.add_directories(local_dir2)
    
    local.add_env(SSH_PRIVATE_KEY="/scitech/shared/home/bamboo/.ssh/workflow_id_rsa")

    sc.add_sites(local)

    # write
    sc.write()


def write_rc(config: configparser.ConfigParser):
    input_file = config.get("all", "input_file")
    if input_file == "":
        input_file = Path("f.a")
    else:
        # is a directory such as '/scitech/shared/scratch-90-days'
        input_dir = Path(input_file) / os.getenv("USER") / "inputs"
        input_dir.mkdir(parents=True, exist_ok=True)
        input_file = input_dir / "f.a"

    with input_file.open("w") as f:
        f.write("This is sample input to KEG")

    rc = ReplicaCatalog()
    rc.add_replica(
        site=config.get("all", "file_site"), lfn="f.a", pfn=input_file.resolve()
    )
    rc.write()


def write_tc(config: configparser.ConfigParser, pegasus_keg_path: str):
    tc = TransformationCatalog()

    for i in range(1, 3):
        sleep = Transformation(
            namespace="cluster",
            name=f"level{i}",
            version="1.0",
            site=config.get("all", "executable_site"),
            pfn=config.get("all", "executable_url") + pegasus_keg_path,
            is_stageable=True,
            os_type=OS.LINUX,
            arch=Arch.X86_64,
        )

        sleep.add_pegasus_profile(
            clusters_size=config.get("all", "clusters_size"),
            clusters_max_runtime=config.get("all", "clusters_maxruntime"),
        )

        tc.add_transformations(sleep)

    tc.write()


if __name__ == "__main__":
    args = parse_args()

    TOP_DIR = Path().cwd().resolve()
    RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

    # --- validate test config dir ---------------------------------------------
    config_dir = Path(__file__).parent / args.config_dir
    if not config_dir.is_dir():
        raise ValueError(
            f"config_dir: {config_dir} does not a directory or does not exist"
        )

    config_file = config_dir / "test.config"
    if not config_file.is_file():
        raise ValueError("{} does not contain required file: {}".format(config_file))

    # --- general test config --------------------------------------------------
    config = configparser.ConfigParser(
        {
            "input_file": "",
            "workflow_name": "horizontal-clustering-test",
            "clusters_size": "3",
            "clusters_maxruntime": "7",
        }
    )

    config.read(str(config_file))

    # --- catalogs -------------------------------------------------------------
    write_sc(TOP_DIR, RUN_ID)
    write_rc(config)
    write_tc(config, args.pegasus_keg_path)

    # --- workflow -------------------------------------------------------------
    wf = Workflow(config.get("all", "workflow_name"))
    input_file = File("f.a")

    # create 4 lvl1 jobs
    for i in range(4):
        job = (
            Job(namespace="cluster", transformation="level1", version="1.0")
            .add_args("-a", "level1", "-T", i + 1, "-i", input_file)
            .add_inputs(input_file)
            .add_profiles(Namespace.PEGASUS, key="job.runtime", value=i + 1)
        )

        wf.add_jobs(job)

        # for each lvl1 job, create 4 lvl2 children
        for j in range(4):
            child = (
                Job(namespace="cluster", transformation="level2", version="1.0")
                .add_args("-a", "level2", "-T", ((j + 1) * 2))
                .add_profiles(Namespace.PEGASUS, key="runtime", value=((j + 1) * 2))
            )

            wf.add_jobs(child)
            wf.add_dependency(job=job, children=[child])

    # plan and run
    execution_site = config.get("all", "execution_site", fallback="local")
    staging_site = config.get("all", "staging_site", fallback="local")
    output_site = config.get("all", "output_site", fallback="local")

    top_pegasusrc = Path(__file__).parent / "pegasusrc"
    pegasusrc = config_dir / "pegasusrc"

    # include anything in __file__/pegasusrc in ./config_dir/pegasusrc
    with top_pegasusrc.open("r") as top_cfg, pegasusrc.open("a") as cfg:
        cfg.write(top_cfg.read())

    try:
        wf.plan(
            conf=str(pegasusrc),
            sites=[execution_site],
            staging_sites={execution_site: staging_site},
            output_sites=[output_site],
            dir="work/submit",
            cleanup="leaf",
            cluster=["horizontal"],
            verbose=3,
            submit=True,
        ).wait().analyze().statistics()
    except PegasusClientError as e:
        print(e)
        print(e.result.stdout)
