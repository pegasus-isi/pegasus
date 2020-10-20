#!/usr/bin/env python3

import os
from enum import Enum, unique
from Pegasus.api import *

from Pegasus.api.site_catalog import SupportedJobs

@unique
class SitesAvailable(Enum):
    LOCAL = 1
    USC_HPCC = 2
    OSG_ISI = 3
    XSEDE_BOSCO = 4
    BLUEWATERS_GLITE = 5
    WRANGLER_GLITE = 6
    SUMMIT_GLITE = 7
    SUMMIT_KUBERNETES = 8


SitesRequireProject = [ SitesAvailable.BLUEWATERS_GLITE, 
                        SitesAvailable.WRANGLER_GLITE, 
                        SitesAvailable.SUMMIT_GLITE, 
                        SitesAvailable.SUMMIT_KUBERNETES ]


class MySite():
    def __init__(self, scratch_parent_dir, storage_parent_dir, target_site:SitesAvailable, project=""):
        self.shared_scratch_parent_dir = scratch_parent_dir
        self.local_storage_parent_dir = storage_parent_dir

        self.sc = SiteCatalog()

        local = Site("local")\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, "scratch"))
                            .add_file_servers(FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, "scratch"), Operation.ALL)),

                        Directory(Directory.LOCAL_STORAGE, os.path.join(self.local_storage_parent_dir, "storage"))
                            .add_file_servers(FileServer("file://" + os.path.join(self.local_storage_parent_dir, "storage"), Operation.ALL))
                    )

        self.sc.add_sites(local)
        
        if target_site is SitesAvailable.LOCAL:
            self.exec_site_name = "condorpool"
            self.condorpool()
        elif target_site is SitesAvailable.USC_HPCC:
            self.exec_site_name = "usc-hpcc"
            self.usc_hpcc()
        elif target_site is SitesAvailable.OSG_ISI:
            self.exec_site_name = "osg-isi"
            self.osg_isi()
        elif target_site is SitesAvailable.XSEDE_BOSCO:
            self.exec_site_name = "xsede"
            self.condorpool()
        elif target_site is SitesAvailable.BLUEWATERS_GLITE:
            self.exec_site_name = "bluewaters"
            self.bluewaters_glite(project)
        elif target_site is SitesAvailable.WRANGLER_GLITE:
            self.exec_site_name = "wrangler"
            self.wrangler_glite(project)
        elif target_site is SitesAvailable.SUMMIT_GLITE:
            self.exec_site_name = "summit"
            self.summit_glite(project)
        elif target_site is SitesAvailable.SUMMIT_KUBERNETES:
            self.exec_site_name = "summit"
            self.summit_kubernetes(project)


    def write(self):
        self.sc.write()


    def condorpool(self):
        condorpool = Site(self.exec_site_name)\
                        .add_pegasus_profile(style="condor")\
                        .add_condor_profile(universe="vanilla")\
                        .add_pegasus_profile(data_configuration="condorio")\
                        .add_condor_profile(periodic_remove="(JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > 10)")\
                        .add_pegasus_profile(clusters_num=2)

        self.sc.add_sites(condorpool)


    def usc_hpcc(self):
        usc = Site(self.exec_site_name)\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
                            .add_file_servers(FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"), Operation.ALL))
                    )\
                    .add_pegasus_profile(style="glite")\
                    .add_condor_profile(grid_resource="batch slurm")\
                    .add_pegasus_profile(queue="quick")\
                    .add_pegasus_profile(data_configuration="sharedfs")\
                    .add_pegasus_profile(auxillary_local=True)\
                    .add_pegasus_profile(clusters_num=2)\
                    .add_pegasus_profile(job_aggregator="mpiexec")\
                    .add_env(key="PEGASUS_HOME", value="/home/rcf-proj/gmj/pegasus/SOFTWARE/pegasus/default")
        
        self.sc.add_sites(usc)


    def osg_isi(self):
        osg = Site(self.exec_site_name)\
                .add_pegasus_profile(style="condor")\
                .add_condor_profile(universe="vanilla")\
                .add_pegasus_profile(data_configuration="condorio")\
                .add_condor_profile(periodic_remove="(JobStatus == 5) && ((CurrentTime - EnteredCurrentStatus) > 10)")\
                .add_condor_profile(requirements="OSGVO_OS_STRING == \"RHEL 6\" && Arch == \"X86_64\" &&  HAS_MODULES == True")\
                .add_profiles(Namespace.CONDOR, key="+ProjectName", value="PegasusTraining")\
                .add_pegasus_profile(clusters_num=2)

        self.sc.add_sites(osg)


    def bluewaters_glite(self, project):
        bluewaters = Site(self.exec_site_name)\
                        .add_directories(
                            Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
                                .add_file_servers(FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"), Operation.ALL))
                        )\
                        .add_pegasus_profile(style="glite")\
                        .add_pegasus_profile(queue="normal")\
                        .add_condor_profile(grid_resource="batch pbs")\
                        .add_pegasus_profile(data_configuration="sharedfs")\
                        .add_pegasus_profile(auxillary_local="true")\
                        .add_pegasus_profile(cores=1)\
                        .add_pegasus_profile(ppn=1)\
                        .add_pegasus_profile(project=project)\
                        .add_pegasus_profile(clusters_num=2)\
                        .add_env(key="PEGASUS_HOME", value="/mnt/a/u/training/instr006/SOFTWARE/install/pegasus/default")
        
        self.sc.add_sites(bluewaters)


    def wrangler_glite(self, project):
        wrangler = Site(self.exec_site_name)\
                        .add_directories(
                            Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
                                .add_file_servers(FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"), Operation.ALL))
                        )\
                        .add_pegasus_profile(style="glite")\
                        .add_pegasus_profile(queue="normal")\
                        .add_condor_profile(grid_resource="batch slurm")\
                        .add_pegasus_profile(data_configuration="sharedfs")\
                        .add_pegasus_profile(auxillary_local="true")\
                        .add_pegasus_profile(cores=1)\
                        .add_pegasus_profile(nodes=1)\
                        .add_pegasus_profile(project=project)\
                        .add_pegasus_profile(job_aggregator="mpiexec")\
                        .add_pegasus_profile(runtime=14400)\
                        .add_pegasus_profile(clusters_num=2)\
                        .add_env(key="PEGASUS_HOME", value="/home/00340/vahik/SOFTWARE/install/pegasus/default")
        
        self.sc.add_sites(wrangler)
        

    def summit_glite(self, project):
        summit = Site(self.exec_site_name)\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
                            .add_file_servers(FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"), Operation.ALL))
                    )\
                    .add_pegasus_profile(style="glite")\
                    .add_pegasus_profile(queue="batch")\
                    .add_condor_profile(grid_resource="batch lsf")\
                    .add_pegasus_profile(data_configuration="sharedfs")\
                    .add_pegasus_profile(auxillary_local="true")\
                    .add_pegasus_profile(nodes=1)\
                    .add_pegasus_profile(project=project)\
                    .add_pegasus_profile(job_aggregator="mpiexec")\
                    .add_pegasus_profile(runtime=1800)\
                    .add_pegasus_profile(clusters_num=2)\
                    .add_env(key="PEGASUS_HOME", value="/ccs/proj/csc355/summit/pegasus/stable")
        
        self.sc.add_sites(summit)
                        

    def summit_kubernetes(self, project):
        summit = Site(self.exec_site_name)\
                    .add_grids(
                        Grid(grid_type=Grid.BATCH, scheduler_type=Scheduler.LSF, contact="${USER}@dtn.ccs.ornl.gov", job_type=SupportedJobs.COMPUTE),
                        Grid(grid_type=Grid.BATCH, scheduler_type=Scheduler.LSF, contact="${USER}@dtn.ccs.ornl.gov", job_type=SupportedJobs.AUXILLARY)
                    )\
                    .add_directories(
                        Directory(Directory.SHARED_SCRATCH, os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"))
                            .add_file_servers(FileServer("file://" + os.path.join(self.shared_scratch_parent_dir, self.exec_site_name, "scratch"), Operation.ALL))
                    )\
                    .add_pegasus_profile(style="ssh")\
                    .add_pegasus_profile(queue="batch")\
                    .add_pegasus_profile(auxillary_local="true")\
                    .add_pegasus_profile(change_dir="true")\
                    .add_pegasus_profile(nodes=1)\
                    .add_pegasus_profile(project=project)\
                    .add_pegasus_profile(job_aggregator="mpiexec")\
                    .add_pegasus_profile(runtime=1800)\
                    .add_pegasus_profile(clusters_num=2)\
                    .add_env(key="PEGASUS_HOME", value="/ccs/proj/csc355/summit/pegasus/stable")
        
        self.sc.add_sites(summit)
                        

