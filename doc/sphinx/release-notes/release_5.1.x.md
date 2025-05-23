## Pegasus 5.1.x Series

### Pegasus 5.1.0

**Release Date:**  May 27, 2025


We are happy to announce the release of Pegasus 5.1.  Pegasus 5.1.0
is  a major release of Pegasus  It also includes all features and
bug fixes from the 5.0 branch. We invite our users to give it a
try.    

The release can be downloaded from:
<https://pegasus.isi.edu/downloads>

If you are an existing user, please carefully follow these
instructions to upgrade at 
<https://pegasus.isi.edu/docs/5.1.0/user-guide/migration.html#migrating-from-pegasus-5-0-x-to-pegasus-5-1> 

#### Highlights of the Release

1) Refined Data Transfer Mechanisms for Containerized Jobs

    PegasusLite now offers two distinct approaches for handling data transfers in  
    containerized jobs. The shift to host-based transfers as the default aims to
    simplify workflows and minimize the overhead associated with customizing container images.

    *   **Host-Based Transfers (Default in 5.1.0):** Input and output data are staged on the 
        host operating system before launching the container. This method utilizes pre-installed
        data transfer tools on the host, reducing the need for additional configurations within 
        the container.
    *   **Container-Based Transfers:** Data transfers occur within the container prior to 
        executing user code. This approach requires the container image to include necessary
        data transfer utilities like curl, ftp, or globus-online. Users preferring this method 
        can set the property pegasus.transfer.container.onhost to false in their configuration files.

    More details can be found in the 
    [documentation](https://pegasus.isi.edu/docs/5.1.0/user-guide/containers.html#data-transfers-for-jobs-when-running-in-container).

   
2) Integration with HTCondor’s Container Universe

    Pegasus 5.1.0 introduces support for HTCondor’s container universe, 
    which is useful in pure HTCondor environments such as PATh/OSPool whereby the container
    management is handled by HTCondor.  This integration simplifies job submission and execution, 
    for environments where HTCondor’s container universe is available.

    This enhancement builds upon Pegasus’s initial container support introduced in 
    version 4.8.0, reflecting ongoing efforts to improve compatibility and user experience.

    More details can be found in the 
    [documentation](https://pegasus.isi.edu/docs/5.1.0/user-guide/containers.html#configuring-workflows-to-use-containers).

3) pegasus-status command line tool was rewritten in python, removing Pegasus perl
   dependency. The new *pegasus-status* command has better support for showing status of
   hierarchical workflows.

4) Improved determination on what site a job runs on. Starting Pegasus 5.1.0 release, PegasusLite 
   wrapped jobs send a location record that enables us to figure out what resource a job runs on.
   The location record can be found toward the end of the job .out file as a pegasus multipart record.
   
   More details can be found in the 
   [documentation](https://pegasus.isi.edu/documentation/reference-guide/funding-citing-usage-stats.html#pegasus-lite-metrics).
 
 
#### New Features and Improvements 

1) update deployment scenarios documentation to include Open OnDemand configuration [\#2112](https://github.com/pegasus-isi/pegasus/issues/2112)
2) Incorporate release notes into the documentation [\#2111](https://github.com/pegasus-isi/pegasus/issues/2111)
3) document use of containers on HPC clusters [\#2110](https://github.com/pegasus-isi/pegasus/issues/2110)
4) update planner worker package staging logic to default to rhel8 for linux and macos\_14 for macos [\#2108](https://github.com/pegasus-isi/pegasus/issues/2108)
5) planner should try and visualize the workflow [\#2099](https://github.com/pegasus-isi/pegasus/issues/2099)
6) CVE-2025-21502 in RHEL 8 project dependency java-11-openjdk [\#2097](https://github.com/pegasus-isi/pegasus/issues/2097)
7) Failures when testing pegasus v5.1.0 release [\#2095](https://github.com/pegasus-isi/pegasus/issues/2095)
8) Improved determination of what resource the job ran on [\#2094](https://github.com/pegasus-isi/pegasus/issues/2094)
9) add support in pegasus-init for submission to remote SLURM cluster via SSH [\#2093](https://github.com/pegasus-isi/pegasus/issues/2093)
10) \[PM-1999\] pick automatically system executables on the local site on the basis of what PATH is set when running the planner [\#2091](https://github.com/pegasus-isi/pegasus/issues/2091)
11) \[PM-1975\] enable bypass staging of container when running in container universe [\#2081](https://github.com/pegasus-isi/pegasus/issues/2081)
12) \[PM-1971\] Update jars that have security vulnerabilities. [\#2077](https://github.com/pegasus-isi/pegasus/issues/2077)
13) \[PM-1970\] Storage constraint test failing [\#2076](https://github.com/pegasus-isi/pegasus/issues/2076)
14) \[PM-1969\] Schema doc should be for YAML and not XML [\#2075](https://github.com/pegasus-isi/pegasus/issues/2075)
15) \[PM-1956\] p-version timestamp issue [\#2069](https://github.com/pegasus-isi/pegasus/issues/2069)
16) \[PM-1950\] enable users to use container universe when running containerized jobs in pure condor environments [\#2063](https://github.com/pegasus-isi/pegasus/issues/2063)
17) \[PM-1942\] Support transfers for a job on the HOST OS instead of from within the container [\#2055](https://github.com/pegasus-isi/pegasus/issues/2055)
18) \[PM-1901\] converting pegasus-analyzer tool to API \(like status\) and add test suite [\#2014](https://github.com/pegasus-isi/pegasus/issues/2014)
19) \[PM-1889\] Escape command line args passed to Job [\#2002](https://github.com/pegasus-isi/pegasus/issues/2002)
20) \[PM-1886\] Recommendations for new Pegasus-Status CLI tool [\#1999](https://github.com/pegasus-isi/pegasus/issues/1999)
21) \[PM-1882\] replace the perl command line client with python client [\#1995](https://github.com/pegasus-isi/pegasus/issues/1995)
22) \[PM-1881\] remove dependency on pegasus-status command line tool , in the Workflow status function [\#1994](https://github.com/pegasus-isi/pegasus/issues/1994)
23) \[PM-1775\] add changes to the Python API to support adding of checkpoint files by pattern [\#1889](https://github.com/pegasus-isi/pegasus/issues/1889)
24) \[PM-1589\] Example workflows repository for 5.0 [\#1703](https://github.com/pegasus-isi/pegasus/issues/1703)
25) \[PM-1974\] update InPlace cleanup algorithm to delete container image from the user submit directory [\#2080](https://github.com/pegasus-isi/pegasus/issues/2080)
26) \[PM-1967\] Kickstart should kill a job gracefully before maxwalltime [\#2073](https://github.com/pegasus-isi/pegasus/issues/2073)
27) \[PM-1955\] Deprecate R API [\#2068](https://github.com/pegasus-isi/pegasus/issues/2068)
28) \[PM-1934\] source builds with multiple python3 installs [\#2047](https://github.com/pegasus-isi/pegasus/issues/2047)
29) \[PM-1915\] Convert pegasus-statistics tool to API and add test suite [\#2028](https://github.com/pegasus-isi/pegasus/issues/2028)
30) \[PM-1914\] update python workflow api to support arm64 [\#2027](https://github.com/pegasus-isi/pegasus/issues/2027)
31) \[PM-1912\] planner should keep in mind units when converting diskspace profiles [\#2025](https://github.com/pegasus-isi/pegasus/issues/2025)
32) \[PM-1860\] aws batch support needs to pick up credentials.conf correctly [\#1973](https://github.com/pegasus-isi/pegasus/issues/1973)
33) \[PM-1819\] 5.0.3 Python API Improvements [\#1932](https://github.com/pegasus-isi/pegasus/issues/1932)
34) \[PM-1801\] sqlalchemy warnings against 5.0 database [\#1915](https://github.com/pegasus-isi/pegasus/issues/1915)
35) \[PM-1793\] refactor pegasus-transfer so that it can be invoked directly from pegasus-checkpoint [\#1907](https://github.com/pegasus-isi/pegasus/issues/1907)
36) \[PM-1782\] incorporate pegasus arm builds into our build infrastructure [\#1896](https://github.com/pegasus-isi/pegasus/issues/1896)
37) \[PM-1781\] pegasus-keg sleep option [\#1895](https://github.com/pegasus-isi/pegasus/issues/1895)
38) \[PM-1756\] paths with spaces need to be escaped [\#1870](https://github.com/pegasus-isi/pegasus/issues/1870)
39) \[PM-1690\] the --json option added in pegasus-plan/run needs to be integrated into the python api client code [\#1804](https://github.com/pegasus-isi/pegasus/issues/1804)

#### Bugs Fixed

1) pegasus aws batch test failing because of urllib3 incompatibility [\#2107](https://github.com/pegasus-isi/pegasus/issues/2107)
2) Planner should catch deep lfn common name problem when using CEDAR [\#2106](https://github.com/pegasus-isi/pegasus/issues/2106)
3) support for condorio deep LFN broke after move to host OS based transfers [\#2105](https://github.com/pegasus-isi/pegasus/issues/2105)
4) when parsing container mount points in the TC, normalize the path to ensure any duplicate / in directory paths are removed [\#2103](https://github.com/pegasus-isi/pegasus/issues/2103)
5) pegasus-graphviz fails on a wf generated with java dax api that has no jobs [\#2101](https://github.com/pegasus-isi/pegasus/issues/2101)
6) \[PM-1954\] Importing six.moves raises ModuleNotFoundError on Python 3.12 [\#2067](https://github.com/pegasus-isi/pegasus/issues/2067)
7) \[PM-1968\] pegasus.gridstart allows values that are not documented [\#2074](https://github.com/pegasus-isi/pegasus/issues/2074)
8) \[PM-1952\] Local universe job fail with pegasus.transfer.bypass.input.staging = true [\#2065](https://github.com/pegasus-isi/pegasus/issues/2065)
9)  \[PM-1924\] API submit still drops debug info to stdout [\#2037](https://github.com/pegasus-isi/pegasus/issues/2037)
10) \[PM-1902\] Pika problem on RHEL 9 - Bump the version to 1.2.1 also [\#2015](https://github.com/pegasus-isi/pegasus/issues/2015)
11)  \[PM-1923\] download form does not send metrics to metric server [\#2036](https://github.com/pegasus-isi/pegasus/issues/2036)


#### Merged pull requests

- PM-1914 gitlab py format fix [\#111](https://github.com/pegasus-isi/pegasus/pull/111) ([zaiyan-alam](https://github.com/zaiyan-alam))
- fixed gitlab lint error [\#110](https://github.com/pegasus-isi/pegasus/pull/110) ([zaiyan-alam](https://github.com/zaiyan-alam))
- PM-1914 Added aarch64 support, added tests, updated reference guide, … [\#109](https://github.com/pegasus-isi/pegasus/pull/109) ([zaiyan-alam](https://github.com/zaiyan-alam))
- PM-1909 added KB conversion for request\_disk, updated unit test [\#107](https://github.com/pegasus-isi/pegasus/pull/107) ([zaiyan-alam](https://github.com/zaiyan-alam))
- PM-1874 bamboo test input dir [\#48](https://github.com/pegasus-isi/pegasus/pull/48) ([zaiyan-alam](https://github.com/zaiyan-alam))
- PM-1874 removing tabs [\#46](https://github.com/pegasus-isi/pegasus/pull/46) ([zaiyan-alam](https://github.com/zaiyan-alam))
- PM-1874 fixing broken bamboo tests [\#45](https://github.com/pegasus-isi/pegasus/pull/45) ([zaiyan-alam](https://github.com/zaiyan-alam))
- PM-1874 test fixes and updates [\#44](https://github.com/pegasus-isi/pegasus/pull/44) ([zaiyan-alam](https://github.com/zaiyan-alam))
- PM-1874 More test fixes and updates [\#43](https://github.com/pegasus-isi/pegasus/pull/43) ([zaiyan-alam](https://github.com/zaiyan-alam))
- test updates and python3 fixes [\#42](https://github.com/pegasus-isi/pegasus/pull/42) ([zaiyan-alam](https://github.com/zaiyan-alam))
- Test updates [\#41](https://github.com/pegasus-isi/pegasus/pull/41) ([zaiyan-alam](https://github.com/zaiyan-alam))
- PM-1874 gsiftp test updates [\#40](https://github.com/pegasus-isi/pegasus/pull/40) ([zaiyan-alam](https://github.com/zaiyan-alam))


