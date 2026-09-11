## Pegasus 5.1.x Series

### Pegasus 5.1.3

**Release Date:** September 12th, 2026

We are happy to announce the release of Pegasus 5.1.3. It is a minor release in the 5.1 branch.
We invite our users to give it a try.

The release can be downloaded from:
<https://pegasus.isi.edu/downloads>


#### Highlights of the Release

1) Support for NERSC Superfacility API

   This release of Pegasus has support for NERSC Superfacility API, enabling scientists to
   submit and manage workflows on the Perlmutter supercomputer directly from their submit host
   — no SSH tunnels, no batch system logins required.
   Futher details about generating SFAPI tokens and configuring Pegasus can be found in the
   [documentation](https://pegasus.isi.edu/docs/5.1.3/user-guide/deployment-scenarios.html#nersc-perlmutter-via-sfapi)

   The training notebooks for [ACCESS Pegasus](https://github.com/pegasus-isi/ACCESS-Pegasus-Examples)
   also have been updated to be able to submit to NERSC from the
   [ACCESS Pegasus endpoint](https://pegasus.access-ci.org/)

2) Hosted Site Catalogs

   With 5.1.3 release, users now have an option to get the planner to download the site catalog from a
   [GitHub repository](https://pegasus.isi.edu/docs/5.1.3/reference-guide/catalogs.html#centrally-hosted-site-catalogs)
   that hosts catalog entries for the various compute infrastructures.

   To specify the file to download from the GitHub repository, you have to set the following property,
   and have Pegasus plan for site named `compute`.

   pegasus.catalog.site.repo.file=\<basename of the file to download\>

   Full details on this can be found [here.](https://pegasus.isi.edu/docs/5.1.3/reference-guide/catalogs.html#centrally-hosted-site-catalogs)

3) Job Tagging

   [Job tagging](https://pegasus.isi.edu/docs/5.1.3/reference-guide/catalogs.html#job-tagging)
   is a new feature that allows users to associate `tags` with their jobs in the
   inout abstract worklow. The planner uses to look up and overlay additional profiles from the
   Site Catalog. This allows you to associate different set of profiles for jobs running at
   the same site. For e.g. when running against a HPC cluster, this can be used to set
   different profiles depending on whether the job is tagged CPU or GPU.

4) Profile Expressions

   Pegasus supports profile expressions — Python expressions that are evaluated when a
   job fails and is about to be retried. These expressions allow you to dynamically
   change resource requirements (memory, cores, GPUs, runtime, queue, project, etc.)
   based on the actual execution metrics collected from the previous attempt,
   enabling smarter retry strategies without manual intervention.

   Full details on this can be found [here.](https://pegasus.isi.edu/docs/5.1.3/reference-guide/profile-expressions.html)

#### New Features and Improvements

1) Create a Streaming based YAML DAXParser [\#2243](https://github.com/pegasus-isi/pegasus/issues/2243)
2) Exit with a specific exitcode in case of plite failure [\#2225](https://github.com/pegasus-isi/pegasus/issues/2225)
3) when running in a condor style, container universe should only be associated with a job if the Pegasus job has a container associated with it [\#2224](https://github.com/pegasus-isi/pegasus/issues/2224)
4) Streamline Pegasus properties loading [\#2223](https://github.com/pegasus-isi/pegasus/issues/2223)
5) add a load function to  python api Properties class [\#2210](https://github.com/pegasus-isi/pegasus/issues/2210)
6) allow for variable expansion to happen via properties file [\#2207](https://github.com/pegasus-isi/pegasus/issues/2207)
7) allow jobs to have tags that then map to set of profiles in the site entry for a job [\#2192](https://github.com/pegasus-isi/pegasus/issues/2192)
8) SFAPI support in Pegasus and HTCondor [\#2186](https://github.com/pegasus-isi/pegasus/issues/2186)
9) Use pegasus classad variables to define values of request\_\* keys in condor submit files [\#2174](https://github.com/pegasus-isi/pegasus/issues/2174)
10) composite event to include information about resources requested in a job [\#2171](https://github.com/pegasus-isi/pegasus/issues/2171)
11) update default site logic in the planner [\#2164](https://github.com/pegasus-isi/pegasus/issues/2164)
12) \[PM-1973\] Catalogs copied in submit dir should have their variables substituted [\#2079](https://github.com/pegasus-isi/pegasus/issues/2079)
13) \[PM-1808\] Provide a way to specify site independent transformations [\#1921](https://github.com/pegasus-isi/pegasus/issues/1921)
14) \[PM-1749\] provide an interface for user defined triggers [\#1863](https://github.com/pegasus-isi/pegasus/issues/1863)
15) \[PM-1677\] add status command to see running triggers [\#1791](https://github.com/pegasus-isi/pegasus/issues/1791)
16) \[PM-1653\] Set when\_to\_transfer\_output = ON\_SUCCESS [\#1767](https://github.com/pegasus-isi/pegasus/issues/1767)
17) \[PM-1536\] pegasus-rc-converter to take in new yaml textual format [\#1650](https://github.com/pegasus-isi/pegasus/issues/1650)
18) \[PM-1103\] Allow for notifications to run p-statistics at event at\_end [\#1217](https://github.com/pegasus-isi/pegasus/issues/1217)
19) \[PM-1080\] Monitord should ignore Exceptions reported due to duplicate events being inserted [\#1194](https://github.com/pegasus-isi/pegasus/issues/1194)
20) \[PM-876\] Add support for 'halt' to ensemble manager [\#994](https://github.com/pegasus-isi/pegasus/issues/994)
21) update Pegasus API and planner to support sfapi as a grid gateway type [\#2187](https://github.com/pegasus-isi/pegasus/issues/2187)
22) Update documentation on how profiles expressions work [\#2184](https://github.com/pegasus-isi/pegasus/issues/2184)
23) update planner to accept expr pegasus profile keys [\#2183](https://github.com/pegasus-isi/pegasus/issues/2183)
24) Update Python Workflow API to allow expression keys to be specified corresponding to the profiles key [\#2182](https://github.com/pegasus-isi/pegasus/issues/2182)
25) change job resource requirements on job failures [\#2180](https://github.com/pegasus-isi/pegasus/issues/2180)
26) support units for certain profiles [\#2178](https://github.com/pegasus-isi/pegasus/issues/2178)
27) Use pegasus classad variables in remote\_ce\_requirements and remote\_environment to define values for glite/ssh job submissions [\#2176](https://github.com/pegasus-isi/pegasus/issues/2176)
28) use native supported htcondor keys for glite/ssh job submissions [\#2175](https://github.com/pegasus-isi/pegasus/issues/2175)
29) generate +pegasus classads for memory and disk [\#2170](https://github.com/pegasus-isi/pegasus/issues/2170)
30) warn if a user specifies condor resource profiles [\#2169](https://github.com/pegasus-isi/pegasus/issues/2169)
31) Revisit default values for request\_ [\#2167](https://github.com/pegasus-isi/pegasus/issues/2167)
32) composite event to include wf\_name and metrics app name [\#2166](https://github.com/pegasus-isi/pegasus/issues/2166)
33) composite events improvements for Pegasus AI [\#2165](https://github.com/pegasus-isi/pegasus/issues/2165)
34) implement transfer input files for shell code generator [\#2161](https://github.com/pegasus-isi/pegasus/issues/2161)
35) Convenience class to provide resource defaults [\#2160](https://github.com/pegasus-isi/pegasus/issues/2160)
36) Support for directory inputs [\#2159](https://github.com/pegasus-isi/pegasus/issues/2159)
37) pegasus-kickstart to expand variable names passed to the -s and -S options [\#2156](https://github.com/pegasus-isi/pegasus/issues/2156)
38) composite event to include total input and output file sizes for a job [\#2155](https://github.com/pegasus-isi/pegasus/issues/2155)
39) host site catalogs in a central GitHub repository to be pulled down when workflows are planned [\#2154](https://github.com/pegasus-isi/pegasus/issues/2154)
40) include invocation data in composite events [\#2149](https://github.com/pegasus-isi/pegasus/issues/2149)
41) \[PM-1972\] DAX used in performance test should be YAML and not XML [\#2078](https://github.com/pegasus-isi/pegasus/issues/2078)
42) \[PM-1919\] PEP 668 / non-os Python dependencies [\#2032](https://github.com/pegasus-isi/pegasus/issues/2032)



#### Bugs Fixed

1) Remove getenv=True for subdax [\#2221](https://github.com/pegasus-isi/pegasus/issues/2221)
2) pegasus-configure-glite does not create symlinks for local\_submit\_attributes.sh file [\#2191](https://github.com/pegasus-isi/pegasus/issues/2191)
3) data transfers broken between compute\(job\) generating an abstract workflow for the pegasusWorkflow job [\#2179](https://github.com/pegasus-isi/pegasus/issues/2179)
4) pegasus tries to mount a relative directory into the container [\#2173](https://github.com/pegasus-isi/pegasus/issues/2173)
5) Resource mapping issue on Slurm [\#2172](https://github.com/pegasus-isi/pegasus/issues/2172)
6) pegasus\_gpus classad in jobs is always set to 1 when no value is specified [\#2168](https://github.com/pegasus-isi/pegasus/issues/2168)
7) .lof files not distributed correctly for merged tasks [\#2163](https://github.com/pegasus-isi/pegasus/issues/2163)
8) unable to mount workflow directory on the shared filesystem into the shifter container [\#2244](https://github.com/pegasus-isi/pegasus/issues/2244)
9) container description in the TC is not serialized [\#2242](https://github.com/pegasus-isi/pegasus/issues/2242)
10) enforce numeric values for pegasus profiles memory and diskspace [\#2177](https://github.com/pegasus-isi/pegasus/issues/2177)
11) \[PM-1071\] Workflow leaf cleanup job fails \(rm of \<dir\>/.\) when relative dir is '.' [\#1185](https://github.com/pegasus-isi/pegasus/issues/1185)




### Pegasus 5.1.2

**Release Date:**  Feb 3rd, 2026

We are happy to announce the release of Pegasus 5.1.2. It is a minor release in the 5.1 branch.  We invite our users to give it a
try.

The release can be downloaded from:
<https://pegasus.isi.edu/downloads>


#### Highlights of the Release

1) Move to Condor File IO for OSDF transfers.

   OSDF transfers are now always delagated to HTCondor to manage using HTCondor file IO,
   especially when turning on Bypass Input File Staging. This applicable both for **condorio** and
   **nonsharedfs data** configurations.

   More details can be found in the
   [documentation](https://pegasus.isi.edu/docs/5.1.2/reference-guide/data-management.html#open-science-data-federation-osdf-stashcp-osdf-stash).

2) Support for Flux
   This release of Pegasus has support for running workflows on HPC resources managed by
   [Flux](https://flux-framework.org) resource manager. This support relies on changes to
   HTCondor which will be made available in an upcoming 25.7.0 release scheduled for
   March 2026.

   Details of mapping Pegasus resource profiles to flux parameters can be found
   [here](https://pegasus.isi.edu/docs/5.1.2/user-guide/deployment-scenarios.html#setting-job-requirements).

   This [repository](https://github.com/TauferLab/pegasus_flux_user_deploy) has useful scripts that a user can use to deploy pegasus in user mode
   on Flux Systems. [\#2143](https://github.com/pegasus-isi/pegasus/issues/2143)


3) Modified Pegasus versioning scheme to use Semantic Versioning Scheme v2 [\#2126](https://github.com/pegasus-isi/pegasus/issues/2126)

4) Support for Python 3.14

#### New Features and Improvements

1) avoid parsing sub workflows into memory when parsing the top level workflow that includes them [\#2148](https://github.com/pegasus-isi/pegasus/issues/2148)
2) flux support [\#2143](https://github.com/pegasus-isi/pegasus/issues/2143)
3) Move OSDF transfers to be via condor file transfers in Pegasus Lite instead of relying on pegasus-transfer [\#2141](https://github.com/pegasus-isi/pegasus/issues/2141)
4) pick user provided env script for PegasusLite from the site where the job runs [\#2136](https://github.com/pegasus-isi/pegasus/issues/2136)
5) limit the number of pegasus-monitord launches in pegasus-dagman [\#2134](https://github.com/pegasus-isi/pegasus/issues/2134)
6) Add support for Python 3.14 [\#2128](https://github.com/pegasus-isi/pegasus/issues/2128)
7) Modify Pegasus versioning scheme to use Semantic Versioning Scheme v2 [\#2126](https://github.com/pegasus-isi/pegasus/issues/2126)
8) enable condorio support for bosco/ssh style job submissions for remote HPC clusters [\#2121](https://github.com/pegasus-isi/pegasus/issues/2121)
9) use job classad variables to shorten paths in transfer\_input\_files key in the job submit directories [\#2120](https://github.com/pegasus-isi/pegasus/issues/2120)
10) Explore condor\_dag\_checker - something we want to use as part of planning? [\#2116](https://github.com/pegasus-isi/pegasus/issues/2116)
11) update sqlite jar to latest stable 3.49.1.0 or higher [\#2109](https://github.com/pegasus-isi/pegasus/issues/2109)
12) \[PM-1833\] pmc cpuinfo invalid detection [\#1946](https://github.com/pegasus-isi/pegasus/issues/1946)
13) \[PM-1098\] encrypt the credentials when transferred with jobs [\#1212](https://github.com/pegasus-isi/pegasus/issues/1212)
14) \[PM-1092\] Ask Condor team to propagate glite errors in gridmanager [\#1206](https://github.com/pegasus-isi/pegasus/issues/1206)

#### Bugs Fixed

1) enforce maximum document parsing size for yaml docs to 2047 MB [\#2152](https://github.com/pegasus-isi/pegasus/issues/2152)
2) expand wf_submit_dir when generating AWS Batch job descriptions #2151 [\#2151](https://github.com/pegasus-isi/pegasus/issues/2151)
3) cpu attributes are not included in the job composite event [\#2150](https://github.com/pegasus-isi/pegasus/issues/2150)
4) ensure user JAVA\_HEAPMAX and JAVA\_HEAPMIN values are propagated to the planner invocations for sub workflows [\#2147](https://github.com/pegasus-isi/pegasus/issues/2147)
5) pegasus-wms.worker incompatible with globus-sdk 4 [\#2146](https://github.com/pegasus-isi/pegasus/issues/2146)
6) bypass in condorio mode gets incorrectly triggered if a directory path specified for a local file [\#2142](https://github.com/pegasus-isi/pegasus/issues/2142)
7) cleanup jobs running remotely in nonsharedfs get associated with a container [\#2137](https://github.com/pegasus-isi/pegasus/issues/2137)
8) CLI tools pollute PYTHONPATH [\#2135](https://github.com/pegasus-isi/pegasus/issues/2135)
9) condor quoting is not triggered for arguments for glite and ssh style jobs [\#2132](https://github.com/pegasus-isi/pegasus/issues/2132)
10) monitord fails to parse job.out file if location record is malformed [\#2131](https://github.com/pegasus-isi/pegasus/issues/2131)
11) pegasus-init remote cluster option creates incorrect paths for local site [\#2125](https://github.com/pegasus-isi/pegasus/issues/2125)
12) worker package staging broken in sharedfs for create dir job \(if set to run remotely\) [\#2124](https://github.com/pegasus-isi/pegasus/issues/2124)
13) monitord overwrites transfer\_attempts records found in the job.out file [\#2123](https://github.com/pegasus-isi/pegasus/issues/2123)
14) pegasus-analyzer --debug-job option broken [\#2122](https://github.com/pegasus-isi/pegasus/issues/2122)
15) pegasus-mpi-cluster: cpuinfo validation fails on hybrid CPU architectures [\#2119](https://github.com/pegasus-isi/pegasus/issues/2119)



#### Merged Pull Requests

1) Bugfix: removes incorrect use of the 'local' keyword in flux\_local\_submit\_attributes.sh [\#2145](https://github.com/pegasus-isi/pegasus/pull/2145) ([ilumsden](https://github.com/ilumsden))
2) Adds a "local\_submit\_attributes.sh" script for the Flux RJMS [\#2144](https://github.com/pegasus-isi/pegasus/pull/2144) ([ilumsden](https://github.com/ilumsden))
3) Update pegasus-halt to fix POSIX compliance issue, bug on OSs where SH-\> dash [\#2139](https://github.com/pegasus-isi/pegasus/pull/2139) ([ahnitz](https://github.com/ahnitz))



### Pegasus 5.1.1 and 5.1.0

**Release Date:**  May 29, 2025


We are happy to announce the release of Pegasus 5.1.1. It is a minor release on top of
Pegasus 5.1.0 which is a major release of Pegasus. It also includes all features and
bug fixes from the 5.0 branch. We invite our users to give it a
try.

We recommend that users upgrade to 5.1.1 and not 5.1.0 because 5.1.1 has a fix for
[\#2113](https://github.com/pegasus-isi/pegasus/issues/2113) .

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

5) Please note that RPM packaging for 5.1.x series is not compatible with the 5.0.x series.
   If you try to update an existing 5.0.x install you will see an error similar to the trace below

```
dnf update pegasus
...
Running transaction check
Transaction check succeeded.
Running transaction test
The downloaded packages were saved in cache until the next successful transaction.
You can remove cached packages by executing 'dnf clean packages'.
Error: Transaction test error:
 file /usr/lib64/pegasus/python from install of pegasus-5.1.0-1.el8.x86_64 conflicts with file from package pegasus-5.0.9-1.el8.x86_64
```

   The recommended way is to first remove the 5.0.x install and then do the install.

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

1) snakeyaml version version 1.32, used by Jackson 2.14 has an in-built circuit breaker that breaks parsing for large yaml documents [\#2113](https://github.com/pegasus-isi/pegasus/issues/2113)
2) pegasus aws batch test failing because of urllib3 incompatibility [\#2107](https://github.com/pegasus-isi/pegasus/issues/2107)
3) Planner should catch deep lfn common name problem when using CEDAR [\#2106](https://github.com/pegasus-isi/pegasus/issues/2106)
4) support for condorio deep LFN broke after move to host OS based transfers [\#2105](https://github.com/pegasus-isi/pegasus/issues/2105)
5) when parsing container mount points in the TC, normalize the path to ensure any duplicate / in directory paths are removed [\#2103](https://github.com/pegasus-isi/pegasus/issues/2103)
6) pegasus-graphviz fails on a wf generated with java dax api that has no jobs [\#2101](https://github.com/pegasus-isi/pegasus/issues/2101)
7) \[PM-1954\] Importing six.moves raises ModuleNotFoundError on Python 3.12 [\#2067](https://github.com/pegasus-isi/pegasus/issues/2067)
8) \[PM-1968\] pegasus.gridstart allows values that are not documented [\#2074](https://github.com/pegasus-isi/pegasus/issues/2074)
9) \[PM-1952\] Local universe job fail with pegasus.transfer.bypass.input.staging = true [\#2065](https://github.com/pegasus-isi/pegasus/issues/2065)
10) \[PM-1924\] API submit still drops debug info to stdout [\#2037](https://github.com/pegasus-isi/pegasus/issues/2037)
11) \[PM-1902\] Pika problem on RHEL 9 - Bump the version to 1.2.1 also [\#2015](https://github.com/pegasus-isi/pegasus/issues/2015)
12) \[PM-1923\] download form does not send metrics to metric server [\#2036](https://github.com/pegasus-isi/pegasus/issues/2036)


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
