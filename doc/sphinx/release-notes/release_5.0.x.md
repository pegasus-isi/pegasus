
## Pegasus 5.0.x Series

### Pegasus 5.0.9

**Release Date:**  December 17, 2024

We are happy to announce the release of Pegasus 5.0.9, which is a minor
bug fix release for Pegasus 5.0 branch. The release has a variety of
improvements and bug fixes to Pegasus support for AWS Batch.

The release can be downloaded from https://pegasus.isi.edu/downloads


#### New Features and Improvements

1) [PM-1960] – Document debugging job submissions to HPC cluster [\#2072](https://github.com/pegasus-isi/pegasus/issues/2072)

2) [PM-1982] – pegasus-aws-batch setup/delete options should not fail at [\#2085](https://github.com/pegasus-isi/pegasus/issues/2085)
               first error

3) [PM-1983] – AWS Batch Error: ClientException: Maximum number of jobs [\#2086](https://github.com/pegasus-isi/pegasus/issues/2086)
               supported is 100

4) [PM-1991] – pegasus-aws-batch fails while interacting with a bucket in us-east-1 [\#2087](https://github.com/pegasus-isi/pegasus/issues/2087)

5) [PM-1992] – AWS Batch Compute Environments and job definitions are not [\#2088](https://github.com/pegasus-isi/pegasus/issues/2088)
               deleted reliably


#### Bugs Fixed

1) [PM-1959] – SGE memory parameter need a unit suffix to be [\#2071](https://github.com/pegasus-isi/pegasus/issues/2071)
               added to remote_ce_requirements

2) [PM-1976] – pegasus-aws-batch –log-file option [\#2082](https://github.com/pegasus-isi/pegasus/issues/2082)

3) [PM-1978] – amazon region is not being picked up correctly when [\#2084](https://github.com/pegasus-isi/pegasus/issues/2084)
               creating a s3 bucket

4) [PM-1997] – site selection for containerized executables should not [\#2089](https://github.com/pegasus-isi/pegasus/issues/2089)
               consider container image url (whether file or not)

5) [PM-1998] – pegasus-statistics lists incomplete jobs in a successful [\#2090](https://github.com/pegasus-isi/pegasus/issues/2090)
               aws batch wf run


### Pegasus 5.0.8

**Release Date:**  August 16, 2024

We are happy to announce the release of Pegasus 5.0.8, which is a minor
bug fix release for Pegasus 5.0 branch.

The release can be downloaded from https://pegasus.isi.edu/downloads

#### New Features and Improvements

1) [PM-1939] – Planner complains on http transfers if the user does not have a credentials file setup [\#2052](https://github.com/pegasus-isi/pegasus/issues/2052)
               

2) [PM-1940] – NPE for planning a workflow with sub workflows, where sub workflow job requires data from http endpoint [\#2053](https://github.com/pegasus-isi/pegasus/issues/2053)
               

3) [PM-1944] – Ability to specify a wrapper/launcher for containerized jobs in PegasusLite [\#2057](https://github.com/pegasus-isi/pegasus/issues/2057)
               

4) [PM-1946] – Improve Auth Token Acquisition For Globus Transfers [\#2059](https://github.com/pegasus-isi/pegasus/issues/2059)

5) [PM-1948] – Document deployment options on HPC centers [\#2061](https://github.com/pegasus-isi/pegasus/issues/2061)

#### Bugs Fixed

1) [PM-1941] – illegal state exception while using inplace cleanup [\#2054](https://github.com/pegasus-isi/pegasus/issues/2054)

2) [PM-1943] – mixed binary/conda installs broken [\#2056](https://github.com/pegasus-isi/pegasus/issues/2056)

3) [PM-1945] – transformation and container with same name causes error at runtime [\#2058](https://github.com/pegasus-isi/pegasus/issues/2058)

4) [PM-1947] – workflow restart fails because of error reading a tc file in /tmp [\#2060](https://github.com/pegasus-isi/pegasus/issues/2060)

5) [PM-1949] – NPE while planning a sub workflow [\#2062](https://github.com/pegasus-isi/pegasus/issues/2062)

6) [PM-1951] – For CLIs, factor out Pegaus Python module from system dir [\#2064](https://github.com/pegasus-isi/pegasus/issues/2064)

7) [PM-1953] – Monitord Failing with ModuleNotFoundError. [\#2066](https://github.com/pegasus-isi/pegasus/issues/2066)

8) [PM-1954] – Importing six.moves raises ModuleNotFoundError on Python 3.12 [\#2067](https://github.com/pegasus-isi/pegasus/issues/2067)

9) [PM-1958] – pegasus lite jobs fail at CIT if there is a lost+found dir in [\#2070](https://github.com/pegasus-isi/pegasus/issues/2070)
               the condor scratch dir


### Pegasus 5.0.7

**Release Date:** February 21, 2024

We are happy to announce the release of Pegasus 5.0.7, which is a minor
bug fix release for Pegasus 5.0 branch.

The release can be downloaded from https://pegasus.isi.edu/downloads

This release included improvements such as
 - users now to specify an input directory with executables to avoid
     creating a transformation catalog
 - support for SGE clusters in pegasus-init
 - support for private tokens while retrieving data from http endpoints
 - preference of apptainer executables over singularity

#### New Features and Improvements

1)  [PM-1926] – pegasus should allow users to specify an input directory with executables to avoid creating a TC [\#2039](https://github.com/pegasus-isi/pegasus/issues/2039)
            
2)  [PM-1888] – Apptainer support [\#2001](https://github.com/pegasus-isi/pegasus/issues/2001)
3)  [PM-1929] – convenient way to associate profiles for a site in properties file [\#2042](https://github.com/pegasus-isi/pegasus/issues/2042)
4)  [PM-1931] – support for local SGE cluster in pegasus-init [\#2044](https://github.com/pegasus-isi/pegasus/issues/2044)
5)  [PM-1933] – support for private-token to curl invocations [\#2046](https://github.com/pegasus-isi/pegasus/issues/2046)
6)  [PM-1936] – CLONE – support for local SGE cluster in pegasus-init [\#2049](https://github.com/pegasus-isi/pegasus/issues/2049)
7)  [PM-1928] – update python api to add the -t|–transformations-dir to the planner [\#2041](https://github.com/pegasus-isi/pegasus/issues/2041)
8)  [PM-1930] – add a convenience function to python api for adding site profiles [\#2043](https://github.com/pegasus-isi/pegasus/issues/2043)
9)  [PM-1935] – update the planner to optionally use the credentials file [\#2048](https://github.com/pegasus-isi/pegasus/issues/2048)
               for http transfers
10) [PM-1937] – support for mixed binary/venv/conda installs [\#2050](https://github.com/pegasus-isi/pegasus/issues/2050)


#### Bugs Fixed

1) [PM-1921] – arm arch string not consistent on linux and mac platforms [\#2034](https://github.com/pegasus-isi/pegasus/issues/2034)
2) [PM-1922] – fix recording of download metrics [\#2035](https://github.com/pegasus-isi/pegasus/issues/2035)
3) [PM-1927] – kickstart fails to filter out some UTF-8 non-printable characters [\#2040](https://github.com/pegasus-isi/pegasus/issues/2040)

### Pegasus 5.0.6

**Release Date:** June 30, 2023

We are happy to announce the release of Pegasus 5.0.6, which is a minor bug
fix release for Pegasus 5.0 branch.

The release can be downloaded from:
https://pegasus.isi.edu/downloads


#### New Features and Improvements

1)  [PM-1907]  Improve stash integration to be osdf:// aware [\#2020](https://github.com/pegasus-isi/pegasus/issues/2020)
2)  [PM-1910]  Handle dagman no longer inheriting user environment for the dagman job [\#2023](https://github.com/pegasus-isi/pegasus/issues/2023)
             
3)  [PM-1911]  Add support for Arm 64 architecture (aarch64) [\#2024](https://github.com/pegasus-isi/pegasus/issues/2024)
4)   [PM-1917]  Enable host-wide metrics collection [\#2030](https://github.com/pegasus-isi/pegasus/issues/2030)

#### Bugs Fixed

1)  [PM-1905]  File dependencies between sub workflow and compute jobs broken [\#2018](https://github.com/pegasus-isi/pegasus/issues/2018)
             
2)  [PM-1906]  Planner container mount point parsing breaks on . in the dir name [\#2019](https://github.com/pegasus-isi/pegasus/issues/2019)
             
3)  [PM-1909]  request_disk is incorrectly set to MBs instead of KBs [\#2022](https://github.com/pegasus-isi/pegasus/issues/2022)
4)  [PM-1913]  +DAGNodeRetry for attrib=value assigment breaks on HTondor 10.0.x when direct submission is disabled [\#2026](https://github.com/pegasus-isi/pegasus/issues/2026)
             
5)  [PM-1916]  Data management between parent compute job and a sub workflow job broken [\#2029](https://github.com/pegasus-isi/pegasus/issues/2029)
             
6)  [PM-1918]  Inplace cleanup broken when a sub workflow job and a parent compute job has a data dependency [\#2031](https://github.com/pegasus-isi/pegasus/issues/2031)
             

### Pegasus 5.0.5

**Release Date:** February 17, 2023

We are happy to announce the release of Pegasus 5.0.5, which is a minor bug
fix release for Pegasus 5.0 branch. This release corrects a build/packaging
problem in 5.0.4, resulting in the planner not finding all classes.

We invite our users to give it a try.

The release can be downloaded from:
https://pegasus.isi.edu/downloads


#### Bugs Fixed

1)  [PM-1904]  Incomplete clean between ant targets [\#2017](https://github.com/pegasus-isi/pegasus/issues/2017)


### Pegasus 5.0.4

**Release Date:** February 9, 2023

We are happy to announce the release of Pegasus 5.0.4, which is a minor bug
fix release for Pegasus 5.0 branch.  This release has some importan
 updates namely

- Support for HTCondor 10.2 series
- Improved sub workflow file handling

We invite our users to give it a try.

The release can be downloaded from:
https://pegasus.isi.edu/downloads


#### New Features and Improvements

1)  [PM-1890]  pegasus-analyzer should show failing jobs [\#2003](https://github.com/pegasus-isi/pegasus/issues/2003)

2)  [PM-1891]  pegasus-analyzer should traverse all sub workflows [\#2004](https://github.com/pegasus-isi/pegasus/issues/2004)

3)  [PM-1898]  File dependencies for sub workflow jobs - differentiate [\#2011](https://github.com/pegasus-isi/pegasus/issues/2011)
             inputs for planner use and those for sub workflow

4)  [PM-1899]  update python api and json schema to expose forPlanning [\#2012](https://github.com/pegasus-isi/pegasus/issues/2012)
             boolean attribute with files in uses section

5)  [PM-1900]  update java wf api to support forPlanner attribute for files [\#2013](https://github.com/pegasus-isi/pegasus/issues/2013)

#### Bugs Fixed

1) [PM-1895]  handle condor_submit updated way of specifying environment [\#2008](https://github.com/pegasus-isi/pegasus/issues/2008)
               in the .dag.condor.sub file

2) [PM-1893]  need to explicitly mount sharedfilesystem dir into container [\#2006](https://github.com/pegasus-isi/pegasus/issues/2006)
             when using shared filesystem as staging site for nonsharedfs

3)  [PM-1894]  worker package transfer into application containers [\#2007](https://github.com/pegasus-isi/pegasus/issues/2007)

4)  [PM-1896]  In pegasus lite scripts worker package strict check is [\#2009](https://github.com/pegasus-isi/pegasus/issues/2009)
             turned off

5)  [PM-1897]  update pegasus-configure-glite to use BLAHPD_LOCATION [\#2010](https://github.com/pegasus-isi/pegasus/issues/2010)

### Pegasus 5.0.3

**Release Date:** October 25, 2022


We are happy to announce the release of Pegasus 5.0.3, which is a minor bug fix release for Pegasus 5.0
branch.  This release has some important updates namely

- Support for Deep LFN’s in CondorIO Mode
  	  If you are using bypass of staging for input files, then support for deep LFN’s depends
	  on associated HTCondor ticket 1325 that will be fixed in HTCondor release 10.1.0.
- Per Job Symlinking
- New Containers exercise in the Pegasus Tutorial

We invite our users to give it a try.

The release can be downloaded from:
https://pegasus.isi.edu/downloads


#### New Features and Improvements

1) [PM-1873] – Add a containers focussed exercise to the tutorial [\#1986](https://github.com/pegasus-isi/pegasus/issues/1986)

2) [PM-1875] – Support deep LFN’s in CondorIO mode [\#1988](https://github.com/pegasus-isi/pegasus/issues/1988)

3) [PM-1806] – Fix dest filenames for transformations [\#1920](https://github.com/pegasus-isi/pegasus/issues/1920)

4) [PM-1815] – Prevent pegasus-lite failure when user passes -w to docker [\#1928](https://github.com/pegasus-isi/pegasus/issues/1928)

5) [PM-1871] – Remove the “version” parameter from worker package transformation in TC documentation [\#1984](https://github.com/pegasus-isi/pegasus/issues/1984)

6) [PM-1879] – Per job symlinking [\#1992](https://github.com/pegasus-isi/pegasus/issues/1992)

7) [PM-1885] – Allow bypass of input files in CondorIO mode to be similar to behavior for nonsharedfs [\#1998](https://github.com/pegasus-isi/pegasus/issues/1998)

8) [PM-1876] – implement moveto support in pegasus-transfer [\#1989](https://github.com/pegasus-isi/pegasus/issues/1989)

9) [PM-1877] – mimic transfer_output_remaps in pegasus-lite-local.sh for local universe jobs [\#1990](https://github.com/pegasus-isi/pegasus/issues/1990)

#### Bugs Fixed

1) [PM-1809] – Job fails when a container has a pre-existing group with the same gid as the [\#1922](https://github.com/pegasus-isi/pegasus/issues/1922)
               one being created with groupadd

2) [PM-1864] – request_memory and request_disk does not get applied for local universe jobs [\#1977](https://github.com/pegasus-isi/pegasus/issues/1977)

3) [PM-1868] – pegasus-init cli tool not working [\#1981](https://github.com/pegasus-isi/pegasus/issues/1981)

4) [PM-1872] – Unable to locate executable when pegasus::worker transform is overridden [\#1985](https://github.com/pegasus-isi/pegasus/issues/1985)

5) [PM-1878] – hierarchical workflows broken on recent condor install [\#1991](https://github.com/pegasus-isi/pegasus/issues/1991)

6) [PM-1883] – users cannot decrease planner logging if pegasus.mode is set to debug [\#1996](https://github.com/pegasus-isi/pegasus/issues/1996)

7) [PM-1884] – pegasus-remove does does not remove a running workflow [\#1997](https://github.com/pegasus-isi/pegasus/issues/1997)

8) [PM-1887] – update broken bamboo tests [\#2000](https://github.com/pegasus-isi/pegasus/issues/2000)


### Pegasus 5.0.2

**Release Date:** April 22, 2022

We are happy to announce the release of Pegasus 5.0.2, which is a minor bug
fix release for Pegasus 5.0 branch.  This release has some important updates
namely

- Updated Pegasus Log4J support to 2.17.
- Globus Online Transfers now have support for consent options on endpoints

The release also has important bug fixes related to correctly detecting job
failures for grid universe jobs.

We invite our users to give it a try.

The release can be downloaded from:
https://pegasus.isi.edu/downloads


#### New Features and Improvements

1)  [PM-1828] – Doc. mismatch [\#1941](https://github.com/pegasus-isi/pegasus/issues/1941)

2)  [PM-1817] – pegasus-run with shell code generator [\#1930](https://github.com/pegasus-isi/pegasus/issues/1930)

3)  [PM-1824] – decaf jobs should be associated with pegasus-exitcode postscript [\#1937](https://github.com/pegasus-isi/pegasus/issues/1937)

4)  [PM-1825] – update glossary [\#1938](https://github.com/pegasus-isi/pegasus/issues/1938)

5)  [PM-1826] – way to pass on additional arguments to clustered jobs [\#1939](https://github.com/pegasus-isi/pegasus/issues/1939)

6)  [PM-1830] – Globus Online transfers required GO consent [\#1943](https://github.com/pegasus-isi/pegasus/issues/1943)

7)  [PM-1835] – Upcoming changes to DAGMan output logging [\#1948](https://github.com/pegasus-isi/pegasus/issues/1948)

8)  [PM-1836] – Update Pegasus Log4J support to 2.16 [\#1949](https://github.com/pegasus-isi/pegasus/issues/1949)

9)  [PM-1839] – allow easy clustering of the whole workflow without associating [\#1952](https://github.com/pegasus-isi/pegasus/issues/1952)
                labels for the jobs

10) [PM-1673] – passing properties as str to Properties() can be error prone, [\#1787](https://github.com/pegasus-isi/pegasus/issues/1787)
    	      	add some preliminary checks before writing

11) [PM-1759] – user facing class/function args shouldn’t be prefixed [\#1873](https://github.com/pegasus-isi/pegasus/issues/1873)
    	        with _ such as _id

12) [PM-1816] – make it easier to add entries to the replica catalog by inferring [\#1929](https://github.com/pegasus-isi/pegasus/issues/1929)
    	        site, lfn, pfn from file Path or URL

13) [PM-1822] – improve parsing of value in Mixins._to_mb(value) [\#1935](https://github.com/pegasus-isi/pegasus/issues/1935)

14) [PM-1827] – type check pfn in ReplicaCatalog.add_replica() [\#1940](https://github.com/pegasus-isi/pegasus/issues/1940)

15) [PM-1831] – planner by default should pick up credentials.conf when pegasus-s3 is used [\#1944](https://github.com/pegasus-isi/pegasus/issues/1944)

16) [PM-1834] – ensure that yaml is serialized in a deterministic manner [\#1947](https://github.com/pegasus-isi/pegasus/issues/1947)

17) [PM-1858] – pegasus-s3 should pick up PEGASUS_CREDENTIAL environment variable [\#1971](https://github.com/pegasus-isi/pegasus/issues/1971)

18) [PM-1859] – Document decaf as a clustering tool in the clustering guide [\#1972](https://github.com/pegasus-isi/pegasus/issues/1972)

#### Bugs Fixed

1)  [PM-1763] – validate all strings that will then be used as filenames or [\#1877](https://github.com/pegasus-isi/pegasus/issues/1877)
               used in sub files

2)  [PM-1821] – job failures not detected for grid universe jobs [\#1934](https://github.com/pegasus-isi/pegasus/issues/1934)

3)  [PM-1823] – Serialization of pegasus.memory results in a floating point no. [\#1936](https://github.com/pegasus-isi/pegasus/issues/1936)

4)  [PM-1832] – extraneous whitespace in arguments for sub workflow job with java generator [\#1945](https://github.com/pegasus-isi/pegasus/issues/1945)

5)  [PM-1837] – planner throws null pointer exception when invalid staging site is given [\#1950](https://github.com/pegasus-isi/pegasus/issues/1950)

6)  [PM-1838] – Intermediate outputs in a clustered job get sent back to staging [\#1951](https://github.com/pegasus-isi/pegasus/issues/1951)
    	      	site when they are not used by subsequent jobs outside of the cluster
		and when stage_out has been set to false for those files

7)  [PM-1840] – Workflow class methods will always be None [\#1953](https://github.com/pegasus-isi/pegasus/issues/1953)

8)  [PM-1851] – PegasusLite submissions to local cluster (Slurm/PBS/etc) unable [\#1964](https://github.com/pegasus-isi/pegasus/issues/1964)
    	        to source pegasus-lite-common.sh

### Pegasus 5.0.1

**Release Date:** October 7, 2021

We are happy to announce the release of Pegasus 5.0.1. Pegasus 5.0.1 is a
minor bug fix release after Pegasus 5.0. We invite our users to give it
a try.

The release features improvements to the Pegasus Python API including
ability to visualize statically the abstract and generated executable
workflows. It also has improved support for DECAF, including an ability to
get clustered jobs in a workflow executed using DECAF.

This release has improvements to data access in PegasusLite jobs, if
data resides on local site, and job runs on a site where “auxiliary.local”
profile is set to true. Users can now use a new Submit Mapper called
Named that allows you to specify what sub directory a job’s submit
files are placed in.

Release also features updated support for submission of jobs using
HubZero Distribute to HPC Clusters and new pegasus.mode called "debug"
to enable verbose logging throughout the Pegasus stack.

The release can be downloaded from:
https://pegasus.isi.edu/downloads


#### New Features and Improvements

1)  [PM-1726] – Update support for HubZero Distribute [\#1840](https://github.com/pegasus-isi/pegasus/issues/1840)

2)  [PM-1751] – Named Submit Directory Mapper [\#1865](https://github.com/pegasus-isi/pegasus/issues/1865)

3)  [PM-1798] – instead of the workflow having explicit data flow jobs, [\#1912](https://github.com/pegasus-isi/pegasus/issues/1912)
               get pegasus to automatically cluster jobs to a decaf
	       representation

4)  [PM-1753] – add Workflow.get_status() [\#1867](https://github.com/pegasus-isi/pegasus/issues/1867)

5)  [PM-1767] – remove the default arguments, output_sites and cleanup in SubWorkflow.add_planner_args() [\#1881](https://github.com/pegasus-isi/pegasus/issues/1881)
                

6)  [PM-1786] – update usage of threading.Thread.isAlive() to be is_alive() in python scripts [\#1900](https://github.com/pegasus-isi/pegasus/issues/1900)
                

7)  [PM-1788] – Add configuration documentation for hierarchical workflows [\#1902](https://github.com/pegasus-isi/pegasus/issues/1902)

8)  [PM-1429] – Introduce PEGASUS_ENV variable to define mode of workflow  i.e. development, production, etc [\#1543](https://github.com/pegasus-isi/pegasus/issues/1543)
                

9)  [PM-1651] – Add more profile keys in the add_pegasus_profile [\#1765](https://github.com/pegasus-isi/pegasus/issues/1765)

10) [PM-1672] – override add_args for SubWorkflow so that args refer to planner args [\#1786](https://github.com/pegasus-isi/pegasus/issues/1786)

11) [PM-1706] – sphinx has hardcoded versios [\#1820](https://github.com/pegasus-isi/pegasus/issues/1820)

12) [PM-1730] – 5.0.1 Python Api improvements [\#1844](https://github.com/pegasus-isi/pegasus/issues/1844)

13) [PM-1733] – expand on checkpointing documentation [\#1847](https://github.com/pegasus-isi/pegasus/issues/1847)

14) [PM-1739] – expose panda job submissions similar to how we support BOSCO [\#1853](https://github.com/pegasus-isi/pegasus/issues/1853)

15) [PM-1742] – allow a tc to be empty without the planner failing [\#1856](https://github.com/pegasus-isi/pegasus/issues/1856)

16) [PM-1743] – allow catalogs to be embedded into workflow when workflow contains sub workflows [\#1857](https://github.com/pegasus-isi/pegasus/issues/1857)

17) [PM-1747] – 031-montage-condor-io-jdbcrc failing [\#1861](https://github.com/pegasus-isi/pegasus/issues/1861)

18) [PM-1768] – replace GRAM workflow tests with bosco [\#1882](https://github.com/pegasus-isi/pegasus/issues/1882)

19) [PM-1769] – update tests since /nfs/ccg3 is gone now [\#1883](https://github.com/pegasus-isi/pegasus/issues/1883)

20) [PM-1771] – pegasus-db-admin upgrade [\#1885](https://github.com/pegasus-isi/pegasus/issues/1885)

21) [PM-1780] – Refactor Transfer Engine Code [\#1894](https://github.com/pegasus-isi/pegasus/issues/1894)

22) [PM-1787] – auxiliary.local is not considered when triggering symlink in PegasusLite in nonsharedfs mode [\#1901](https://github.com/pegasus-isi/pegasus/issues/1901)
                

23) [PM-1792] – decaf jobs over bosco [\#1906](https://github.com/pegasus-isi/pegasus/issues/1906)

24) [PM-1794] – put in support for additional keys required by decaf [\#1908](https://github.com/pegasus-isi/pegasus/issues/1908)

25) [PM-1796] – passing properties to be set for sub workflow jobs [\#1910](https://github.com/pegasus-isi/pegasus/issues/1910)

26) [PM-1800] – enable inplace cleanup for hierarchical workflows [\#1914](https://github.com/pegasus-isi/pegasus/issues/1914)

27) [PM-1802] – Add support for Debian 11 [\#1916](https://github.com/pegasus-isi/pegasus/issues/1916)

28) [PM-1803] – use force option when doing a docker rm of the container image [\#1917](https://github.com/pegasus-isi/pegasus/issues/1917)

29) [PM-1810] – Extend debug capabilities for pegasus.mode [\#1923](https://github.com/pegasus-isi/pegasus/issues/1923)

30) [PM-1811] – add pegasus-keg to worker package [\#1924](https://github.com/pegasus-isi/pegasus/issues/1924)

31) [PM-1818] – new pegasus.mode debug [\#1931](https://github.com/pegasus-isi/pegasus/issues/1931)

32) [PM-1723] – add_<namespace>_profile() should be plural [\#1837](https://github.com/pegasus-isi/pegasus/issues/1837)

33) [PM-1731] – functions that take in File objects as input parameters should also accept strings for convenience [\#1845](https://github.com/pegasus-isi/pegasus/issues/1845)

34) [PM-1744] – progress bar from wf.wait() should include “UNRDY” as shown in status output [\#1858](https://github.com/pegasus-isi/pegasus/issues/1858)

35) [PM-1755] – catalog write location should be stored upon call to catalog.write() [\#1869](https://github.com/pegasus-isi/pegasus/issues/1869)

36) [PM-1757] – add pegasus profile relative.submit.dir [\#1871](https://github.com/pegasus-isi/pegasus/issues/1871)

37) [PM-1784] – Refactor Stagein Generator code out of Transfer Engine [\#1898](https://github.com/pegasus-isi/pegasus/issues/1898)

38) [PM-1790] – extend site catalog schema to indicate shared file system access for a directory [\#1904](https://github.com/pegasus-isi/pegasus/issues/1904)

39) [PM-1791] – update planner to parse sharedFileSystem attribute from site catalog [\#1905](https://github.com/pegasus-isi/pegasus/issues/1905)

40) [PM-1797] – use logging over print statements [\#1911](https://github.com/pegasus-isi/pegasus/issues/1911)

41) [PM-1804] – add verbose options for development mode [\#1918](https://github.com/pegasus-isi/pegasus/issues/1918)

#### Bugs Fixed

1)  [PM-1709] – the yaml handler in pegasus-graphviz needs to handle ‘checkpoint’ link type [\#1823](https://github.com/pegasus-isi/pegasus/issues/1823)

2)  [PM-1722] – Job node_label attribute is not identified by the planner [\#1836](https://github.com/pegasus-isi/pegasus/issues/1836)

3)  [PM-1725] – nodeLabel for a job needs to be parsed in yaml handler if it is given [\#1839](https://github.com/pegasus-isi/pegasus/issues/1839)

4)  [PM-1736] – Pegasus pollutes the job env when getenv=true [\#1850](https://github.com/pegasus-isi/pegasus/issues/1850)

5)  [PM-1737] – monitord fails on divide by 0 error while computing avg cpu utilization [\#1851](https://github.com/pegasus-isi/pegasus/issues/1851)

6)  [PM-1745] – time.txt in stats is misformatted [\#1859](https://github.com/pegasus-isi/pegasus/issues/1859)

7)  [PM-1746] – jobs aborted by dagman, but with kickstart exitcode as 0 are not marked as failed job [\#1860](https://github.com/pegasus-isi/pegasus/issues/1860)

8)  [PM-1748] – planner fails with NPE on empty workflow [\#1862](https://github.com/pegasus-isi/pegasus/issues/1862)

9)  [PM-1750] – ensemble mgr workflow priorities need to be reversed [\#1864](https://github.com/pegasus-isi/pegasus/issues/1864)

10) [PM-1752] – fix checkpoint.time in add_pegasus_profile [\#1866](https://github.com/pegasus-isi/pegasus/issues/1866)

11) [PM-1754] – pegasus-db-admin fails to upgrade database [\#1868](https://github.com/pegasus-isi/pegasus/issues/1868)

12) [PM-1761] – pegasus-analyzer showing “failed to send files” error when root cause is exec format error [\#1875](https://github.com/pegasus-isi/pegasus/issues/1875)

13) [PM-1762] – pegasus-analyzer showing no error at all when workflow failed based on status output [\#1876](https://github.com/pegasus-isi/pegasus/issues/1876)

14) [PM-1764] – fix pegasus-analyzer output typo [\#1878](https://github.com/pegasus-isi/pegasus/issues/1878)

15) [PM-1765] – for SubWorkflow jobs, the planner argument, –output-sites, isn’t being set [\#1879](https://github.com/pegasus-isi/pegasus/issues/1879)

16) [PM-1766] – for SubWorkflow jobs, the planner argument, –force, isn’t being set [\#1880](https://github.com/pegasus-isi/pegasus/issues/1880)

17) [PM-1770] – 041-jdbcrc-performance failing [\#1884](https://github.com/pegasus-isi/pegasus/issues/1884)

18) [PM-1772] – db upgrade leaves transient tables [\#1886](https://github.com/pegasus-isi/pegasus/issues/1886)

19) [PM-1777] – pegasus-graphviz producing incorrect dot file when redundant edges removed [\#1891](https://github.com/pegasus-isi/pegasus/issues/1891)

20) [PM-1779] – Stage out job executed on local instead of remote site (donut) [\#1893](https://github.com/pegasus-isi/pegasus/issues/1893)

21) [PM-1783] – bypass input staging in nonsharedfs mode does not work for file URL and auxiliary.local set [\#1897](https://github.com/pegasus-isi/pegasus/issues/1897)

22) [PM-1785] – hostnames missing from elasticsearch job data [\#1899](https://github.com/pegasus-isi/pegasus/issues/1899)

23) [PM-1789] – Scratch dir GET/PUT operations get overridden [\#1903](https://github.com/pegasus-isi/pegasus/issues/1903)

24) [PM-1795] – Output Mapper in conjunction with data dependencies between sub workflow jobs [\#1909](https://github.com/pegasus-isi/pegasus/issues/1909)

25) [PM-1799] – json schema validation fails for selector profiles [\#1913](https://github.com/pegasus-isi/pegasus/issues/1913)

26) [PM-1820] – Deserializing a YAML transformation files always sets the os.type to linux [\#1933](https://github.com/pegasus-isi/pegasus/issues/1933)



### Pegasus 5.0.0

**Release Date:** November 12, 2020

We are happy to announce the release of Pegasus 5.0.  Pegasus 5.0
is be a major release of Pegasus and builds upon the beta version
released couple of months back. It also includes all features and
bug fixes from the 4.9 branch. We invite our users to give it a
try.

The release can be downloaded from:
https://pegasus.isi.edu/downloads

If you are an existing user, please carefully follow these
instructions to upgrade at
https://pegasus.isi.edu/docs/5.0.0/user-guide/migration.html#migrating-from-pegasus-4-9-x-to-pegasus-5-0

#### Highlights of the Release

1)  Reworked Python API:

    This new API has been developed from the
    ground up so that, in addition to generating the abstract workflow
    and all the catalogs, it now allows you to plan, submit, monitor,
    analyze and generate statistics of your workflow.
    To use this new Python API refer to the Moving From
    DAX3 to Pegasus.api at
    https://pegasus.isi.edu/docs/5.0.0/user-guide/migration.html#moving-from-dax3

2)  Adoption of YAML formats:

    With Pegasus 5.0, we are moving to
    adoption of YAML for representation of all major catalogs. We
    have provided catalog converters for you to convert your existing
    catalogs to the new formats. In 5.0, the following are now represented
    in YAML:
       - Abstract Workflow
       - Replica Catalog
       - Transformation Catalog
       - Site Catalog
       - Kickstart Provenance Records

3)  Python3 Support:

    All Pegasus tools are Python 3 compliant.
    5.0 release will require Python 3 on workflow submit node
    Python PIP packages for workflow composition and monitoring


4) Default data configuration

   In Pegasus 5.0, the default data configuration has been changed
   to condorio . Up to 4.9.x releases, the default configuration
   was sharedfs.

5) Zero configuration required to submit to local HTCondor pool

6) Data Management Improvements

   - New output replica catalog that registers outputs including
     file metadata such as size and checksums
   - Ability to do bypass staging of files at a per file,
     executable and container level
   - Improved support for hierarchal workflows allow you to create
     data dependencies between sub workflow jobs and compute jobs
   - Support for staging of generated outputs to multiple output sites
   - Support for integrity checking of user executables and application
     containers in addition to data
   - Support for webdav transfers
   - Easier enabling of data reuse by specifying previous workflow
     submit directories using –reuse option to pegasus-plan.
   - Stagein transfer jobs are assigned priorities based on the number
     of child compute jobs. Details can be found in JIRA ticket 1385

7)  New Jupyter Notebook Based Tutorial

    With this release, we are pleased to announce a brand new tutorial
    based on a Docker container running interactive Jupyter notebooks.
    You can access the tutorial at
     PM-1385 [\#1499](https://github.com/pegasus-isi/pegasus/issues/1499)

8)  Support for CWL (Common Workflow Language)

    The pegasus-cwl-converter command line tool has been developed to
    convert a subset of the Common Workflow Language (CWL) to Pegasus’s
    native YAML format. Given the following three files: a CWL workflow
    file, a workflow inputs specification file, and a transformation
    (executable) specification file, pegasus-cwl-converter
    will do a best-effort translation. This will also work with CWL
    workflow specifications that refer to Docker containers. The entire
    CWL language specification is not yet covered by this converter, and
    as such we can provide additional support in converting
    your workflows into the Pegasus YAML format.

9) Support for triggers in Ensemble Manager

    The Pegasus Ensemble Manager is a service that manages collections
    of workflows. In this latest release of Pegasus, workflow triggering
    functionality has been added to this service. With the ensemble manager
    service up and running, the pegasus-em command can now be used to start
    workflow triggers. Two triggersare currently supported:
    - a cron based trigger: The cron based trigger will, at a given
      interval, submit a new workflow to your ensemble.
    - a cron based, file pattern trigger: The cron based, file pattern
      trigger, much like the cron based trigger, will submit a new
      workflow to your ensemble at a given time interval, with the addition
      of any new files that are detected based on a given file pattern.
      This is useful for automatically processing data as it arrives.

10) Improved events for each job reported to AMQP

    Historically the events reported to AMQP endpoints are normalized
    events corresponding to the stampede database, which makes
    correlation hard. Pegasus now also reports a new job composite event
    (stampede.job_inst.composite) to AMQP end points that have a
    complete information about a job execution.

11) Revamped Documentation

    Documentation has been overhauled and broken down into a user guide
    and a reference guide. In addition, we have moved to readthedocs style
    documentation using restructured text.  The documentation can be found
    at https://pegasus.isi.edu/docs/5.0.0/index.html .

12) pegasus-statistics reports memory usage and avg cpu utilization

    pegasus-statistics now reports memory usage and average cpu utilization
    for your jobs in the transformation statistics file (breakdown.txt).

13) PegasusLite Improvements

    - Users can now specify environment setup scripts in the site catalog
      that need to be sourced to setup the environment before the job is
      launched by PegasusLite. More details at
      https://pegasus.isi.edu/docs/5.0.0/reference-guide/pegasus-lite.html#setting-the-environment-in-pegasuslite-for-your-job
    - Users can get the transfers in PegasusLite to run on DTN nodes while
      the jobs run on the the compute nodes. More details at
      https://pegasus.isi.edu/docs/5.0.0/reference-guide/pegasus-lite.html#specify-compute-job-in-pegasuslite-to-run-on-different-node

13) Credentials existence is checked upfront by planner

14) Performance improvements for pegasus-rc-client

    pegasus-rc-client now does bulk inserts when inserting entries into
    a database backed Replica Catalog.

15) Pegasus ensures a consistent UTF8 environment across full workflow
    Details can be found in JIRA ticket 1592 .



#### New Features

1) [PM-603] – Enable workflows to reference local input files directly instead of symlinking them [\#721](https://github.com/pegasus-isi/pegasus/issues/721)

2) [PM-1133] – Kickstart should send a heartbeat so that condor can kill stuck jobs [\#1247](https://github.com/pegasus-isi/pegasus/issues/1247)

3) [PM-1156] – PegasusLite to tar up the contents of the cwd in case of job failure [\#1270](https://github.com/pegasus-isi/pegasus/issues/1270)

4) [PM-1278] – stats should include cpu and memory utilization [\#1392](https://github.com/pegasus-isi/pegasus/issues/1392)

5) [PM-1309] – develop a pip package that only contains the DAX API and the catalogs API [\#1423](https://github.com/pegasus-isi/pegasus/issues/1423)

6) [PM-1335] – YAML based transformation catalog [\#1449](https://github.com/pegasus-isi/pegasus/issues/1449)

7) [PM-1339] – construct a default entry for local site if not present in site catalog [\#1453](https://github.com/pegasus-isi/pegasus/issues/1453)

8) [PM-1345] – Support for Shifter at Nersc [\#1459](https://github.com/pegasus-isi/pegasus/issues/1459)

9) [PM-1351] – YAML based kickstart records [\#1465](https://github.com/pegasus-isi/pegasus/issues/1465)

10) [PM-1352] – Build failure on Debian 10 due to mariadb/MySQL-Python incompatibility [\#1466](https://github.com/pegasus-isi/pegasus/issues/1466)

11) [PM-1354] – pegasus-init to support titan tutorial [\#1468](https://github.com/pegasus-isi/pegasus/issues/1468)

12) [PM-1355] – composite records when sending events to AMQP [\#1469](https://github.com/pegasus-isi/pegasus/issues/1469)

13) [PM-1357] – In lite jobs, chirp durations for stage in, stage out of data [\#1471](https://github.com/pegasus-isi/pegasus/issues/1471)

14) [PM-1367] – Support for retrieval from HPSS tape store using commands htar and hsi [\#1481](https://github.com/pegasus-isi/pegasus/issues/1481)

15) [PM-1390] – ensure all machine parseable information is one file associated with job [\#1504](https://github.com/pegasus-isi/pegasus/issues/1504)

16) [PM-1396] – kickstart yaml parser fails because of : unacceptable character #x001b: special characters are not allowed [\#1510](https://github.com/pegasus-isi/pegasus/issues/1510)

17) [PM-1398] – include machine information in job_instance.composite event [\#1512](https://github.com/pegasus-isi/pegasus/issues/1512)

18) [PM-1402] – pegasus-init to support summit as execution env for tutorial [\#1516](https://github.com/pegasus-isi/pegasus/issues/1516)

19) [PM-1411] – create the schema for a YAML based DAX [\#1525](https://github.com/pegasus-isi/pegasus/issues/1525)

20) [PM-1438] – YAML Based Site Catalog [\#1552](https://github.com/pegasus-isi/pegasus/issues/1552)

21) [PM-1461] – Ability to specify a wrapper/launcher for compute jobs in PegasusLite [\#1575](https://github.com/pegasus-isi/pegasus/issues/1575)

22) [PM-1470] – pegasus-graphviz needs a yaml handler [\#1584](https://github.com/pegasus-isi/pegasus/issues/1584)

23) [PM-1493] – YAML Based Replica Catalog [\#1607](https://github.com/pegasus-isi/pegasus/issues/1607)

24) [PM-1501] – Parse YAML DAX files [\#1615](https://github.com/pegasus-isi/pegasus/issues/1615)

25) [PM-1516] – Planner should create a default condorpool compute site if a user does not have it specified [\#1630](https://github.com/pegasus-isi/pegasus/issues/1630)

26) [PM-1528] – update DAX R API to emit new workflow format in yaml [\#1642](https://github.com/pegasus-isi/pegasus/issues/1642)

27) [PM-1529] – set default data configuration to condorio [\#1643](https://github.com/pegasus-isi/pegasus/issues/1643)

28) [PM-1551] – Update JAVA DAX API to generate yaml formatted DAX [\#1665](https://github.com/pegasus-isi/pegasus/issues/1665)

29) [PM-1552] – move to using pegasus lite for cases, where we transfer pegasus-transfer [\#1666](https://github.com/pegasus-isi/pegasus/issues/1666)

30) [PM-1608] – data dependencies between dax jobs and compute jobs in a workflow [\#1722](https://github.com/pegasus-isi/pegasus/issues/1722)

31) [PM-1620] – enable integrity checking for containers [\#1734](https://github.com/pegasus-isi/pegasus/issues/1734)

32) [PM-1681] – Enable easy data reuse from previous runs [\#1795](https://github.com/pegasus-isi/pegasus/issues/1795)

33) [PM-1685] – Python package to clone github repos for pegasus-init in 5.0 [\#1799](https://github.com/pegasus-isi/pegasus/issues/1799)

34) [PM-1286] – Deprecate Perl DAX API [\#1400](https://github.com/pegasus-isi/pegasus/issues/1400)

35) [PM-1376] – Add LSF local attributes [\#1490](https://github.com/pegasus-isi/pegasus/issues/1490)

36) [PM-1378] – Handle (copy) HPSS credentials when an environment variable is set [\#1492](https://github.com/pegasus-isi/pegasus/issues/1492)

37) [PM-1382] – Add ppc64le to the known architectures [\#1496](https://github.com/pegasus-isi/pegasus/issues/1496)

38) [PM-1383] – Switching to AMQP 0.9.1 in Pegasus Monitord [\#1497](https://github.com/pegasus-isi/pegasus/issues/1497)

39) [PM-1400] – Remove MacOS .pkg builder [\#1514](https://github.com/pegasus-isi/pegasus/issues/1514)

40) [PM-1401] – Deprecate pegasus-plots [\#1515](https://github.com/pegasus-isi/pegasus/issues/1515)

41) [PM-1412] – Upgrade documentation [\#1526](https://github.com/pegasus-isi/pegasus/issues/1526)

42) [PM-1413] – Upgrade Pegasus Databases to be Unicode Compatible [\#1527](https://github.com/pegasus-isi/pegasus/issues/1527)

43) [PM-1416] – add data collection setup instructions to docs under section 6.7.1.1. Monitord, RabbitMQ, ElasticSearch Example [\#1530](https://github.com/pegasus-isi/pegasus/issues/1530)

44) [PM-1446] – create tests for python client [\#1560](https://github.com/pegasus-isi/pegasus/issues/1560)

45) [PM-1467] – Update jupyter notebook code to use new 5.0 api [\#1581](https://github.com/pegasus-isi/pegasus/issues/1581)

46) [PM-1484] – create a default site catalog [\#1598](https://github.com/pegasus-isi/pegasus/issues/1598)

47) [PM-1485] – change default pegasus.data.configuration from sharedfs to condorio [\#1599](https://github.com/pegasus-isi/pegasus/issues/1599)

48) [PM-1486] – update default filenames for catalogs and workflow [\#1600](https://github.com/pegasus-isi/pegasus/issues/1600)

49) [PM-1495] – checksum.value and checksum.type need to be added as optional rc entry fields [\#1609](https://github.com/pegasus-isi/pegasus/issues/1609)

50) [PM-1510] – stageOut and registerReplica fields in Uses to be omitted for Uses of type input [\#1624](https://github.com/pegasus-isi/pegasus/issues/1624)

51) [PM-1513] – Merge DECAF branch to master to support decaf integration [\#1627](https://github.com/pegasus-isi/pegasus/issues/1627)

52) [PM-1524] – Use entrypoint in docker containers [\#1638](https://github.com/pegasus-isi/pegasus/issues/1638)

53) [PM-1533] – Database Schema Cleanup [\#1647](https://github.com/pegasus-isi/pegasus/issues/1647)

54) [PM-1537] – update existing workflow tests to use yaml [\#1651](https://github.com/pegasus-isi/pegasus/issues/1651)

55) [PM-1547] – for hierarchical workflows, sc and tc cannot be inlined into the workflow file [\#1661](https://github.com/pegasus-isi/pegasus/issues/1661)

56) [PM-1602] – Decaf development for the tess_dense example [\#1716](https://github.com/pegasus-isi/pegasus/issues/1716)

57) [PM-1663] – remove old grid types from schema and add slurm to scheduler type [\#1777](https://github.com/pegasus-isi/pegasus/issues/1777)

58) [PM-1679] – pegasus-s3 mkdir should cleanly exit if bucket already exists and is owned by user [\#1793](https://github.com/pegasus-isi/pegasus/issues/1793)

59) [PM-1689] – pegasus-rc-client delete semantics [\#1803](https://github.com/pegasus-isi/pegasus/issues/1803)

60) [PM-1692] – Add any missing options from pegasus-plan cli to Workflow.plan() and Client.plan() [\#1806](https://github.com/pegasus-isi/pegasus/issues/1806)

61) [PM-1697] – Add major and minor number options to pegasus-version [\#1811](https://github.com/pegasus-isi/pegasus/issues/1811)

62) [PM-643] – Better support for stdout of clustered jobs [\#761](https://github.com/pegasus-isi/pegasus/issues/761)

63) [PM-1049] – Jobs should not be retried immediately, but rather delayed for some time [\#1163](https://github.com/pegasus-isi/pegasus/issues/1163)

64) [PM-1170] – statistics should include memory details [\#1284](https://github.com/pegasus-isi/pegasus/issues/1284)

65) [PM-1232] – Allow for multiple output sites [\#1346](https://github.com/pegasus-isi/pegasus/issues/1346)

66) [PM-1235] – Python 2/3 compatible code [\#1349](https://github.com/pegasus-isi/pegasus/issues/1349)

67) [PM-1247] – Revisit release-tools/get-system-python [\#1361](https://github.com/pegasus-isi/pegasus/issues/1361)

68) [PM-1321] – Move transfer staging into the container rather than the host OS [\#1435](https://github.com/pegasus-isi/pegasus/issues/1435)

69) [PM-1323] – pegasus transfer should not try and transfer a file that does not exist [\#1437](https://github.com/pegasus-isi/pegasus/issues/1437)

70) [PM-1324] – pegasus plan should be able to create site scratch directories with unique names [\#1438](https://github.com/pegasus-isi/pegasus/issues/1438)

71) [PM-1329] – pegasus integrity causes LIGO workflows to fail [\#1443](https://github.com/pegasus-isi/pegasus/issues/1443)

72) [PM-1338] – Add support for TACC wrangler to pegasus-init [\#1452](https://github.com/pegasus-isi/pegasus/issues/1452)

73) [PM-1341] – Transition from vendored `configobj` to release [\#1455](https://github.com/pegasus-isi/pegasus/issues/1455)

74) [PM-1344] – update pam usage by pamela [\#1458](https://github.com/pegasus-isi/pegasus/issues/1458)

75) [PM-1349] – Improve error message when jobs fail due to deep dir. structure with depth of > 20. [\#1463](https://github.com/pegasus-isi/pegasus/issues/1463)

76) [PM-1356] – Replace Google CDN with a different CDN as China block it [\#1470](https://github.com/pegasus-isi/pegasus/issues/1470)

77) [PM-1359] – Don’t support Chinese character in the file path [\#1473](https://github.com/pegasus-isi/pegasus/issues/1473)

78) [PM-1363] – Condor Configuration MOUNT_UNDER_SCRATCH causes pegasus auxiliary jobs to fail [\#1477](https://github.com/pegasus-isi/pegasus/issues/1477)

79) [PM-1368] – Implement Catch and Release for integrity errors [\#1482](https://github.com/pegasus-isi/pegasus/issues/1482)

80) [PM-1373] – Debian Buster no longer provides openjdk-8-jdk [\#1487](https://github.com/pegasus-isi/pegasus/issues/1487)

81) [PM-1374] – make monitord resilient to dagman logging the debug level in dagman.out [\#1488](https://github.com/pegasus-isi/pegasus/issues/1488)

82) [PM-1375] – Do not run integrity checks on symlinked files [\#1489](https://github.com/pegasus-isi/pegasus/issues/1489)

83) [PM-1385] – Prioritize transfers bases on dependencies [\#1499](https://github.com/pegasus-isi/pegasus/issues/1499)

84) [PM-1386] – bypass should be a per-file option [\#1500](https://github.com/pegasus-isi/pegasus/issues/1500)

85) [PM-1391] – Allow properties to be set via environment variables. [\#1505](https://github.com/pegasus-isi/pegasus/issues/1505)

86) [PM-1392] – YAML based braindump file [\#1506](https://github.com/pegasus-isi/pegasus/issues/1506)

87) [PM-1403] – Support POWER9 nodes in PMC [\#1517](https://github.com/pegasus-isi/pegasus/issues/1517)

88) [PM-1417] – add type field in job, dax, and dag [\#1531](https://github.com/pegasus-isi/pegasus/issues/1531)

89) [PM-1418] – No code to handle postgresql backup. [\#1532](https://github.com/pegasus-isi/pegasus/issues/1532)

90) [PM-1427] – remove STAT profile namespace [\#1541](https://github.com/pegasus-isi/pegasus/issues/1541)

91) [PM-1429] – Introduce PEGASUS_ENV variable to define mode of workflow i.e. development, production, etc [\#1543](https://github.com/pegasus-isi/pegasus/issues/1543)

92) [PM-1431] – Remove -0 suffixes from generated code files. [\#1545](https://github.com/pegasus-isi/pegasus/issues/1545)

93) [PM-1432] – Remove deprecated pegasus-plan CLI args [\#1546](https://github.com/pegasus-isi/pegasus/issues/1546)

94) [PM-1459] – Upgrade Java Unit Test Setup [\#1573](https://github.com/pegasus-isi/pegasus/issues/1573)

95) [PM-1462] – Remove pegasus-submit-dag [\#1576](https://github.com/pegasus-isi/pegasus/issues/1576)

96) [PM-1463] – improve insert performance of pegasus-rc-client [\#1577](https://github.com/pegasus-isi/pegasus/issues/1577)

97) [PM-1472] – Update pegasus-s3 regions [\#1586](https://github.com/pegasus-isi/pegasus/issues/1586)

98) [PM-1474] – Extend command line tools with –json option. [\#1588](https://github.com/pegasus-isi/pegasus/issues/1588)

99) [PM-1476] – Explore deprecation/replacement of Perl codebase [\#1590](https://github.com/pegasus-isi/pegasus/issues/1590)

100) [PM-1481] – add status progress bar to python client code [\#1595](https://github.com/pegasus-isi/pegasus/issues/1595)

101) [PM-1489] – Implement a generic data access credential handler [\#1603](https://github.com/pegasus-isi/pegasus/issues/1603)

102) [PM-1490] – Support Webdav for data transfers [\#1604](https://github.com/pegasus-isi/pegasus/issues/1604)

103) [PM-1508] – in the python api, the method signature to manually add dependencies needs to be updated [\#1622](https://github.com/pegasus-isi/pegasus/issues/1622)

104) [PM-1512] – Removed unused code from planner codebase [\#1626](https://github.com/pegasus-isi/pegasus/issues/1626)

105) [PM-1514] – Move to boto3 and port p-s3 [\#1628](https://github.com/pegasus-isi/pegasus/issues/1628)

106) [PM-1527] – p-status shows failure when state is unknown [\#1641](https://github.com/pegasus-isi/pegasus/issues/1641)

107) [PM-1532] – python client “plan” should accept –sites as a list needs a –staging-site flag [\#1646](https://github.com/pegasus-isi/pegasus/issues/1646)

108) [PM-1539] – Support PANDA GAHP – allow condorio for glite style [\#1653](https://github.com/pegasus-isi/pegasus/issues/1653)

109) [PM-1544] – pegasus-transfer logs skips container verification when retrieving from singularity hub [\#1658](https://github.com/pegasus-isi/pegasus/issues/1658)

110) [PM-1545] – handling metadata in 5.0 [\#1659](https://github.com/pegasus-isi/pegasus/issues/1659)

111) [PM-1550] – resolve relative input-dir and output-dir options for dax jobs in hierarchical workflows [\#1664](https://github.com/pegasus-isi/pegasus/issues/1664)

112) [PM-1580] – build packages for ubuntu 20 for 5.0 [\#1694](https://github.com/pegasus-isi/pegasus/issues/1694)

113) [PM-1581] – pegasus-integrity callout from kickstart [\#1695](https://github.com/pegasus-isi/pegasus/issues/1695)

114) [PM-1584] – 5.0 Python Api Improvements [\#1698](https://github.com/pegasus-isi/pegasus/issues/1698)

115) [PM-1592] – Consistent UTF8 environment across full workflow [\#1706](https://github.com/pegasus-isi/pegasus/issues/1706)

116) [PM-1594] – Short circuit p-transfer in the case of excessive failure in large transfers [\#1708](https://github.com/pegasus-isi/pegasus/issues/1708)

117) [PM-1621] – Basic support for gpus in Docker and Singularity containers [\#1735](https://github.com/pegasus-isi/pegasus/issues/1735)

118) [PM-1622] – /bin/sh compatibility [\#1736](https://github.com/pegasus-isi/pegasus/issues/1736)

119) [PM-1626] – Allow users to specify arbitrary cli arguments for containers [\#1740](https://github.com/pegasus-isi/pegasus/issues/1740)

120) [PM-1647] – Add more profile keys in the add_condor_profile [\#1761](https://github.com/pegasus-isi/pegasus/issues/1761)

121) [PM-1650] – replace –dax with a positional argument [\#1764](https://github.com/pegasus-isi/pegasus/issues/1764)

122) [PM-1651] – Add more profile keys in the add_pegasus_profile [\#1765](https://github.com/pegasus-isi/pegasus/issues/1765)

123) [PM-1654] – pick up workflow api from vendor extensions encoded in the workflow [\#1768](https://github.com/pegasus-isi/pegasus/issues/1768)

124) [PM-1657] – a pre-flight check needs to be done for the python package attrs [\#1771](https://github.com/pegasus-isi/pegasus/issues/1771)

125) [PM-1674] – Deprecate hints profile namespace and use selector namespace [\#1788](https://github.com/pegasus-isi/pegasus/issues/1788)

126) [PM-1687] – disable warnings when validating yaml files [\#1801](https://github.com/pegasus-isi/pegasus/issues/1801)

127) [PM-1691] – Add pre script hook to hub repos [\#1805](https://github.com/pegasus-isi/pegasus/issues/1805)

128) [PM-1236] – Compatibility Module [\#1350](https://github.com/pegasus-isi/pegasus/issues/1350)

129) [PM-1237] – Python 3: Pegasus Dashboard [\#1351](https://github.com/pegasus-isi/pegasus/issues/1351)

130) [PM-1238] – Python 3: Pegasus Monitord [\#1352](https://github.com/pegasus-isi/pegasus/issues/1352)

131) [PM-1239] – Python 2/3 Compatible: Pegasus Transfer [\#1353](https://github.com/pegasus-isi/pegasus/issues/1353)

132) [PM-1240] – Python 3: Pegasus Statistics [\#1354](https://github.com/pegasus-isi/pegasus/issues/1354)

133) [PM-1241] – Python 3: Pegasus DB Admin [\#1355](https://github.com/pegasus-isi/pegasus/issues/1355)

134) [PM-1242] – Python 3: Pegasus Metadata [\#1356](https://github.com/pegasus-isi/pegasus/issues/1356)

135) [PM-1328] – support sharedfs on the compute site as staging site [\#1442](https://github.com/pegasus-isi/pegasus/issues/1442)

136) [PM-1340] – make planner os releases consistent with builds [\#1454](https://github.com/pegasus-isi/pegasus/issues/1454)

137) [PM-1353] – update monitord to parse both xml and yams based ks records [\#1467](https://github.com/pegasus-isi/pegasus/issues/1467)

138) [PM-1361] – create the schema for a YAML based Site Catalog [\#1475](https://github.com/pegasus-isi/pegasus/issues/1475)

139) [PM-1365] – remove __ from event keys wherever possible [\#1479](https://github.com/pegasus-isi/pegasus/issues/1479)

140) [PM-1371] – Python 3 Compatible: DAX [\#1485](https://github.com/pegasus-isi/pegasus/issues/1485)

141) [PM-1372] – Python 3 Compatible: pegasus-exitcode [\#1486](https://github.com/pegasus-isi/pegasus/issues/1486)

142) [PM-1381] – Associated planner changes to handle LSF sites [\#1495](https://github.com/pegasus-isi/pegasus/issues/1495)

143) [PM-1387] – make the netlogger events consistent with the documentation [\#1501](https://github.com/pegasus-isi/pegasus/issues/1501)

144) [PM-1404] – pegasus tutorial for summit from Kubernetes [\#1518](https://github.com/pegasus-isi/pegasus/issues/1518)

145) [PM-1407] – Python 3: Netlogger Monitord Code [\#1521](https://github.com/pegasus-isi/pegasus/issues/1521)

146) [PM-1408] – Python 3: Pegasus Analyzer [\#1522](https://github.com/pegasus-isi/pegasus/issues/1522)

147) [PM-1410] – create the schema for a YAML based Replica Catalog [\#1524](https://github.com/pegasus-isi/pegasus/issues/1524)

148) [PM-1419] – Java [\#1533](https://github.com/pegasus-isi/pegasus/issues/1533)

149) [PM-1420] – add checksum field for Containers in the TransformationCatalog schema [\#1534](https://github.com/pegasus-isi/pegasus/issues/1534)

150) [PM-1422] – integrate the client code into the dax api [\#1536](https://github.com/pegasus-isi/pegasus/issues/1536)

151) [PM-1423] – refactor and improve tests [\#1537](https://github.com/pegasus-isi/pegasus/issues/1537)

152) [PM-1428] – change dax and dag to subworkflow [\#1542](https://github.com/pegasus-isi/pegasus/issues/1542)

153) [PM-1430] – ensure that api can write out unicode characters in utf-8 [\#1544](https://github.com/pegasus-isi/pegasus/issues/1544)

154) [PM-1433] – Remove old SC RC TC catalog versions [\#1547](https://github.com/pegasus-isi/pegasus/issues/1547)

155) [PM-1435] – create a “moving from dax3 to new api” doc [\#1549](https://github.com/pegasus-isi/pegasus/issues/1549)

156) [PM-1436] – implement method chaining using decorators [\#1550](https://github.com/pegasus-isi/pegasus/issues/1550)

157) [PM-1437] – add type annotations [\#1551](https://github.com/pegasus-isi/pegasus/issues/1551)

158) [PM-1439] – Planner support for YAML based Site Catalog [\#1553](https://github.com/pegasus-isi/pegasus/issues/1553)

159) [PM-1441] – Python [\#1555](https://github.com/pegasus-isi/pegasus/issues/1555)

160) [PM-1442] – Integrate Check in CI [\#1556](https://github.com/pegasus-isi/pegasus/issues/1556)

161) [PM-1443] – implement api for pegasus.conf [\#1557](https://github.com/pegasus-isi/pegasus/issues/1557)

162) [PM-1445] – update monitord to parse yaml based brain dump file [\#1559](https://github.com/pegasus-isi/pegasus/issues/1559)

163) [PM-1447] – update pegasus-sc-converter to covert old format catalog to new yaml based one [\#1561](https://github.com/pegasus-isi/pegasus/issues/1561)

164) [PM-1448] – SiteFactory should auto detect version and load the correct implementation [\#1562](https://github.com/pegasus-isi/pegasus/issues/1562)

165) [PM-1450] – Module to load/dump YAML files [\#1564](https://github.com/pegasus-isi/pegasus/issues/1564)

166) [PM-1453] – Module to load/dump Workflow(DAX) files [\#1567](https://github.com/pegasus-isi/pegasus/issues/1567)

167) [PM-1454] – Module to load/dump Replica Catalog files [\#1568](https://github.com/pegasus-isi/pegasus/issues/1568)

168) [PM-1455] – Module to load/dump Transformation Catalog files [\#1569](https://github.com/pegasus-isi/pegasus/issues/1569)

169) [PM-1456] – Module to load/dump Site Catalog files [\#1570](https://github.com/pegasus-isi/pegasus/issues/1570)

170) [PM-1457] – Module to load/dump Properties files [\#1571](https://github.com/pegasus-isi/pegasus/issues/1571)

171) [PM-1460] – add docs to user guide [\#1574](https://github.com/pegasus-isi/pegasus/issues/1574)

172) [PM-1464] – disallow variable expansion while converting site catalog from one format to another [\#1578](https://github.com/pegasus-isi/pegasus/issues/1578)

173) [PM-1465] – Module to load/dump JSON files [\#1579](https://github.com/pegasus-isi/pegasus/issues/1579)

174) [PM-1468] – remove catalog [\#1582](https://github.com/pegasus-isi/pegasus/issues/1582)

175) [PM-1469] – update Pegasus-DAX3-Tutorial.ipynb [\#1583](https://github.com/pegasus-isi/pegasus/issues/1583)

176) [PM-1471] – update jupyter docs [\#1585](https://github.com/pegasus-isi/pegasus/issues/1585)

177) [PM-1473] – fix logging bug [\#1587](https://github.com/pegasus-isi/pegasus/issues/1587)

178) [PM-1475] – Pegasus Plan [\#1589](https://github.com/pegasus-isi/pegasus/issues/1589)

179) [PM-1477] – pegasus-config [\#1591](https://github.com/pegasus-isi/pegasus/issues/1591)

180) [PM-1478] – pegasus-remove [\#1592](https://github.com/pegasus-isi/pegasus/issues/1592)

181) [PM-1479] – pegasus-run [\#1593](https://github.com/pegasus-isi/pegasus/issues/1593)

182) [PM-1480] – pegasus-status [\#1594](https://github.com/pegasus-isi/pegasus/issues/1594)

183) [PM-1482] – Clean up javadoc warnings [\#1596](https://github.com/pegasus-isi/pegasus/issues/1596)

184) [PM-1483] – rename ProfileMixin.add_<ns>() to ProfileMixin.add_profile_<ns>() [\#1597](https://github.com/pegasus-isi/pegasus/issues/1597)

185) [PM-1487] – update python api to write new default filenames [\#1601](https://github.com/pegasus-isi/pegasus/issues/1601)

186) [PM-1491] – Update pegasus-tc-converter to convert from old format to new YAML format [\#1605](https://github.com/pegasus-isi/pegasus/issues/1605)

187) [PM-1492] – support docker/singularity container usage in cwl [\#1606](https://github.com/pegasus-isi/pegasus/issues/1606)

188) [PM-1494] – Parse YAML Based RC files [\#1608](https://github.com/pegasus-isi/pegasus/issues/1608)

189) [PM-1496] – update rc schema so that rc entries can have checksum fields [\#1610](https://github.com/pegasus-isi/pegasus/issues/1610)

190) [PM-1497] – update rc api to support checksum fields [\#1611](https://github.com/pegasus-isi/pegasus/issues/1611)

191) [PM-1498] – update RC docs to mention checksum values [\#1612](https://github.com/pegasus-isi/pegasus/issues/1612)

192) [PM-1499] – Implement YAML Based RC Backend [\#1613](https://github.com/pegasus-isi/pegasus/issues/1613)

193) [PM-1500] – create workflow test [\#1614](https://github.com/pegasus-isi/pegasus/issues/1614)

194) [PM-1502] – add infer_dependencies=True to wf.write [\#1616](https://github.com/pegasus-isi/pegasus/issues/1616)

195) [PM-1503] – in the python api, write out yml in order [\#1617](https://github.com/pegasus-isi/pegasus/issues/1617)

196) [PM-1504] – flatten out the uses section. remove the extra file property aggregation [\#1618](https://github.com/pegasus-isi/pegasus/issues/1618)

197) [PM-1505] – for job arguments, files should be added as a strings [\#1619](https://github.com/pegasus-isi/pegasus/issues/1619)

198) [PM-1507] – update schema for stdin, stdout, stderr in jobs to s.t only lfn is used [\#1621](https://github.com/pegasus-isi/pegasus/issues/1621)

199) [PM-1509] – update “type” field options in the AbstractJob schema to be “job”, “pegasusWorkflow”, and “condorWorkflow” [\#1623](https://github.com/pegasus-isi/pegasus/issues/1623)

200) [PM-1511] – Update DAXParser Factory to load the right parser based on content of input dax file [\#1625](https://github.com/pegasus-isi/pegasus/issues/1625)

201) [PM-1515] – prefer catalog entries for SC, RC and TC in the DAX over everything else [\#1629](https://github.com/pegasus-isi/pegasus/issues/1629)

202) [PM-1517] – update default –sites option in python client planner wrapper code [\#1631](https://github.com/pegasus-isi/pegasus/issues/1631)

203) [PM-1518] – Update Replica Factory to auto load RC backend based on type of file [\#1632](https://github.com/pegasus-isi/pegasus/issues/1632)

204) [PM-1519] – update schema for job args s.t. types can be strings and scalars [\#1633](https://github.com/pegasus-isi/pegasus/issues/1633)

205) [PM-1520] – Update Transformation Factory to auto load Transformation backend based on type of file [\#1634](https://github.com/pegasus-isi/pegasus/issues/1634)

206) [PM-1523] – work on replica catalog converter [\#1637](https://github.com/pegasus-isi/pegasus/issues/1637)

207) [PM-1525] – support in planner for compound transformations [\#1639](https://github.com/pegasus-isi/pegasus/issues/1639)

208) [PM-1526] – update python api to write out tr requirements in the format “namespace::name:version” [\#1640](https://github.com/pegasus-isi/pegasus/issues/1640)

209) [PM-1535] – default paths picked up should be logged in the properties file in the submit directory [\#1649](https://github.com/pegasus-isi/pegasus/issues/1649)

210) [PM-1538] – convert test 023-sc4-ssh-http to use yaml [\#1652](https://github.com/pegasus-isi/pegasus/issues/1652)

211) [PM-1540] – allow mount point regex to parse shell variable names [\#1654](https://github.com/pegasus-isi/pegasus/issues/1654)

212) [PM-1541] – change the pegasus-plan invocation via pegasus lite in dagman prescripts to handle new worker package organization [\#1655](https://github.com/pegasus-isi/pegasus/issues/1655)

213) [PM-1542] – convert test 024-sc4-gridftp-http to use yaml [\#1656](https://github.com/pegasus-isi/pegasus/issues/1656)

214) [PM-1543] – convert test 025-sc4-file-http to use yaml [\#1657](https://github.com/pegasus-isi/pegasus/issues/1657)

215) [PM-1546] – add metadata to entry in replica catalog [\#1660](https://github.com/pegasus-isi/pegasus/issues/1660)

216) [PM-1548] – preserve case for property keys when properties python api is used [\#1662](https://github.com/pegasus-isi/pegasus/issues/1662)

217) [PM-1549] – registration of outputs in Pegasus 5.0 [\#1663](https://github.com/pegasus-isi/pegasus/issues/1663)

218) [PM-1555] – Chapter 2. Tutorial – Update and review [\#1669](https://github.com/pegasus-isi/pegasus/issues/1669)

219) [PM-1556] – Chapter 3. Installation – Update and review [\#1670](https://github.com/pegasus-isi/pegasus/issues/1670)

220) [PM-1557] – Chapter 4. Creating Workflows – Update and review [\#1671](https://github.com/pegasus-isi/pegasus/issues/1671)

221) [PM-1558] – Chapter 5. Running Workflows – Update and review [\#1672](https://github.com/pegasus-isi/pegasus/issues/1672)

222) [PM-1559] – Chapter 6. Monitoring, Debugging and Statistics – Update and review [\#1673](https://github.com/pegasus-isi/pegasus/issues/1673)

223) [PM-1560] – Chapter 7. Execution Environments – Update and review [\#1674](https://github.com/pegasus-isi/pegasus/issues/1674)

224) [PM-1561] – Chapter 8. Containers – Update and review [\#1675](https://github.com/pegasus-isi/pegasus/issues/1675)

225) [PM-1562] – Chapter 9. Example Workflows – Update and review [\#1676](https://github.com/pegasus-isi/pegasus/issues/1676)

226) [PM-1563] – Chapter 10. Data Management – Update and review [\#1677](https://github.com/pegasus-isi/pegasus/issues/1677)

227) [PM-1564] – Chapter 11. Optimizing Workflows for Efficiency and Scalability – Update and review [\#1678](https://github.com/pegasus-isi/pegasus/issues/1678)

228) [PM-1565] – Chapter 12. Pegasus Service – Update and review [\#1679](https://github.com/pegasus-isi/pegasus/issues/1679)

229) [PM-1566] – Chapter 13. Configuration – Update and review [\#1680](https://github.com/pegasus-isi/pegasus/issues/1680)

230) [PM-1567] – Chapter 14. Submit Directory Details – Update and review [\#1681](https://github.com/pegasus-isi/pegasus/issues/1681)

231) [PM-1568] – Chapter 15. Jupyter Notebooks – Update and review [\#1682](https://github.com/pegasus-isi/pegasus/issues/1682)

232) [PM-1569] – Chapter 16. API Reference – Update and review [\#1683](https://github.com/pegasus-isi/pegasus/issues/1683)

233) [PM-1570] – Chapter Command Line Tools man pages – Update and review [\#1684](https://github.com/pegasus-isi/pegasus/issues/1684)

234) [PM-1571] – rewrite Introduction [\#1685](https://github.com/pegasus-isi/pegasus/issues/1685)

235) [PM-1572] – Chapter “Packages” – Update and review [\#1686](https://github.com/pegasus-isi/pegasus/issues/1686)

236) [PM-1573] – Chapter 17. Useful Tips – Update and review [\#1687](https://github.com/pegasus-isi/pegasus/issues/1687)

237) [PM-1574] – Chapter 18. Funding, citing, and anonymous usage statistics – Update and review [\#1688](https://github.com/pegasus-isi/pegasus/issues/1688)

238) [PM-1575] – Chapter 19. Glossary – Update and review [\#1689](https://github.com/pegasus-isi/pegasus/issues/1689)

239) [PM-1576] – Chapter 20. Tutorial VM – Update and review [\#1690](https://github.com/pegasus-isi/pegasus/issues/1690)

240) [PM-1577] – convert test 045-hierarchy-sharedfs [\#1691](https://github.com/pegasus-isi/pegasus/issues/1691)

241) [PM-1578] – Migration Guide to 5.0 [\#1692](https://github.com/pegasus-isi/pegasus/issues/1692)

242) [PM-1579] – convert test 045-hierarchy-sharedfs-b to use yaml [\#1693](https://github.com/pegasus-isi/pegasus/issues/1693)

243) [PM-1582] – ensure checksum and file metadata also appears in the output replica catalog [\#1696](https://github.com/pegasus-isi/pegasus/issues/1696)

244) [PM-1583] – Parse meta files as a RC backend in the planner [\#1697](https://github.com/pegasus-isi/pegasus/issues/1697)

245) [PM-1585] – remove glibc from schema and api as it is not used anymore [\#1699](https://github.com/pegasus-isi/pegasus/issues/1699)

246) [PM-1586] – add built-in support for pathlib.Path objects where ever paths are used [\#1700](https://github.com/pegasus-isi/pegasus/issues/1700)

247) [PM-1587] – _DirectoryType enums should have underscores in name [\#1701](https://github.com/pegasus-isi/pegasus/issues/1701)

248) [PM-1588] – in add_pegasus_profile(), add data_configuration as a kwarg [\#1702](https://github.com/pegasus-isi/pegasus/issues/1702)

249) [PM-1590] – in the Workflow object set infer_dependencies to be True by default [\#1704](https://github.com/pegasus-isi/pegasus/issues/1704)

250) [PM-1591] – Only require site and pfn in Transformation constructor when automatically creating a TransformationSite [\#1705](https://github.com/pegasus-isi/pegasus/issues/1705)

251) [PM-1596] – pegasus-db-admin should use PEGASUS_HOME to discover pegasus-version etc [\#1710](https://github.com/pegasus-isi/pegasus/issues/1710)

252) [PM-1597] – Pegasus Run [\#1711](https://github.com/pegasus-isi/pegasus/issues/1711)

253) [PM-1598] – Output originating from pegasus-tools should be output by workflow object as is (without any log category) [\#1712](https://github.com/pegasus-isi/pegasus/issues/1712)

254) [PM-1599] – Improve exception handling for failed execution of pegasus client commands [\#1713](https://github.com/pegasus-isi/pegasus/issues/1713)

255) [PM-1600] – Update workflow and client python apis to support multiple output sites [\#1714](https://github.com/pegasus-isi/pegasus/issues/1714)

256) [PM-1601] – Client plan input_dir must take in a list of str [\#1715](https://github.com/pegasus-isi/pegasus/issues/1715)

257) [PM-1604] – Fix deprecation warnings [\#1718](https://github.com/pegasus-isi/pegasus/issues/1718)

258) [PM-1605] – Get ensemble manager running on master (Python3) [\#1719](https://github.com/pegasus-isi/pegasus/issues/1719)

259) [PM-1606] – Merge in/factor in code implemented in add-ensemble-triggers branch [\#1720](https://github.com/pegasus-isi/pegasus/issues/1720)

260) [PM-1607] – Add time interval based triggering on a given directory/file pattern [\#1721](https://github.com/pegasus-isi/pegasus/issues/1721)

261) [PM-1609] – register_replica should be set to True by default [\#1723](https://github.com/pegasus-isi/pegasus/issues/1723)

262) [PM-1610] – ensure pegasus-db-admin downgrade works [\#1724](https://github.com/pegasus-isi/pegasus/issues/1724)

263) [PM-1611] – add pegasus-graphviz functionality into the api [\#1725](https://github.com/pegasus-isi/pegasus/issues/1725)

264) [PM-1612] – update monitord to record avg cpu utilization and maxrss [\#1726](https://github.com/pegasus-isi/pegasus/issues/1726)

265) [PM-1613] – update database schema to track maxrss and avg_cpu [\#1727](https://github.com/pegasus-isi/pegasus/issues/1727)

266) [PM-1614] – update planner for revised 5.0 replica catalog format [\#1728](https://github.com/pegasus-isi/pegasus/issues/1728)

267) [PM-1615] – unique constraint failed error when using multiple output sites [\#1729](https://github.com/pegasus-isi/pegasus/issues/1729)

268) [PM-1616] – add checksums for executables [\#1730](https://github.com/pegasus-isi/pegasus/issues/1730)

269) [PM-1617] – add missing integrity check for executables [\#1731](https://github.com/pegasus-isi/pegasus/issues/1731)

270) [PM-1618] – update 039-black-metadata to use python 5.0 api [\#1732](https://github.com/pegasus-isi/pegasus/issues/1732)

271) [PM-1623] – fix 5.0 python auto generated python api documentation [\#1737](https://github.com/pegasus-isi/pegasus/issues/1737)

272) [PM-1624] – convert 032-black-checkpoint [\#1738](https://github.com/pegasus-isi/pegasus/issues/1738)

273) [PM-1625] – Pegasus specific profile for requesting GPU resources [\#1739](https://github.com/pegasus-isi/pegasus/issues/1739)

274) [PM-1627] – workflow uuid, submit dir, submit hostname, root wf id should be accessible from the workflow object [\#1741](https://github.com/pegasus-isi/pegasus/issues/1741)

275) [PM-1628] – files generated by the api should have comments specifying that they have been auto generated by the api [\#1742](https://github.com/pegasus-isi/pegasus/issues/1742)

276) [PM-1629] – Add metadata to container [\#1743](https://github.com/pegasus-isi/pegasus/issues/1743)

277) [PM-1631] – create 032-kickstart-chkpoint-signal-condorio [\#1745](https://github.com/pegasus-isi/pegasus/issues/1745)

278) [PM-1632] – create 032-kickstart-chkpoint-signal-nonsharedfs [\#1746](https://github.com/pegasus-isi/pegasus/issues/1746)

279) [PM-1633] – CLI: manpage pegasus-plan [\#1747](https://github.com/pegasus-isi/pegasus/issues/1747)

280) [PM-1634] – CLI: manpage pegasus-db-admin [\#1748](https://github.com/pegasus-isi/pegasus/issues/1748)

281) [PM-1635] – CLI: manpage pegasus-status [\#1749](https://github.com/pegasus-isi/pegasus/issues/1749)

282) [PM-1636] – CLI: manpage pegasus-remove [\#1750](https://github.com/pegasus-isi/pegasus/issues/1750)

283) [PM-1637] – CLI: manpage pegasus-anaylzer [\#1751](https://github.com/pegasus-isi/pegasus/issues/1751)

284) [PM-1638] – CLI: manpage pegasus-statistics [\#1752](https://github.com/pegasus-isi/pegasus/issues/1752)

285) [PM-1639] – CLI: manpage pegasus-run [\#1753](https://github.com/pegasus-isi/pegasus/issues/1753)

286) [PM-1640] – CLI: manpage pegasus-transfer [\#1754](https://github.com/pegasus-isi/pegasus/issues/1754)

287) [PM-1641] – CLI: manpage pegasus-s3 [\#1755](https://github.com/pegasus-isi/pegasus/issues/1755)

288) [PM-1642] – CLI: manpage pegasus-integrity [\#1756](https://github.com/pegasus-isi/pegasus/issues/1756)

289) [PM-1643] – CLI: manpage pegasus-rc-converter [\#1757](https://github.com/pegasus-isi/pegasus/issues/1757)

290) [PM-1644] – CLI: manpage pegasus-sc-converter [\#1758](https://github.com/pegasus-isi/pegasus/issues/1758)

291) [PM-1645] – CLI: manpage pegasus-tc-converter [\#1759](https://github.com/pegasus-isi/pegasus/issues/1759)

292) [PM-1646] – update database overview, and update schema picture [\#1760](https://github.com/pegasus-isi/pegasus/issues/1760)

293) [PM-1648] – add unit tests that put/pull to/from aws s3 and ceph [\#1762](https://github.com/pegasus-isi/pegasus/issues/1762)

294) [PM-1649] – remove old config parameters if they are not used [\#1763](https://github.com/pegasus-isi/pegasus/issues/1763)

295) [PM-1652] – update pegasus client api to pass the workflow to be planned at the end [\#1766](https://github.com/pegasus-isi/pegasus/issues/1766)

296) [PM-1655] – the metrics server should pick up key wf_api , and default back to dax_api if not present [\#1769](https://github.com/pegasus-isi/pegasus/issues/1769)

297) [PM-1656] – Add unit tests to ensure YAML is being generated correctly [\#1770](https://github.com/pegasus-isi/pegasus/issues/1770)

298) [PM-1659] – in the python api, add pegasus profile key container.arguments [\#1773](https://github.com/pegasus-isi/pegasus/issues/1773)

299) [PM-1660] – add boolean bypass flag for input files [\#1774](https://github.com/pegasus-isi/pegasus/issues/1774)

300) [PM-1661] – expose bypass parameter for Containers and TransformationSites [\#1775](https://github.com/pegasus-isi/pegasus/issues/1775)

301) [PM-1662] – document behavior in user guide about bypassing file staging [\#1776](https://github.com/pegasus-isi/pegasus/issues/1776)

302) [PM-1665] – expose –randomdir/–randomdir=<path> in client code in the python api [\#1779](https://github.com/pegasus-isi/pegasus/issues/1779)

303) [PM-1666] – get live output from pegasus cli tools when they are called with the client code [\#1780](https://github.com/pegasus-isi/pegasus/issues/1780)

304) [PM-1667] – when client code is called, it should be more obvious what pegasus-<tool> is being called [\#1781](https://github.com/pegasus-isi/pegasus/issues/1781)

305) [PM-1668] – fb-nlp workflow generator creates multiple jobs that create same output file [\#1782](https://github.com/pegasus-isi/pegasus/issues/1782)

306) [PM-1669] – remove “pegasus: <version>” from tc when inlined in a workflow [\#1783](https://github.com/pegasus-isi/pegasus/issues/1783)

307) [PM-1670] – expose a schema validation function that can be used in unit tests [\#1784](https://github.com/pegasus-isi/pegasus/issues/1784)

308) [PM-1682] – add –reuse option to Workflow.plan in python api [\#1796](https://github.com/pegasus-isi/pegasus/issues/1796)

309) [PM-1683] – update section 7.3 supported transfer protocols in data management guide [\#1797](https://github.com/pegasus-isi/pegasus/issues/1797)

310) [PM-1684] – reorg table of contents [\#1798](https://github.com/pegasus-isi/pegasus/issues/1798)

311) [PM-1688] – update pegasus-db-admin as a new ‘trigger’ table has been added to the schema [\#1802](https://github.com/pegasus-isi/pegasus/issues/1802)

312) [PM-1693] – convert test 010-runtime-clustering-CondorIO to use python api [\#1807](https://github.com/pegasus-isi/pegasus/issues/1807)

313) [PM-1694] – convert test 010-runtime-clustering-Non-SharedFS to use python api [\#1808](https://github.com/pegasus-isi/pegasus/issues/1808)

314) [PM-1695] – convert 010-runtime-clustering-SharedFS to use python api [\#1809](https://github.com/pegasus-isi/pegasus/issues/1809)

315) [PM-1696] – convert 010-runtime-clustering-SharedFS Staging and No Kickstart to use python api [\#1810](https://github.com/pegasus-isi/pegasus/issues/1810)

316) [PM-1698] – Release notes for 5.0 release [\#1812](https://github.com/pegasus-isi/pegasus/issues/1812)

317) [PM-1699] – add logo the documentation pages [\#1813](https://github.com/pegasus-isi/pegasus/issues/1813)

318) [PM-1700] – add SIGINT handler to Client.wait() so that it can exit cleanly [\#1814](https://github.com/pegasus-isi/pegasus/issues/1814)

319) [PM-1701] – planner should get chmod jobs to run locally if compute site has auxiliary.local set [\#1815](https://github.com/pegasus-isi/pegasus/issues/1815)

#### Bugs Fixed

1) [PM-1150] – Pegasus should verify that required credentials exists before starting a workflow [\#1264](https://github.com/pegasus-isi/pegasus/issues/1264)

2) [PM-1192] – User supplied env setup script for lite [\#1306](https://github.com/pegasus-isi/pegasus/issues/1306)

3) [PM-1199] – Notification naming / meta notifications [\#1313](https://github.com/pegasus-isi/pegasus/issues/1313)

4) [PM-1326] – singularity suffix computed incorrectly [\#1440](https://github.com/pegasus-isi/pegasus/issues/1440)

5) [PM-1327] – bypass input file staging broken for container execution [\#1441](https://github.com/pegasus-isi/pegasus/issues/1441)

6) [PM-1330] – .meta files created even when integrity checking is disabled. [\#1444](https://github.com/pegasus-isi/pegasus/issues/1444)

7) [PM-1332] – monitord is failing on a dagman.out file [\#1446](https://github.com/pegasus-isi/pegasus/issues/1446)

8) [PM-1333] – amqp endpoint errors should not disable database population for multiplexed sinks [\#1447](https://github.com/pegasus-isi/pegasus/issues/1447)

9) [PM-1334] – pegasus dagman is not exiting cleanly [\#1448](https://github.com/pegasus-isi/pegasus/issues/1448)

10) [PM-1336] – pegasus-submitdir is broken [\#1450](https://github.com/pegasus-isi/pegasus/issues/1450)

11) [PM-1346] – Pegasus job checkpointing is incompatible with condorio [\#1460](https://github.com/pegasus-isi/pegasus/issues/1460)

12) [PM-1347] – pegasus will always try and transfer output when a code has checkpointed [\#1461](https://github.com/pegasus-isi/pegasus/issues/1461)

13) [PM-1350] – pegasus is ignoring when_to_transfer_output [\#1464](https://github.com/pegasus-isi/pegasus/issues/1464)

14) [PM-1358] – HTCondor 8.8.0/8.8.1 remaps /tmp, and can break access to x509 credentials [\#1472](https://github.com/pegasus-isi/pegasus/issues/1472)

15) [PM-1360] – planner drops transfer_(in|out)put_files if NoGridStart is used [\#1474](https://github.com/pegasus-isi/pegasus/issues/1474)

16) [PM-1362] – Chinese characters in the file path [\#1476](https://github.com/pegasus-isi/pegasus/issues/1476)

17) [PM-1366] – Pegasus Cluster Label – Job Env Not Picked Up in Containers [\#1480](https://github.com/pegasus-isi/pegasus/issues/1480)

18) [PM-1377] – A + in a tc name breaks pegasus-plan [\#1491](https://github.com/pegasus-isi/pegasus/issues/1491)

19) [PM-1379] – Stage out job fails – wrong src location [\#1493](https://github.com/pegasus-isi/pegasus/issues/1493)

20) [PM-1380] – Support for Singularity Library [\#1494](https://github.com/pegasus-isi/pegasus/issues/1494)

21) [PM-1389] – pegasus.cores causes issues on Summit [\#1503](https://github.com/pegasus-isi/pegasus/issues/1503)

22) [PM-1395] – GLite LSF scripts don’t work as intended on OLCF’s DTNs [\#1509](https://github.com/pegasus-isi/pegasus/issues/1509)

23) [PM-1405] – Is Pegasus supposed to build on 32-bit x86 (Debian i386 Stretch)? [\#1519](https://github.com/pegasus-isi/pegasus/issues/1519)

24) [PM-1409] – Python virtual environments not considered first before system wide installation [\#1523](https://github.com/pegasus-isi/pegasus/issues/1523)

25) [PM-1421] – p-lite generated sh files fail, when used with Docker containers, that make use of USER argument [\#1535](https://github.com/pegasus-isi/pegasus/issues/1535)

26) [PM-1466] – Upgrade Python Package Versions [\#1580](https://github.com/pegasus-isi/pegasus/issues/1580)

27) [PM-1488] – Get condorpool worker machine ipaddr error!!! [\#1602](https://github.com/pegasus-isi/pegasus/issues/1602)

28) [PM-1506] – pegasus-python-wrapper locates executable from PEGASUS_HOME instead of dirname of the exec. [\#1620](https://github.com/pegasus-isi/pegasus/issues/1620)

29) [PM-1521] – Decaf Jobs does not generate valid JSON for Decaf anymore [\#1635](https://github.com/pegasus-isi/pegasus/issues/1635)

30) [PM-1522] – output not being capture in pegasus client code [\#1636](https://github.com/pegasus-isi/pegasus/issues/1636)

31) [PM-1530] – site selector does map job correctly when using stageable or all mapper with a containerized job [\#1644](https://github.com/pegasus-isi/pegasus/issues/1644)

32) [PM-1531] – pegasus-db-admin fails on macosx on a clean db [\#1645](https://github.com/pegasus-isi/pegasus/issues/1645)

33) [PM-1534] – integrity checking with bypass input staging if checksums are specified in replica catalog [\#1648](https://github.com/pegasus-isi/pegasus/issues/1648)

34) [PM-1595] – hostname is not generated in case of integrity failures [\#1709](https://github.com/pegasus-isi/pegasus/issues/1709)

35) [PM-1619] – pegasus-analyzer output doesn’t match pegasus-status [\#1733](https://github.com/pegasus-isi/pegasus/issues/1733)

36) [PM-1630] – PATH variable in Docker containers is not preserved in some cases [\#1744](https://github.com/pegasus-isi/pegasus/issues/1744)

37) [PM-1671] – ConfigParser still referenced in pegasus-worker CLI scripts [\#1785](https://github.com/pegasus-isi/pegasus/issues/1785)

38) [PM-1675] – planner throws error when using “pegasus.dir.storage.mapper.replica.file” without specifying “pegasus.dir.storage.mapper.replica” [\#1789](https://github.com/pegasus-isi/pegasus/issues/1789)

39) [PM-1676] – hierarchical workflow failing only when integrity checking is on [\#1790](https://github.com/pegasus-isi/pegasus/issues/1790)

40) [PM-1680] – kickstart needs to quote argument vector when outputting it to yaml [\#1794](https://github.com/pegasus-isi/pegasus/issues/1794)

41) [PM-1702] – registration jobs fail out of a 5.0 binary install [\#1816](https://github.com/pegasus-isi/pegasus/issues/1816)

42) [PM-1705] - priorities are not propagated correctly [\#1819](https://github.com/pegasus-isi/pegasus/issues/1819)

### Pegasus 5.0.0beta1

**Release Date:** July 27, 2020

We are happy to announce the beta1 release of Pegasus 5.0.  Pegasus
5.0 will be a major release of pegasus and this beta version has most
of the features and bug fixes that will be in 5.0.

We invite our users to give it a try. Since this is a beta release,
it has to manually downloaded from the website at
https://download.pegasus.isi.edu/pegasus/5.0.0beta1/

There, you can find there the various RPM/DEB packages and the binary
tarballs.

If you are an existing user, please carefully follow these instructions
to upgrade.
https://pegasus.isi.edu/docs/5.0.0dev/migration.html#migrating-from-pegasus-4-9-x-to-pegasus-5-0

#### Highlights of the new Release

1) Reworked Python API:

   This new API has been developed from the grounds up that in addition
   to generating the abstract workflow and all the catalogs, allows you to
   plan, submit, monitor, analyze and generate statistics of your workflow.
   To use this new Python API refer to the Moving From DAX3 to Pegasus.api.
   https://pegasus.isi.edu/docs/5.0.0dev/migration.html#moving-from-dax3

2) Adoption of YAML formats:

   With Pegasus 5.0, we are moving to adoption of YAML for representation
   of all major catalogs. In 5.0, the following are now represented in YAML

      * Abstract Workflow
      * Replica Catalog
      * Transformation Catalog
      * Site Catalog
      * Kickstart Provenance Records

3) Python3 Support
      * All Pegasus tools are Python 3 compliant.
      * 5.0 release will require Python 3 on workflow submit node
      * Python PIP packages for workflow composition and monitoring

4) Default data configuration

   In Pegasus 5.0, the default data configuration has been changed to condorio .
   Uptil 4.9.x releases, the default configuration was sharedfs.

5) Zero configuration required to submit to local HTCondor pool

6) Data Management Improvements

     * New output replica catalog that registers outputs including file metadata
       such as size and checksums
     * Ability to do bypass staging of files at a per file, executable and container
       level.
     * Improved support for hierarchal workflows allow you to create data dependencies
       between sub workflow jobs and compute jobs

7) Support for integrity checking of user executables and application containers
   in addition to data

8) Revamped Documentation

   The documentation has been moved to readthedocs style documentation using
   restructured text. The documentation can be found at
   https://pegasus.isi.edu/docs/5.0.0dev/index.html


