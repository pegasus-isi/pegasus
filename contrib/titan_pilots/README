In order to get this to work you need to download Condor and install it into
this directory. The version you want for Titan is RHEL5 (Titan is not RHEL5,
but it will work).

0. Download this directory to titan.
1. Download condor into this directory, untar it, and name the sub-directory
   "condor".
2. Modify pilot.pbs to use the host name where your collector resides.
3. Make sure your Collector host firewall accepts connections on 9618.
4. Make sure your Collector allows WRITE from *.ccs.ornl.gov
5. qsub pilot.pbs
6. The pilot job should appear in your condor_status after the pilot job starts
   running.
7. Copy hostname.sub to your condor submit host and submit it. It should run
   on the pilot (on the service node).
8. Compile mpi_hello.c on Titan: $ CC -o mpi_hello mpi_hello.c
9. Copy mpi_hello.sub to your condor submit host and modify the path to
   mpi_hello.sh. It should be the path on titan.
10. Update mpi_hello.sh on titan to have the correct path to mpi_hello that
    you compiled earlier.
11. Submit mpi_hello.sub from your Condor submit host. It should run on
    the titan compute nodes.

