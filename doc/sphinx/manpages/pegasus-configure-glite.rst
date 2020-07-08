=======================
pegasus-configure-glite
=======================

install Pegasus-specific glite configuration
::

      pegasus-configure-glite [GLITE_LOCATION]



Description
===========

**pegasus-configure-glite** installs the Pegasus-specific scripts and
configuration used by Pegasus to submit jobs via Glite. It installs:

1. \*_local_submit_attributes.sh scripts that map Pegasus profiles to
   batch system-specifc job requirements.

2. Scripts for Moab and modifications to batch_gahp.config to enable
   Moab job submission.



Options
=======

**GLITE_LOCATION**
   The directory where glite is installed. If this is not provided, then
   **condor_config_val** will be called to get the value of
   **GLITE_LOCATION** from the Condor configuration files.



Authors
=======

Pegasus Team http://pegasus.isi.edu
