Pegasus Service
===============
This project implements several services that build interfaces on top of 
Pegasus for monitoring, ensemble management, and other future functinality.

For more information see the manual in the `doc` directory.

Installation
------------
Installation is accomplished via the `setup.py` script.

If you want to install in development mode use:

    $ python setup.py develop

To install normally just type:

    $ python setup.py install

To run the unit tests type (set the environment variables if you want to
include the performance and integration tests, which take a long time and
require Condor and Pegasus):

    $ export ENABLE_PERFORMANCE_TESTS=1
    $ export ENABLE_INTEGRATION_TESTS=1
    $ python setup.py test

Setup
-----
The `pegasus-service-admin` script is used to manage the service.

To add tables to the database run:

    $ pegasus-service-admin create

You can set the SQLAlchemy database URI with the `--dburi` argument.

To add a new user run:

    $ pegasus-service-admin useradd USERNAME EMAIL

Where USERNAME is the desired username and EMAIL is the user's email address.
It will prompt you for the new user's password.

Running the Service
-------------------
Once the service is installed (normally or in development mode) you can
start the server by running:

    $ pegasus-service-server

By default, the server will start on http://localhost:5000. You can set
the host and port in the configuration file (see next section).

Configuration
-------------
The configuration file is $HOME/.pegasus/service.py. Look in
pegasus/service/defaults.py for the variables and default values.

You can change the host and port of the service by setting the SERVER\_HOST
and SERVER\_PORT variables.

All clients that connect to the web API will require the USERNAME and
PASSWORD settings in the configuration file.


Ensemble Manager
----------------
The ensemble manager is a service that manages collections of workflows
called ensembles. The ensemble manager is useful when you have a set of
workflows you need to run over a long period of time. It can throttle
the number of concurrent planning and running workflows, and plan and
run workflows in priority order. A typical use-case is a user with 100
workflows to run, who needs no more than one to be planned at a time,
and needs no more than two to be running concurrently.

The ensemble manager also allows workflows to be submitted and monitored
programmatically through its RESTful interface, which makes it an ideal
platform for integrating workflows into larger applications such as
science gateways and portals.

The ensemble manager requires Pegasus and Condor. It will start
automatically when pegasus-service-server is started if Pegasus and Condor
are found in the server's PATH, or if the PEGASUS\_HOME and CONDOR\_HOME
settings are specified correctly in the configuration file.

Once the ensemble manager is running, you can create an ensemble with:

    $ pegasus-service-ensemble create -e "myruns"

where "myruns" is the name of the ensemble.

Before submitting workflows to the ensemble you must upload replica,
site, and transformation catalogs:

    $ pegasus-service-catalog upload -t replica -n "replicas" -F regex -f rc.txt
    $ pegasus-service-catalog upload -t site -n "sites" -F xml -f sites.xml
    $ pegasus-service-catalog upload -t transformation -n "xforms" -F text -f tc.txt

Then you can submit a workflow to the ensemble using the previously uploaded
catalogs:

    $ pegasus-service-ensemble submit -e "myruns" -w "run1" -d run1.dax -R replicas \
    -S sites -T xforms -s local -o local -c pegasus.properties

To check the status of your ensembles run:

    $ pegasus-service-ensemble ensembles

To check the status of your workflows run:

    $ pegasus-service-ensemble workflows -e "myruns"

For more information about the ensemble manager see the user guide.

