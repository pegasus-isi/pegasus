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

To install normally just run:

    $ python setup.py install

To run the unit tests run:

    $ python setup.py test

Creating the Database
---------------------
The `pegasus-service-admin` script is used to manage the database, among
other things.

To add tables to the database run:

    $ pegasus-service-admin create

You can set the SQLAlchemy database URI with the `--dburi` argument.

Running the Service
-------------------
Once the package is installed (normally or in development mode) you can
start the server by running:

    $ pegasus-service-server

Add the `-d` argument to enable debugging, and the `--dburi` argument to change
the SQLAlchemy database URI.

