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

To run the unit tests type:

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

By default, the server will start on http://localhost:5000. Add the `-d`
argument to enable debugging, and the `--dburi` argument to change the
SQLAlchemy database URI.

