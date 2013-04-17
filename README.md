Pegasus Service
===============
This project implements several services that build interfaces on top of 
Pegasus for monitoring, ensemble management, and other future functinality.

For more information see the manual in the `doc` directory.

Configuration
-------------
Copy the `service_config.py` file to `~/.pegasus/service_config.py` or to
`/etc/pegasus/service_config.py` and change the settings.

Installation
------------
Installation is accomplished via the `setup.py` script.

If you want to install in development mode use:

    $ python setup.py develop

To install normally just run:

    $ python setup.py install

Creating the Database
---------------------
The `pegasus-service-admin` script is used to manage the database, among
other things.

To create the database run:

    $ pegasus-service-admin create

Running the Service
-------------------
Once the package is installed (normally or in development mode) you can
start the server by running:

    $ pegasus-service-server

You can also create a .wsgi script for Apache mod\_wsgi that looks like this:

    from pegasus.service import app
    application = app

