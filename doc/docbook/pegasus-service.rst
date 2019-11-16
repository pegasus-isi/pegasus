.. _pegasus-service:

===============
Pegasus Service
===============

Service Administration
======================

Service Configuration
---------------------

Create a file called service.py in $HOME/.pegasus/ OR modify the
lib/pegasus/python/Pegasus/service/defaults.py file. The servuce can be
configured using the properties described below.

.. table:: Pegasus Service Configuration Options

   ================= ================= ======================================================================================================================================================================================================================================================================================================================================================================================================================================
   Property          Default Value     Description
   ================= ================= ======================================================================================================================================================================================================================================================================================================================================================================================================================================
   SERVER_HOST       127.0.0.1         SERVER_HOST specifies the hostname/network interface on which the service listens for requests.
   SERVER_PORT       5000              SERVER_PORT specifies the port number on which the service listens for requests.
   CERTIFICATE       None              SSL certificate file used to encrypt sessions. If no certificate, key files are provided the service will generate and use self-signed certificates.
   PRIVATE_KEY       None              SSL key file used to encrypt connections. If no certificate, key files are provided the service will generate and use self-signed certificates.
   AUTHENTICATION    PAMAuthentication By default the service uses PAM authentication i.e. When prompted for a username and password users can use the credentials that they use to login to the machine. Users can specify NoAuthentication to disable username/password prompt.
   ADMIN_USERS       None              ADMIN_USERS can be used to specify which users have the ability to access other users workflow info. If ADMIN_USERS is None, False, or '' then users can only access their own workflow information. If ADMIN_USERS is '*' then all users are admin users and can access everyones workflow information. If ADMIN_USERS = {'u1', .., 'un'} OR ['u1', .., 'un'] then only users u1, .., un can access other users workflow information.
   PROCESS_SWITCHING True              File created by running Pegasus workflows have permissions as per user configuration. So one user migt not be able to view workflow information of other users. Setting PROCESS_SWITCHING to True makes the service change the process UID to the UID of the user whose information is being requested. pegasus-service must be started as root for PROCESS_SWITCHING to work. PROCESS_SWITCHING can be set to False.
   USERNAME          ''                The username which pegasus-em client uses to connect to the pegasus-em server.
   PASSWORD          ''                The password which pegasus-em client uses to connect to the pegasus-em server.
   ================= ================= ======================================================================================================================================================================================================================================================================================================================================================================================================================================

All clients that connect to the web API will require the USERNAME and
PASSWORD settings in the configuration file.

Running the Service
-------------------

Pegasus Service can be started using the pegasus-service command as
follows

::

   $ pegasus-service

By default, the server will start on
`https://localhost:5000 <http://localhost:5000>`__. You can set the host
and port in the configuration file OR pass it as a command line switch
to pegasus-service as follows.

::

   $ pegasus-service --host <SERVER_HOSTNAME> --port <SERVER_PORT>

.. _dashboard:

Dashboard
=========

The dashboard is automatically started when pegasus-service command is
executed.

.. _pegasus-service-apache:

Running Pegasus Service under Apache HTTPD
==========================================

**Prerequisites** Apache HTTPD, mod_ssl, and mod_wsgi to be installed.

To run pegasus-service under Apache HTTPD

1. Copy file share/pegasus/service/pegasus-service.wsgi to some other
   directory. We will refer to this directory as <WSGI_FILE_DIR>.

   Configure pegasus service by setting the AUTHENTICATION,
   PROCESS_SWITCHING, and/or ADMIN_USERS properties in the
   <WSGI_FILE_DIR>/pegasus-service.wsgi file as desired.

2. Copy file share/pegasus/service/pegasus-service-httpd.conf to your
   Apache conf directory.

   1. Replace PEGASUS_PYTHON_EXTERNALS with absolute path to pegasus
      python externals directory. Execute pegasus-config
      --python-externals to get this path

   2. Replace HOSTNAME with the hostname on which the server should
      listen for requests.

   3. Replace DOCUMENT_ROOT with <WSGI_FILE_DIR>

   4. Replace USER_NAME with the username as which the WSGIDaemonProcess
      should start

   5. Replace GROUP_NAME with the groupname as which the
      WSGIDaemonProcess should start

   6. Replace PATH_TO_PEGASUS_SERVICE_WSGI_FILE with
      <WSGI_FILE_DIR>/pegasus-service.wsgi

   7. Replace PATH_TO_SSL_CERT with absolute location of your SSL
      certificate file

   8. Replace PATH_TO_SSL_KEY with absolute location of your SSL private
      key file

For additional mod_wsgi configuration refer to
https://code.google.com/p/modwsgi/wiki/ConfigurationDirectives

.. _ensemble-manager:

Ensemble Manager
================

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

To start the ensemble manager server, run:

::

   $ pegasus-em server

Once the ensemble manager is running, you can create an ensemble with:

::

   $ pegasus-em create myruns

where "myruns" is the name of the ensemble.

Then you can submit a workflow to the ensemble by running:

::

   $ pegasus-em submit myruns.run1 ./plan.sh run1.dax

Where the name of the ensemble is "myruns", the name of the workflow is
"run1", and "./plan.sh run1.dax" is the command for planning the
workflow from the current working directory. The planning command should
either be a direct invocation of pegasus-plan, or a shell script that
calls pegasus-plan. If a shell script is used, then it should not
redirect the output of pegasus-plan, because the ensemble manager reads
the output to determine whether pegasus-plan succeeded and what is the
submit directory of the workflow.

To check the status of your ensembles run:

::

   $ pegasus-em ensembles

To check the status of your workflows run:

::

   $ pegasus-em workflows myruns

To check the status of a specific workflow, run:

::

   $ pegasus-em status myruns.run1

To help with debugging, the ensemble manager has an analyze command that
emits diagnostic information about a workflow, including the output of
pegasus-analyzer, if possible. To analyze a workflow, run:

::

   $ pegasus-em analyze myruns.run1

Ensembles can be paused to prevent workflows from being planned and
executed. Workflows in a paused ensemble will continue to run, but no
new workflows will be planned or executed. To pause an ensemble, run:

::

   $ pegasus-em pause myruns

Paused ensembles can be reactivated by running:

::

   $ pegasus-em activate myruns

A workflow might fail during planning. In that case, run the analyze
command to examine the planner output, make the necessary corrections to
the workflow configuration, and replan the workflow by running:

::

   $ pegasus-em replan myruns.run1

A workflow might also fail during execution. In that case, run the
analyze command to identify the issue, correct the problem, and rerun
the workflow by running:

::

   $ pegasus-em rerun myruns.run1

Workflows in an ensemble can have different priorities. These priorities
are used to determine the order in which workflows in the ensemble will
be planned and executed. Priorities are specified using the '-p' option
of the submit command. They can also be modified after a workflow has
been submitted by running:

::

   $ pegasus-em priority myruns.run1 -p 10

where 10 is the desired priority. Higher values have higher priority,
the default is 0, and negative values are allowed.

Each ensemble has a pair of throttles that limit the number of workflows
that are concurrently planning and executing. These throttles are called
max_planning and max_running. Max planning limits the number of
workflows in the ensemble that can be planned concurrently. Max running
limits the number of workflows in the ensemble that can be running
concurrently. These throttles are useful to limit the impact of planning
on the memory usage of the submit host, and the load on the submit host
and remote site caused by concurrently running workflows. The throttles
can be specified with the '-R' and '-P' options of the create command.
They can also be updated using the config command:

::

   $ pegasus-em config myruns.run1 -P 1 -R 5
