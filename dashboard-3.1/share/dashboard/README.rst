==================================
README for Stampede mini-dashboard
==================================
:Author: Dan Gunter <dkgunter@lbl.gov>
:Date: $Date$
:Revision: $Revision$

Configuring
===========
The database is hardcoded as the constant DB_URL
in the file dash.py. Change this to an appropriate
SQLAlchemy URL.

Running
=======
First, configure the database URL (see Configuring).

Change to directory with dash.py
Then simply run it:
   python dash.py

In your web browser, navigate to:
   http://localhost:8080/static/dash.html

You should see the dashboard.
