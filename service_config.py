# This is the config file for the pegasus service. Save a copy of it in
# one of the following locations:
#
#   1. $PWD/service_config.py
#   2. ~/.pegasus/service_config.py
#   3. /etc/pegasus/service_config.py
#
# The first file found is the one that will be used.
#
import os

# The secret key used by Flask to encrypt session keys
SECRET_KEY = os.urandom(24)

# The URI of the database
#SQLALCHEMY_DATABASE_URI = "mysql://pegasus:secret@127.0.0.1:3306/pegasus_service"
SQLALCHEMY_DATABASE_URI = 'sqlite:///%s/.pegasus/workflow.db' % os.getenv('HOME')


