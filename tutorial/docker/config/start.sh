#!/bin/bash

# set up the jupyter notebook
if [ "x$TUTORIAL_PASSWORD" = "x" ]; then
    TUTORIAL_PASSWORD="scitech"
fi
if [ "x$TUTORIAL_BASE_URL" = "x" ]; then
    TUTORIAL_BASE_URL="/"
fi
ENCPASSWORD=$(python3 -c "from notebook.auth import passwd;print(passwd(\"$TUTORIAL_PASSWORD\"))")
mkdir -p /home/scitech/.jupyter
cat >/home/scitech/.jupyter/jupyter_notebook_config.json <<EOF
{ "NotebookApp":
   { 
      "base_url": "$TUTORIAL_BASE_URL",
      "password": "$ENCPASSWORD"
   }
}
EOF
chown -R scitech: /home/scitech/.jupyter
cat /home/scitech/.jupyter/jupyter_notebook_config.json

exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf

