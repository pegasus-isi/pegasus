#!/bin/bash

yum -y install python-pip python-devel gcc

pip install --upgrade pip

pip install jupyter

ln -s /usr/share/firefox/firefox /usr/bin/firefox

mkdir -p /home/tutorial/jupyter
cp /usr/share/pegasus/jupyter/Pegasus-DAX3-Tutorial.ipynb /home/tutorial/jupyter/Pegasus-DAX3-Tutorial.ipynb
cp /usr/share/pegasus/init/split/pegasus.html /home/tutorial/jupyter/pegasus.html

chown -R tutorial:tutorial /home/tutorial/jupyter
