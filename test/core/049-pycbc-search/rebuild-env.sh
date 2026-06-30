#!/bin/bash

set -e

TARGET_DIR=/scitech/shared/projects/LIGO/test-wf/env

if [ -e $TARGET_DIR ]; then
    echo "$TARGET_DIR already exists. Exiting... " 1>&2
    exit 1
fi

cd /tmp
wget -nv -O conda_setup.sh https://repo.anaconda.com/miniconda/Miniconda3-py311_26.3.2-2-Linux-x86_64.sh
bash conda_setup.sh -b -p $TARGET_DIR
rm -f conda_setup.sh

cd $TARGET_DIR
. bin/activate
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r
conda install fftw --yes
conda install anaconda::mkl --yes

pip3 install --upgrade pip
pip3 install --upgrade pip setuptools

mkdir -p tmp
cd tmp

# checkout pycbc and install
git clone https://github.com/gwastro/pycbc.git
cd pycbc
# /scitech/shared/projects/LIGO/test-wf/rebuild-env.sh
pip3 install pylib-fftw3
pip3 install -r requirements.txt
pip3 install -r companion.txt
pip3 install  .

# install the pegasus python dependencies
pip3 install boto3 certifi pyjwt pyyaml s3transfer six urllib3
