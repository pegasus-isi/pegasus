bootstrap:docker
From:ubuntu:xenial

# Note: python3 is used for this project

%post

apt-get update && apt-get upgrade -y

apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        gdal-bin \
        gsfonts \
        imagemagick \
        libfreetype6-dev \
        libgdal-dev \
        libpng12-dev \
        libzmq3-dev \
        lsb-release \
        module-init-tools \
        openjdk-8-jdk \
        pkg-config \
        python \
        python3 \
        python3-dev \
        python3-pip \
        rsync \
        unzip \
        vim \
        wget

apt-get clean 
rm -rf /var/lib/apt/lists/*

pip3 install --upgrade pip==9.0.3
pip3 install --upgrade setuptools

pip3 install typing
pip3 install numpy
export CFLAGS=$(gdal-config --cflags) && pip3 install GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}')
pip3 install pandas
pip3 install geopandas
pip3 install rasterio==0.36.0


