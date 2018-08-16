From ubuntu:xenial

# Note: python3 is used for this project

RUN apt-get update && apt-get upgrade -y

RUN apt-get update && apt-get install -y --no-install-recommends \
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
        python3 \
        python3-dev \
        python3-pip \
        rsync \
        unzip \
        vim \
        wget

RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip
RUN pip3 install --upgrade setuptools

RUN pip3 install typing
RUN pip3 install numpy
RUN export CFLAGS=$(gdal-config --cflags) && pip3 install GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}')
RUN pip3 install pandas
RUN pip3 install geopandas
RUN pip3 install rasterio

# add user
RUN useradd --gid 100 --uid 550 --create-home --password mint mint

USER mint
WORKDIR /home/mint
