FROM ubuntu:trusty

MAINTAINER Rajiv Mayani <mayani@isi.edu>

RUN apt-get update

RUN useradd --gid 100 --uid 550 --create-home --password bamboo123 bamboo 

# Set Timezone
RUN cp /usr/share/zoneinfo/America/Los_Angeles /etc/localtime

# Test requirements
RUN apt-get -y install ant ant-optional gcc g++ tar openjdk-7-jdk make libmysql-java mpich2 bc python-setuptools 

# Deb package build requirements
RUN apt-get -y install build-essential debhelper asciidoc fop xsltproc lintian

USER bamboo

RUN echo "pegasus.catalog.replica.db.driver = com.mysql.jdbc.Driver\n"\
"pegasus.catalog.replica.db.url = jdbc:mysql://stewie.isi.edu/jdbcrc_test\n"\
"pegasus.catalog.replica.db.user = jdbcrc_test\n"\
"pegasus.catalog.replica.db.password = candy124" > /home/bamboo/.jdbcrc.conf

RUN mkdir /home/bamboo/.pegasus

ENV HOME /home/bamboo

WORKDIR /home/bamboo/pegasus-source
