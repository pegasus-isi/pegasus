FROM centos:centos6

MAINTAINER Rajiv Mayani <mayani@isi.edu>

RUN yum -y update

RUN useradd --gid 100 --uid 550 --create-home --password bamboo123 bamboo

# Test requirements
RUN yum -y install ant ant-nodeps ant-junit gcc gcc-c++ tar which mpich2-devel bc python-setuptools
RUN yum -y --enablerepo=centosplus install openssl-devel

RUN echo -e "MPD_SECRETWORD=mr45-j9z\n" > ~/.mpd.conf && chmod 600 ~/.mpd.conf

# RPM Build requirements
RUN yum -y install rpm-build ant-apache-regexp java7-devel python-devel asciidoc libxslt fop

# Set Timezone
RUN cp /usr/share/zoneinfo/America/Los_Angeles /etc/localtime

USER bamboo

RUN echo -e "pegasus.catalog.replica.db.driver = com.mysql.jdbc.Driver\n"\
"pegasus.catalog.replica.db.url = jdbc:mysql://stewie.isi.edu/jdbcrc_test\n"\
"pegasus.catalog.replica.db.user = jdbcrc_test\n"\
"pegasus.catalog.replica.db.password = candy124" > /home/bamboo/.jdbcrc.conf

#RUN chown bamboo:users /home/bamboo/.jdbcrc.conf

RUN echo -e "MPD_SECRETWORD=mr45-j9z\n" > /home/bamboo/.mpd.conf && chmod 600 /home/bamboo/.mpd.conf

RUN mkdir /home/bamboo/.pegasus

ENV HOME /home/bamboo

WORKDIR /home/bamboo/pegasus-source
