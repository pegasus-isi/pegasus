FROM amazonlinux:latest

# required for fetch_and_run to execute
RUN yum -y install which unzip aws-cli

# required for pegasus worker package to work
RUN yum -y install gzip python3 wget tar
ADD fetch_and_run.sh /usr/local/bin/fetch_and_run.sh
WORKDIR /tmp
USER nobody

ENTRYPOINT ["/usr/local/bin/fetch_and_run.sh"]
