FROM amazonlinux:latest

# fix urllib3 ssh dependency
# https://github.com/urllib3/urllib3/issues/3016
RUN yum -y install openssl

# required for pegasus worker package to work
RUN yum -y install gzip python3 python3-pip  wget tar

# required for fetch_and_run to execute
RUN yum -y install which unzip aws-cli

# pin the urllib3 version
# https://github.com/psf/requests/issues/6443
RUN python3 -m pip install "urllib3<2"

ADD fetch_and_run.sh /usr/local/bin/fetch_and_run.sh
WORKDIR /tmp
USER nobody

ENTRYPOINT ["/usr/local/bin/fetch_and_run.sh"]
