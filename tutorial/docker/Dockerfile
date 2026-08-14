FROM rockylinux/rockylinux:9

RUN <<EOT
set -e

dnf -y update
dnf -y install 'dnf-command(config-manager)'
dnf -y config-manager --set-enabled devel
dnf -y config-manager --add-repo=https://download.docker.com/linux/centos/docker-ce.repo
dnf -y install epel-release
dnf -y install https://htcss-downloads.chtc.wisc.edu/repo/25.x/htcondor-release-current.el9.noarch.rpm

# HTCondor
dnf -y install --enablerepo crb minicondor
sed -i 's/condor@/scitech@/g' /etc/condor/config.d/00-minicondor

# Pegasus runtime dependencies
dnf -y install --allowerasing --nobest \
    tar \
    curl wget \
    vim nano emacs \
    docker-ce \
    sudo \
    glibc-langpack-en \
    langpacks-en \
    python3.12-pip \
    supervisor

echo -e "scitech ALL=(ALL)       NOPASSWD:ALL\n" >> /etc/sudoers
groupadd --gid 808 scitech-group
useradd --gid 808 --groups docker,condor --uid 550 --create-home --password '$6$ouJkMasm5X8E4Aye$QTFH2cHk4b8/TmzAcCxbTz7Y84xyNFs.gqm/HWEykdngmOgELums1qOi3e6r8Z.j7GEA9bObS/2pTN1WArGNf0' scitech
cp /usr/share/zoneinfo/America/Los_Angeles /etc/localtime
pip3.12 install jupyter
EOT

# Locale and timezone
ENV LANG=en_US.UTF-8

ADD ./config/start.sh /start.sh
ADD ./config/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
ADD ./config/wrapdocker /usr/local/bin/wrapdocker
ADD ./config/htcondor-wrapper /usr/local/bin/htcondor-wrapper
ADD ./config/10-dynamic-slots /etc/condor/config.d/10-dynamic-slots
ADD ./notebooks /home/scitech/notebooks

# Pegasus -- rebuild everything below here
#            http://dev.im-bot.com/docker-select-caching/
ARG CACHEBUST=1
ARG PEGASUS_VERSION=1
COPY pegasus-*-*.rpm /tmp/
ADD ./config/kernel.json /usr/local/share/jupyter/kernels/python3/kernel.json
RUN <<EOT
set -e

chown -R scitech /home/scitech/
chmod +x /usr/local/bin/wrapdocker /usr/local/bin/htcondor-wrapper

# dnf -y install /tmp/pegasus-*-*.`arch`.rpm
dnf -y install https://download.pegasus.isi.edu/pegasus/6.0.0-dev.0/pegasus-6.0.0-dev.0-1.el9.aarch64.rpm

su -s /bin/bash scitech <<EOS
# Set up config for ensemble manager and service
mkdir -p /home/scitech/.pegasus

cat > /home/scitech/.pegasus/service.py <<'PY'
#!/usr/bin/env python3.12
USERNAME='scitech'
PASSWORD='scitech123'
AUTHENTICATION='NoAuthentication'
SERVER_HOST='0.0.0.0'
PY
chmod u+x /home/scitech/.pegasus/service.py

echo -e "export PATH=/home/scitech/.pyenv/bin:\$PATH:/usr/lib64/mpich/bin" >> /home/scitech/.bashrc

pegasus-db-admin create
EOS

# Cleanup
rm -rf /tmp/pegasus-*.rpm
EOT

WORKDIR /home/scitech
USER root
CMD ["/start.sh"]
