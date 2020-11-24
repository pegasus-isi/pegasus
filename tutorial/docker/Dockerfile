FROM centos:8

RUN groupadd --gid 808 scitech-group
RUN useradd --gid 808 --uid 550 --create-home --password '$6$ouJkMasm5X8E4Aye$QTFH2cHk4b8/TmzAcCxbTz7Y84xyNFs.gqm/HWEykdngmOgELums1qOi3e6r8Z.j7GEA9bObS/2pTN1WArGNf0' scitech

RUN dnf -y update && \
    dnf -y install 'dnf-command(config-manager)' && \
    dnf -y config-manager --set-enabled PowerTools && \
    dnf -y config-manager --add-repo=https://download.docker.com/linux/centos/docker-ce.repo && \
    dnf -y install epel-release

RUN dnf -y install --allowerasing --nobest \
     curl \
     docker-ce \
     glibc-langpack-en \
     graphviz \
     java-11-openjdk-devel \
     langpacks-en \
     perl-Getopt-Long \
     python3-devel \
     python3-GitPython \
     python3-pika \
     python3-pip \
     python3-pyOpenSSL \
     python3-PyYAML \
     python3-setuptools \
     sudo \
     tar \
     vim \ 
     wget

ADD ./config/wrapdocker /usr/local/bin/wrapdocker
RUN chmod +x /usr/local/bin/wrapdocker
RUN usermod -aG docker scitech

# Python packages
RUN python3 -m pip install jupyter

# Locale and timezone
ENV LANG en_US.UTF-8 
RUN cp /usr/share/zoneinfo/America/Los_Angeles /etc/localtime

# HTCondor
RUN dnf -y config-manager --add-repo=https://research.cs.wisc.edu/htcondor/yum/repo.d/htcondor-stable-rhel8.repo && \
    rpm --import https://research.cs.wisc.edu/htcondor/yum/RPM-GPG-KEY-HTCondor && \
    dnf -y install condor minicondor && \
    sed -i 's/condor@/scitech@/g' /etc/condor/config.d/00-minicondor

RUN usermod -a -G condor scitech
RUN chmod -R g+w /var/{lib,log,lock,run}/condor

# Pegasus -- rebuild everything below here
#            http://dev.im-bot.com/docker-select-caching/
ARG CACHEBUST=1
ARG PEGASUS_VERSION=1
RUN dnf -y install https://download.pegasus.isi.edu/pegasus/$PEGASUS_VERSION/pegasus-$PEGASUS_VERSION-1.el8.x86_64.rpm

ADD ./notebooks /home/scitech/notebooks
RUN chown -R scitech /home/scitech/

RUN echo -e "condor_master > /dev/null 2>&1" >> /home/scitech/.bashrc

# User setup
RUN echo -e "scitech ALL=(ALL)       NOPASSWD:ALL\n" >> /etc/sudoers
USER scitech

WORKDIR /home/scitech

# Set up config for ensemble manager
RUN mkdir /home/scitech/.pegasus \
    && echo -e "#!/usr/bin/env python3\nUSERNAME='scitech'\nPASSWORD='scitech123'\n" >> /home/scitech/.pegasus/service.py \
    && chmod u+x /home/scitech/.pegasus/service.py

# Set up pegasus database
RUN pegasus-db-admin create

# Set Kernel for Jupyter (exposes PATH and PYTHONPATH for use when terminal from jupyter is used)
ADD ./config/kernel.json /usr/local/share/jupyter/kernels/python3/kernel.json
RUN echo -e "export PATH=/home/scitech/.pyenv/bin:\$PATH:/usr/lib64/mpich/bin" >> /home/scitech/.bashrc

# Set notebook password to 'scitech'. This pw will be used instead of token authentication
RUN mkdir /home/scitech/.jupyter \ 
    && echo "{ \"NotebookApp\": { \"password\": \"sha1:30a323540baa:6eec8eaf3b4e0f44f2f2aa7b504f80d5bf0ad745\" } }" >> /home/scitech/.jupyter/jupyter_notebook_config.json

# wrapdocker required for nested docker containers
ENTRYPOINT ["sudo", "/usr/local/bin/wrapdocker"]
CMD ["su", "-", "scitech", "-c", "jupyter notebook --notebook-dir=/home/scitech/notebooks --port=8888 --no-browser --ip=0.0.0.0 --allow-root"] 
