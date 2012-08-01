#!/bin/bash

set -e

# Size of image in GB
SIZE=4

# Password for tutorial user
PASSWORD="pegasus"

# URL to download condor from
#CONDOR_URL=file:///var/www/html/pub/condor/condor-7.8.1-x86_64_rhap_6.2-stripped.tar.gz
CONDOR_URL=http://juve.isi.edu/pub/condor/condor-7.8.1-x86_64_rhap_6.2-stripped.tar.gz

# URL to download pegasus from
PEGASUS_URL=http://download.pegasus.isi.edu/wms/download/4.0/pegasus-binary-4.0.1-x86_64_rhel_6.tar.gz



if ! [[ "$(cat /etc/redhat-release 2>/dev/null)" =~ "CentOS release 6" ]]; then
    echo "This script must be run on a CentOS 6 machine"
    exit 1
fi

if [ $(id -u) -ne 0 ]; then
    echo "This script must be run as user root so that it can mount loopback devices"
    exit 1
fi

if [ $(sestatus | grep enabled | wc -l) -ne 0 ]; then
    echo "Disable SELinux before running this script"
    exit 1
fi

if [ "X$(which qemu-img)" == "X" ]; then
    echo "qemu-img is required"
    exit 1
fi

if [ "X$(which losetup)" == "X" ]; then
    echo "losetup is required"
    exit 1
fi

if [ "X$(which mkfs.ext4)" == "X" ]; then
    echo "mkfs.ext4 is required"
    exit 1
fi

if [ $# -ne 1 ]; then
    echo "Usage: $0 disk.img"
    exit 1
fi

raw=$1
if ! [[ "$raw" =~ ".img" ]]; then
    echo "Image name must end with .img: $raw"
    exit 1
fi

mnt=$PWD/${raw/.img/}

if [ -f "$raw" ]; then
    echo "$raw exists"
    exit 1
fi



echo "Creating $SIZE GB image..."
dd if=/dev/zero of=$raw bs=1M count=1 seek=$(((SIZE*1024)-1))



echo "Creating first loop device..."
loop0=$(losetup -f --show $raw)



echo "Partitioning image..."
! fdisk $loop0 <<END
n
p
1


a
1
w
END



echo "Creating second loop device..."
loop1=$(losetup -o 32256 -f --show $raw)



echo "Formatting partition 1..."
# For some reason this tries to create a file system that is too big unless you specify the number of blocks
mkfs.ext4 -L rootdisk $loop1 $(((((SIZE*1024)-1)*1024*1024)/4096))



echo "Mounting partition 1..."
mkdir -p $mnt
mount $loop1 $mnt



echo "Creating basic directory layout..."
mkdir -p $mnt/{proc,etc,dev,var/{cache,log,lock/rpm}}



echo "Creating devices..."
MAKEDEV -d $mnt/dev -x console null zero urandom random



echo "Mounting /proc file system"
mount -t proc none $mnt/proc



echo "Creating /etc/fstab..."
cat > $mnt/etc/fstab << EOF
LABEL=rootdisk     /         ext4    defaults        1 1
tmpfs              /dev/shm  tmpfs   defaults        0 0
devpts             /dev/pts  devpts  gid=5,mode=620  0 0
none               /proc     proc    defaults        0 0
none               /sys      sysfs   defaults        0 0
EOF



echo "Installing minimal base packages..."
yum -c yum.conf --installroot=$mnt/ -y install yum dhclient rsyslog openssh-server openssh-clients curl passwd kernel grub e2fsprogs rootfiles vim-minimal java-1.7.0-openjdk sudo perl
yum -c yum.conf --installroot=$mnt/ -y clean all




echo "Creating /etc files..."
#/etc/hosts
echo '127.0.0.1 localhost.localdomain localhost' > $mnt/etc/hosts
cat > $mnt/etc/sysconfig/network-scripts/ifcfg-eth0 <<EOF
DEVICE=eth0
ONBOOT=yes
BOOTPROTO=dhcp
EOF
touch $mnt/etc/resolv.conf
cat > $mnt/etc/sysconfig/network <<EOF
NETWORKING=yes
HOSTNAME=localhost.localdomain
EOF



echo "Installing grub..."

# Identify kernel and ramdisk
pushd $mnt/boot
KERNEL=$(ls vmlinuz-*)
RAMDISK=$(ls initramfs-*)
popd

# Create grub.conf
cat > $mnt/boot/grub/grub.conf <<EOF
default 0
timeout 0
splashimage=(hd0,0)/boot/grub/splash.xpm.gz
hiddenmenu
title CentOS
    root (hd0,0)
    kernel /boot/$KERNEL ro root=LABEL=rootdisk rd_NO_LUKS rd_NO_LVM rd_NO_MD rd_NO_DM LANG=en_US.UTF-8 KEYBOARDTYPE=pc KEYTABLE=us nomodeset quiet selinux=0
    initrd /boot/$RAMDISK
EOF

# Create menu.lst
pushd $mnt/boot/grub
ln -s ./grub.conf menu.lst
popd

# Install grub stages
cp /boot/grub/stage1 /boot/grub/e2fs_stage1_5 /boot/grub/stage2 $mnt/boot/grub

# Install grub to MBR
grub --device-map=/dev/null <<EOF
device (hd0) $raw
root (hd0,0)
setup (hd0)
EOF



echo "Installing Condor..."
curl -s $CONDOR_URL | tar -xz -C $mnt/usr/local
chroot $mnt /bin/bash <<ENDL
cd /usr/local

ln -s condor-* condor

mkdir -p /var/condor/{spool,execute,log} /etc/condor

# Instal condor init.d script
cp /usr/local/condor/etc/init.d/condor /etc/init.d/
cp /usr/local/condor/etc/sysconfig/condor /etc/sysconfig/
sed -i 's/CONDOR_CONFIG_VAL=/#CONDOR_CONFIG_VAL=/' /etc/sysconfig/condor
echo 'CONDOR_CONFIG_VAL="/usr/local/condor/bin/condor_config_val"' >> /etc/sysconfig/condor

# Update condor_config
cp /usr/local/condor/etc/examples/condor_config /etc/condor
sed -i 's/^LOCAL_/#LOCAL_/' /etc/condor/condor_config
cat >> /etc/condor/condor_config <<END
LOCAL_DIR = /var/condor
LOCAL_CONFIG_FILE = /etc/condor/condor_config.local
END

# Create condor_config.local
cat > /etc/condor/condor_config.local <<END
CONDOR_HOST = \\\$(FULL_HOSTNAME)

DAEMON_LIST = MASTER, SCHEDD, NEGOTIATOR, COLLECTOR, STARTD

START = True
SUSPEND = False
PREEMPT = False

# This is required for Condor 7.8
TRUST_UID_DOMAIN = True
END

# Create condor profile
cat > /etc/profile.d/condor.sh <<END
CONDOR_HOME=/usr/local/condor
export PATH=\\\$PATH:\\\$CONDOR_HOME/bin:\\\$CONDOR_HOME/sbin
END

# Add condor user
useradd condor
chown -R condor:condor /var/condor /etc/condor
chkconfig --add condor
ENDL



echo "Installing Pegasus..."
curl -s $PEGASUS_URL | tar -xz -C $mnt/usr/local
chroot $mnt /bin/bash <<END
cd /usr/local

ln -s pegasus* pegasus

cat > /etc/profile.d/pegasus.sh <<ENDL
PEGASUS_HOME=/usr/local/pegasus
export PATH=\\\$PATH:\\\$PEGASUS_HOME/bin
ENDL
END



echo "Creating tutorial user..."

# Create the user, set the password, and generate an ssh key
chroot $mnt /bin/bash <<END
useradd tutorial
echo $PASSWORD | passwd --stdin tutorial

mkdir -p /home/tutorial/.ssh
chmod 0700 /home/tutorial/.ssh
ssh-keygen -t rsa -b 2048 -N "" -f /home/tutorial/.ssh/id_rsa
cp /home/tutorial/.ssh/id_rsa.pub /home/tutorial/.ssh/authorized_keys
chmod 0600 /home/tutorial/.ssh/authorized_keys

echo 'tutorial	ALL=(ALL) 	ALL' >> /etc/sudoers
END

# Copy tutorial files into tutorial user's home dir
if [ -d ../../doc/docbook/tutorial ]; then
    cp -R ../../doc/docbook/tutorial/* $mnt/home/tutorial/
    rm -rf $mnt/home/tutorial/.svn $mnt/home/tutorial/bin/.svn $mnt/home/tutorial/input/.svn
fi

chroot $mnt /bin/bash <<END
chown -R tutorial:tutorial /home/tutorial
END



echo "Unmounting partition 1..."
sync
umount $mnt/proc
umount $mnt
rmdir $mnt



echo "Deleting loop devices..."
losetup -d $loop1
losetup -d $loop0



echo "Creating vmdk..."
qemu-img convert -f raw -O vmdk $raw ${raw/.img/.vmdk}

