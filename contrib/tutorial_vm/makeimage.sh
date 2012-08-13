#!/bin/bash

set -e

# Size of image in GB
SIZE=2

# Password for tutorial user
PASSWORD="pegasus"


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
    echo "Usage: $0 name"
    exit 1
fi

name=$1

img1=${name}.fg
img2=${name}.ec2
vmdk=${name}.vmdk

mnt1=$PWD/image1
mnt2=$PWD/image2

if [ -f "$img1" ]; then
    echo "$img1 exists"
    exit 1
fi


echo "Creating $SIZE GB image..."
dd if=/dev/zero of=$img1 bs=1M count=1 seek=$(((SIZE*1024)-1))


echo "Creating loop device..."
loop0=$(losetup -f --show $img1)


echo "Formatting image..."
# For some reason this tries to create a file system that is too big unless you specify the number of blocks
mkfs.ext4 -L rootdisk -b 4096 $loop0 $(((SIZE*262144)-256))


echo "Mounting image..."
mkdir -p $mnt1
mount $loop0 $mnt1


echo "Creating basic directory layout..."
mkdir -p $mnt1/{proc,etc,dev,var/{cache,log,lock/rpm}}


echo "Creating devices..."
MAKEDEV -d $mnt1/dev -x console null zero urandom random


echo "Mounting /proc file system..."
mount -t proc none $mnt1/proc


echo "Creating /etc/fstab..."
cat > $mnt1/etc/fstab << EOF
LABEL=rootdisk     /         ext4    defaults        1 1
tmpfs              /dev/shm  tmpfs   defaults        0 0
devpts             /dev/pts  devpts  gid=5,mode=620  0 0
none               /proc     proc    defaults        0 0
none               /sys      sysfs   defaults        0 0
EOF


echo "Installing minimal base packages..."
yum -c yum.conf --installroot=$mnt1/ -y install yum dhclient rsyslog openssh-server openssh-clients curl passwd kernel grub e2fsprogs rootfiles vim-minimal sudo perl
yum --installroot=$mnt1/ -y clean all


echo "Creating /etc files..."

echo '127.0.0.1 localhost.localdomain localhost' > $mnt1/etc/hosts

cat > $mnt1/etc/sysconfig/network-scripts/ifcfg-eth0 <<EOF
DEVICE=eth0
ONBOOT=yes
BOOTPROTO=dhcp
EOF

touch $mnt1/etc/resolv.conf

# NOZEROCONF is required for the metadata server to work on Eucalyptus
cat > $mnt1/etc/sysconfig/network <<EOF
NETWORKING=yes
NOZEROCONF=yes
EOF

# This has to be removed or the interface will not come up on Eucalyptus
rm -f $mnt1/etc/udev/rules.d/70-persistent-net.rules


echo "Installing Condor..."
cat > $mnt1/etc/yum.repos.d/condor.repo <<END
[condor]
name=Condor
baseurl=http://www.cs.wisc.edu/condor/yum/stable/rhel6
enabled=1
gpgcheck=0
END

yum --installroot=$mnt1 install -y condor

echo "TRUST_UID_DOMAIN = True" >> $mnt1/etc/condor/condor_config.local


echo "Installing Pegasus..."
cat > $mnt1/etc/yum.repos.d/pegasus.repo <<END
[pegasus]
name=Pegasus
baseurl=http://download.pegasus.isi.edu/wms/download/rhel/6/\$basearch/
gpgcheck=0
enabled=1
END

yum --installroot=$mnt1 install -y pegasus


echo "Creating tutorial user..."

# Create the user, set the password, and generate an ssh key
chroot $mnt1 /bin/bash <<END
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
if [ -d ../../doc/tutorial ]; then
    cp -R ../../doc/tutorial/* $mnt1/home/tutorial/
    rm -rf $mnt1/home/tutorial/.svn $mnt1/home/tutorial/bin/.svn $mnt1/home/tutorial/input/.svn
fi

chroot $mnt1 /bin/bash <<END
chown -R tutorial:tutorial /home/tutorial
END


echo "Cleaning up image..."
yum --installroot=$mnt1/ -y clean all


echo "Creating $SIZE GB image..."
dd if=/dev/zero of=$img2 bs=1M count=1 seek=$(((SIZE*1024)-1))


echo "Creating second loop device..."
loop1=$(losetup -f --show $img2)


echo "Partitioning second image..."
! fdisk $loop1 <<END
n
p
1


a
1
w
END


echo "Creating third loop device..."
loop2=$(losetup -o 32256 -f --show $img2)


echo "Formatting second image..."
# For some reason this tries to create a file system that is too big unless you specify the number of blocks
mkfs.ext4 -L rootdisk -b 4096 $loop2 $(((SIZE*262144)-256))


echo "Mounting second image..."
mkdir -p $mnt2
mount $loop2 $mnt2


echo "Copying image..."
rsync -ax -W $mnt1/ $mnt2


echo "Mounting second proc..."
mount -t proc none $mnt2/proc


echo "Installing grub..."

# Identify kernel and ramdisk
pushd $mnt2/boot
KERNEL=$(ls vmlinuz-*)
RAMDISK=$(ls initramfs-*)
popd

# Create grub.conf
cat > $mnt2/boot/grub/grub.conf <<EOF
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
pushd $mnt2/boot/grub
ln -s ./grub.conf menu.lst
popd

# Install grub stages
cp $mnt2/usr/share/grub/x86_64-redhat/* $mnt2/boot/grub

# Install grub to MBR
grub --device-map=/dev/null <<EOF
device (hd0) $img2
root (hd0,0)
setup (hd0)
EOF


echo "Unmounting images..."
sync
umount $mnt1/proc
umount $mnt2/proc
umount $mnt1
umount $mnt2
rmdir $mnt1
rmdir $mnt2


echo "Deleting loop devices..."
losetup -d $loop2
losetup -d $loop1
losetup -d $loop0


echo "Creating vmdk..."
qemu-img convert -f raw -O vmdk $img2 $vmdk


echo "Zipping vmdk..."
zip ${name}.zip $vmdk

