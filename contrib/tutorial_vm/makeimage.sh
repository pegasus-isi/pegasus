#!/bin/bash

# Size of image in GB
SIZE=4

# Password for root user
ROOTPASS="pegasus"

# Password for tutorial user
TUTORIALPASS="pegasus"

# URL to download condor from
#CONDOR_URL=file:///var/www/html/pub/condor/condor-7.8.1-x86_64_rhap_6.2-stripped.tar.gz
CONDOR_URL=http://juve.isi.edu/pub/condor/condor-7.8.1-x86_64_rhap_6.2-stripped.tar.gz

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

if [ "X$(which yum)" == "X" ]; then
    echo "yum is required"
    exit 1
fi

if [ "X$(which qemu-img)" == "X" ]; then
    echo "qemu-img is required"
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

#echo "Partitioning image..."
#parted $raw mklabel msdos
#parted $raw mkpart primary 1 4095
#parted $raw set 1 boot on

echo "Creating first loop device..."
loop0=$(losetup -f --show $raw)

#echo "Zeroing partition table..."
#dd if=/dev/zero of=$loop0 bs=1M count=1

echo "Partitioning image..."
fdisk $loop0 <<END
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
yum -c yum.conf --installroot=$mnt/ -y install yum dhclient sysklogd openssh-server openssh-clients curl passwd kernel grub e2fsprogs rootfiles vim-minimal
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



echo "Setting root password..."
chroot $mnt /bin/bash -c "echo '$ROOTPASS' | passwd --stdin root"



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



echo "Installing condor..."
curl -s $CONDOR_URL | tar -xz -C $mnt/usr/local
pushd $mnt/usr/local
ln -s condor-* condor
popd
mkdir -p $mnt/var/condor/{spool,execute,log} $mnt/etc/condor
cp $mnt/usr/local/condor/etc/examples/condor_config $mnt/etc/condor
cp $mnt/usr/local/condor/etc/init.d/condor $mnt/etc/init.d/
cp $mnt/usr/local/condor/etc/sysconfig/condor $mnt/etc/sysconfig/
sed -i 's/CONDOR_CONFIG_VAL=/#CONDOR_CONFIG_VAL=/' $mnt/etc/sysconfig/condor
echo 'CONDOR_CONFIG_VAL="/usr/local/condor/bin/condor_config_val"' >> $mnt/etc/sysconfig/condor
sed -i 's/^LOCAL_/#LOCAL_/' $mnt/etc/condor/condor_config
cat >> $mnt/etc/condor/condor_config <<END
LOCAL_DIR = /var/condor
LOCAL_CONFIG_FILE = /etc/condor/condor_config.local
END
cat > $mnt/etc/condor/condor_config.local <<END
CONDOR_HOST = \$(FULL_HOSTNAME)

DAEMON_LIST = MASTER, SCHEDD, NEGOTIATOR, COLLECTOR, STARTD
END
cat > $mnt/etc/profile.d/condor.sh <<END
CONDOR_HOME=/usr/local/condor
export PATH=$PATH:$CONDOR_HOME/bin:$CONDOR_HOME/sbin
END
chroot $mnt useradd condor
chroot $mnt chown -R condor:condor /var/condor /etc/condor
chroot $mnt chkconfig --add condor


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

