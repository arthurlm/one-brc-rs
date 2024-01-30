#!/bin/bash
set -e

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit
fi

typeset RAMDISK_PATH="/mnt/ramdisk"

if mount | grep "$RAMDISK_PATH" > /dev/null 2>&1 ; then
    echo "RAM disk already mounted"
    exit
fi

mkdir -p "$RAMDISK_PATH"
mount -t tmpfs -o size=14G tmpfs "$RAMDISK_PATH"
