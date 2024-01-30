#!/bin/bash
set -e

if [ "$EUID" -ne 0 ]; then
    echo "Please run as root"
    exit
fi

sync
echo 1 > /proc/sys/vm/drop_caches
