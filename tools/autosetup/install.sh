#!/bin/sh

BASEDIR=$(dirname $0)

apt-get install mysql-client-core-5.5 build-essential binutils-doc autoconf libboost-all-dev curl bzip2 ccache cmake liblog4cxx10-dev libnuma-dev libev-dev libhwloc-dev libbfd-dev libev-dev libtbb-dev python-requests

sh -ex $BASEDIR/hyriseExternalDeps.sh
