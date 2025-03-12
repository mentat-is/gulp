#!/usr/bin/env bash
_pwd=$(pwd)
cd src/gulp/libgulp
if [ "$1" == "clean" ]; then
	make clean
	cd $_pwd
	exit 0
fi

make
if [ $? -ne 0 ]; then
	cd $_pwd
	exit 1
fi
make install
cd $_pwd
