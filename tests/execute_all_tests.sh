#!/bin/bash


cd $(dirname $0)
for path in `find . -type d -name 'test_*'`; do

  pushd $path;../exec_flowytest.sh;popd
done