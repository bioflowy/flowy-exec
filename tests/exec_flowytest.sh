#!/bin/bash
if [ -d actual ] ; then
  rm -rf actual
fi
mkdir actual
cd actual
flowytest ../workflow.json >std.out 2>std.err
echo "$?" > exitCode
cd ..
diff actual expected
