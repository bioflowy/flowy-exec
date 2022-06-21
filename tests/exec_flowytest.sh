#!/bin/bash
if [ -d actual ] ; then
  rm -rf actual
fi
mkdir actual
cd actual
flowytest ../workflow.json
echo "$?" > exitCode
cd ..
diff actual expected
