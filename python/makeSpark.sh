#!/usr/bin/env bash

entity=~/Github/dig-entity-merger
workflows=~/Github/dig-workflows/pySpark-workflows/
sparkUtilVersion=1.0.14

sparkUtilFilename=digSparkUtil-$sparkUtilVersion
sparkutil=https://pypi.python.org/packages/source/d/digSparkUtil/$sparkUtilFilename.tar.gz#md5=5f8193c94f7e3e8076e5d69c2f7d6911
rm -rf lib
mkdir lib
cd lib
curl -o ./sparkUtil.tar.gz $sparkutil
tar -xf sparkUtil.tar.gz
mv -f $sparkUtilFilename/* ./
rm -rf $sparkUtilFilename
rm sparkUtil.tar.gz



cp $workflows/javaToPythonSpark.py ./
cp $workflows/workflow.py ./


cp $entity/digEntityMerger/*.py ./
zip -r python-lib.zip *
cd ..
