#!/usr/bin/env bash


diggeonames=/Users/amandeep/Github/dig-geonames
diger=/Users/amandeep/Github/dig-entity-resolution

nltkurl=https://pypi.python.org/packages/source/n/nltk/nltk-3.2.tar.gz#md5=cec19785ffd176fa6d18ef41a29eb227
rm -rf lib
mkdir lib
cd lib
curl -o ./nltk.tar.gz $nltkurl
tar -xf nltk.tar.gz
mv -f nltk-3.2/* ./
rm -rf nltk-3.2
rm nltk.tar.gz


cp ../*py ./
cp $diggeonames/python/dictionaries.py .
cp $diggeonames/python/geoname_extractor.py .
cp $diger/EntityResolution/EntityResolution/EntityResolution/ProbabilisticER.py .
cp $diger/EntityResolution/EntityResolution/EntityResolution/Toolkit.py .

zip -r python-lib.zip *
cd ..
