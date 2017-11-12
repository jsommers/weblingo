#!/bin/bash

curl -O http://downloads.majestic.com/majestic_million.csv
curl -O http://s3.amazonaws.com/alexa-static/top-1m.csv.zip
curl -O https://statvoo.com/dl/top-1million-sites.csv.zip
python3 topsites.py majestic_million.csv top-1m.csv.zip top-1million-sites.csv.zip
#rm -f majestic_million.csv
#rm -f top-1m.csv.zip
#rm -f top-1million-sites.csv.zip
