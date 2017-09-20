#!/bin/bash -x

~/Downloads/spark-2.2.0/bin/spark-submit spark_analy.py analyze=spark_lang_extract dir=`pwd`/../testdata pattern='GB_langpref_0000*' 

# ~/Downloads/spark-2.2.0/bin/spark-submit spark_analy.py analyze=errs dir=`pwd`/../testdata pattern='GB_langpref_*' out=errs.txt
