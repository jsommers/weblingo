#!/bin/bash -x

~/Downloads/spark-2.2.0/bin/spark-submit --py-files primarylang.py spark_analy.py analyze=primarylang dir=`pwd`/testdata pattern='GB_default_summary*'

