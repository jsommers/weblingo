#!/bin/bash -x

#~/Downloads/spark-2.2.0/bin/spark-submit spark_analy.py analyze=spark_lang_extract dir=`pwd`/testdata pattern='GB_langpref_0000*' out='GB_langpref_summary.json'
# ~/Downloads/spark-2.2.0/bin/spark-submit spark_analy.py analyze=errs dir=`pwd`/testdata pattern='GB_langpref_*' out=errs.txt

spark-submit --master local[8] spark_analy.py analyze=errs dir=/data/weblingo/GB pattern='GB_langpref_??????' out='GB_langpref_errs.txt'

spark-submit --master local[8] spark_analy.py analyze=spark_lang_extract dir=/data/weblingo/GB pattern='GB_langpref_??????' out='GB_langpref_summary.json'

spark-submit --master local[8] spark_analy.py analyze=primarylang dir=/data/weblingo/GB pattern='GB_langpref_summary*' out='GB_langpref_summary.txt'

