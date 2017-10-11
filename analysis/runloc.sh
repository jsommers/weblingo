#!/bin/bash -x

# ~/Downloads/spark-2.2.0/bin/spark-submit --py-files primarylang.py spark_analy.py analyze=primarylang dir=`pwd`/testdata pattern='GB_default_summary*' out=GB_defaultX.txt

# ~/Downloads/spark-2.2.0/bin/spark-submit --py-files primarylang.py spark_analy.py analyze=primarylang dir=`pwd`/testdata pattern='GB_langpref_summary*' out=GB_langprefX.txt

~/Downloads/spark-2.2.0/bin/spark-submit spark_analy.py analyze=alllang dir=`pwd`/testdata pattern='MX_langpref_summary' out=MX_alllang.txt

for CC in 
~/Downloads/spark-2.2.0/bin/spark-submit spark_analy.py analyze=primarylang dir=`pwd`/testdata pattern='MX_langpref_summary' out=MX_primarylang.txt

# ~/Downloads/spark-2.2.0/bin/spark-submit spark_analy.py analyze=checkcontneg dir=`pwd`/testdata pattern='MX_langpref_vary'
