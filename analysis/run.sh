#!/bin/bash -x

# spark-submit --master local[8] spark_analy.py analyze=primarylang dir=/data/weblingo/GB pattern='GB_langpref_summary*' out='GB_langpref_summary.txt'

# spark-submit --master local[8] spark_analy.py analyze=errs dir=/data/weblingo/GB pattern='GB_langpref_??????' out='GB_langpref_errs.txt'

# spark-submit --master local[8] --py-files spark_lang_extract.py spark_analy.py analyze=spark_lang_extract.py dir=/data/weblingo/GB pattern='GB_langpref_0?????' out='GB_langpref_summary2.json'

# AR  DZ  GB  HK  JP  KE  MX  NL  NZ  PE  TH  US
# langpref and default; for us also tcn

for CC in US;
do
    for RUNTYPE in default langpref tcn;
    do
        spark-submit --master spark://192.168.100.254:7077 --py-files spark_lang_extract.py spark_analy.py analyze=spark_lang_extract.py dir=/data/weblingo/${CC} pattern="${CC}_${RUNTYPE}_??????" out=${CC}_${RUNTYPE}_summary.json
        # spark-submit --master spark://192.168.100.254:7077 --py-files errs.py spark_analy.py analyze=errs.py dir=/data/weblingo/${CC} pattern="${CC}_${RUNTYPE}_??????" out=${CC}_${RUNTYPE}_errs.txt
    done
done
