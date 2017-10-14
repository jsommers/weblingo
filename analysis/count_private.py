#!/usr/bin/env python3

from glob import glob
import langtags
from collections import Counter

def get_line(f, lnum):
    with open(f) as infile:
        while lnum > 0:
            line = infile.readline()
            lnum -= 1
    xopen = line.find('{')
    xclose = line.find('}', len(line)-10)
    return eval(line[xopen:(xclose+1)])

def get_all(f):
    return get_line(f, 3)

for f in glob("testdata/??_*_primarylang.txt"):
    alltags = get_all(f)
    numsub = Counter()
    for rawt,ct in alltags.items():
        try:
            t = langtags.Tag(rawt)
            numsub[len(t)] += ct
        except:
            pass
    print(numsub)
    break
