#!/usr/bin/env python3

from glob import glob

def get_line(f, lnum):
    with open(f) as infile:
        while lnum > 0:
            line = infile.readline()
            lnum -= 1
    xopen = line.find('{')
    xclose = line.find('}', xopen+1)
    return eval(line[xopen:(xclose+1)])

def get_region(f):
    return get_line(f, 7)

for f in glob("testdata/??_*_primarylang.txt"):
    reg = get_region(f)
    regions = len(reg)
    regcount = sum(reg.values())
    
    print(f"{f} {regions} {regcount}")
