#!/usr/bin/env python3

from glob import glob
from json import loads

def get_line(f, lnum):
    with open(f) as infile:
        while lnum > 0:
            line = infile.readline()
            lnum -= 1
    return loads(line)

def get_explicit(f):
    return get_line(f, 2) 

def get_inferred(f):
    return get_line(f, 4)

for f in glob("testdata/??_*_alllang.txt"):
    exp = get_explicit(f)
    inf = get_inferred(f)
    expcount = sum(exp.values())
    infcount = sum(inf.values())
    infset = set(inf.keys())
    expset = set(exp.keys())
    newtags = infset - expset
    print(f"{f} inf {len(inf)} exp {len(exp)} infcount {infcount} expcount {expcount} {len(inf)/(len(inf)+len(exp))} {infcount/(infcount+expcount)} {len(newtags)}")
