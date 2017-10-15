"""
Make bar plots for region for each CC
"""

import sys
from glob import glob
import os
import re
import json
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
from colorgen import colorgen


def loadlangdata(cc, xtype):
    line = 7
    with open(f"../testdata/{cc}_{xtype}_primarylang.txt") as infile:
        while line != 0:
            nextline = infile.readline()
            line -= 1
        i = nextline.find('{')
        j = nextline.find('}', i+1)
        return eval(nextline[i:(j+1)])


def plotit(cc, xli):
    # langpref, default
    regiondata = loadlangdata(cc, 'langpref')
    # default = loadlangdata(cc, 'default')

    # dict, where key is langcode and val is count
    defaultli = sorted(regiondata.items(), key=lambda x: x[1], reverse=True)

    defaultli = defaultli[:40]

    region = [t[0] for t in defaultli]
    count = [t[1] for t in defaultli]
    ranks = list(range(len(count)))

    fig = plt.figure(figsize=(7,3))
    ax = fig.add_subplot(111)
    
    ax.set_xlabel("region subtag")
    ax.set_ylabel("observed occurrences")
    ax.set_ylim(1, max(count)*1.1)
    plt.yscale('log')

    cg = colorgen(2)
    ax.bar(ranks, count, color=next(cg))
    plt.xticks(ranks, region, rotation=60)
    ax.tick_params(axis='x', labelsize=9)
    plt.tight_layout()
    plt.savefig(f"{cc}_region.pdf", bbox_inches='tight')
    plt.clf()


def main():
    cchash = defaultdict(list)

    for name in glob("../testdata/??_*_alllang.txt"):
        base, ext = os.path.splitext(os.path.basename(name))
        mobj = re.match("(\w{2})_(\w+)_alllang", base)
        if mobj:
            cc = mobj.group(1)
            xtype = mobj.group(2)
            cchash[cc].append(xtype)

    for cc, xli in cchash.items():
        if len(xli) == 2:
            plotit(cc, xli)


main()
