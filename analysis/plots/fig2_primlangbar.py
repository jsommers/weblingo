"""
Make bar plots for each CC, two bars for each language showing default/langpref.
"""

from glob import glob
import os
import re
import json
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict


def loadlangdata(cc, xtype):
    with open(f"../testdata/{cc}_{xtype}_primarylang.txt") as infile:
        for i in range(4):
            infile.readline()
        line = infile.readline()
        i = line.find('{')
        j = line.find('}', i+1)
        return eval(line[i:(j+1)])


def plotit(cc, xli):
    # langpref, default
    langpref = loadlangdata(cc, 'langpref')
    default = loadlangdata(cc, 'default')

    # dict, where key is langcode and val is count
    defaultli = sorted(default.items(), key=lambda x: x[1], reverse=True)
    MAX = 40
    defaultli = defaultli[:MAX] 

    langprefct = [langpref.get(t[0], 0) for t in defaultli]
    defaultct = [t[1] for t in defaultli]
    langcodes = [t[0] for t in defaultli]

    data = [(defaultct[i], langprefct[i]) for i in range(len(defaultct))]

    fig = plt.figure(figsize=(7,3))
    ax = fig.add_subplot(111)
    
    ax.set_xlabel("language code")
    ax.set_ylabel("observed occurrences")
    ax.set_yscale('log')
    ax.set_ylim(1, 350000)
    # plt.title(f"{cc} primary language")
    # plt.yscale('log')

    dim = 2 # two compare bars for each langcode
    w = 0.75
    dimw = w / dim

    x = np.arange(len(data))
    for i in range(len(data[0])):
        y = [d[i] for d in data]
        b = ax.bar(x + i * dimw, y, dimw, bottom=0.001)

    plt.xticks(x + dimw / 2, langcodes, fontsize=8, rotation=60)

    plt.legend(["Default", "Local preference"], loc=1, fontsize=8)
    plt.tight_layout()
    plt.savefig(f"{cc}_primarycmp.pdf", bbox_inches='tight')
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
