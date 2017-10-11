"""
Make bar plots for each CC, two bars for each language showing default 
primary lang + content-detected language.
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


def loadcdetect(cc, xtype):
    with open("../contentdetect.txt") as infile:
        while True:
            line = infile.readline()
            if line.startswith(f"# {cc.upper()} {xtype}"):
                data = infile.readline()
                return json.loads(data)


def plotit(cc, xli):
    default = loadlangdata(cc, 'default')
    cdetect = dict(loadcdetect(cc, 'default'))

    # dict, where key is langcode and val is count
    default = sorted(default.items(), key=lambda x: x[1], reverse=True)
    default = default[:40]

    cdetect = [(t[0],cdetect.get(t[0],0)) for t in default]

    
    defaultct = [t[1] for t in default]
    detectct = [t[1] for t in cdetect]
    langcodes = [t[0] for t in default]

    data = [(defaultct[i], detectct[i]) for i in range(len(defaultct))]

    fig = plt.figure(figsize=(7,3))
    ax = fig.add_subplot(111)
    
    ax.set_xlabel("language code")
    ax.set_ylabel("observed occurrences")
    ax.set_ylim(1, max(detectct)*1.1)
    # plt.title(f"{cc} primary language")
    plt.yscale('log')


    dim = 2 # two compare bars for each langcode
    w = 0.75
    dimw = w / dim

    x = np.arange(len(data))
    for i in range(len(data[0])):
        y = [d[i] for d in data]
        b = ax.bar(x + i * dimw, y, dimw, bottom=0.001)

    plt.xticks(x + dimw / 2, langcodes, rotation=60) # map(str, x))
    ax.tick_params(axis='x', labelsize=8)

    plt.legend(["Language tag included", "Language detected"], loc=1, fontsize=8)

    plt.tight_layout()
    plt.savefig(f"{cc}_cdetect.pdf", bbox_inches='tight')
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
