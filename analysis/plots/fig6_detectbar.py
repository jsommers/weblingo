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
from colorgen import colorgen


def plotit(cc, given, detected):
    censorlist = set(['ms', 'br'])
    for c in censorlist:
        if c in given:
            del given[c]
        if c in detected:
            del detected[c]

    # dict, where key is langcode and val is count
    given = sorted(given.items(), key=lambda x: x[1], reverse=True)
    given = [t for t in given if t[0] in detected]
    default = given[:40]
    cdetect = [(t[0],detected[t[0]]) for t in default]

    defaultct = [t[1] for t in default]
    detectct = [t[1] for t in cdetect]
    langcodes = [t[0] for t in default]

    data = [(defaultct[i], detectct[i]) for i in range(len(defaultct))]

    fig = plt.figure(figsize=(6,3))
    ax = fig.add_subplot(111)
    
    ax.set_xlabel("language subtag")
    ax.set_ylabel("observed occurrences")
    ax.set_ylim(1, max(detectct)*1.1)
    # plt.title(f"{cc} primary language")
    plt.yscale('log')


    dim = 2 # two compare bars for each langcode
    w = 0.75
    dimw = w / dim

    cg = colorgen(2)
    x = np.arange(len(data))
    for i in range(len(data[0])):
        y = [d[i] for d in data]
        b = ax.bar(x + i * dimw, y, dimw, bottom=0.001, color=next(cg))

    plt.xticks(x + dimw / 2, langcodes, rotation=60) # map(str, x))
    ax.tick_params(axis='x', labelsize=9)

    plt.legend(["Language subtag included", "Language detected"], loc=1, fontsize=9)

    plt.tight_layout()
    plt.savefig(f"{cc}_cdetect.pdf", bbox_inches='tight')
    plt.clf()


def main():
    datahash = {}

    with open('../cdetect2.txt') as infile:
        for line in infile:
            if line.startswith('#'):
                fields = line.split()
                cc = fields[1]
                xtype = fields[2]
                given = dict(json.loads(infile.readline()))
                detected = dict(json.loads(infile.readline()))
                if xtype == 'default':
                    datahash[cc] = [given, detected]

    for cc, data in datahash.items():
        plotit(cc, *data)

main()
