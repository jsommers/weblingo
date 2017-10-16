"""
Make bar plots for all CCs, somehow on the same plot
"""

from glob import glob
import os
import re
import json
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
from colorgen import colorgen


def loadlangdata(cc, xtype):
    with open(f"../testdata/{cc}_{xtype}_primarylang.txt") as infile:
        for i in range(4):
            infile.readline()
        line = infile.readline()
        i = line.find('{')
        j = line.find('}', i+1)
        return eval(line[i:(j+1)])

def main(xtype):
    # langpref, default
    cclist = sorted(['AR', 'GB', 'JP', 'TH', 'KE'])
    xdata = {}
    xdata['US'] = loadlangdata('US', xtype)

    for cc in cclist:
        xdata[cc] = loadlangdata(cc, xtype)

    usdata = xdata['US']
    xdata['US'] = sorted(usdata.items(), key=lambda t:t[1], reverse=True)
    print("Num langs for US", len(xdata['US']))
    xdata['US'] = xdata['US'][:40]

    langcodes = [t[0] for t in xdata['US']]

    for cc in cclist:
        xd = xdata[cc]
        print(f"Num langs for {cc}: {len(xd)}")
        xdata[cc] = [xd.get(t[0], 0) for t in xdata['US']]

    xdata['US'] = [t[1] for t in xdata['US']]

    alldata = []
    cclist.append('US')
    for i in range(len(xdata['US'])):
        alldata.append(tuple([xdata[cc][i] for cc in cclist]))

    fig = plt.figure(figsize=(7,3))
    ax = fig.add_subplot(111)
    
    ax.set_xlabel("language subtag")
    ax.set_ylabel("observed occurrences")
    ax.set_ylim(1,500000)
    # plt.title(f"{cc} primary language")
    plt.yscale('log')

    dim = 6 # two compare bars for each langcode
    w = 0.75
    dimw = w / dim

    cg = colorgen(6)

    x = np.arange(len(alldata))
    for i in range(len(alldata[0])):
        y = [d[i] for d in alldata]
        b = ax.bar(x + i * dimw, y, dimw, bottom=0.001, label=cclist[i])

    plt.xticks(x + dimw / dim, langcodes, fontsize=9, rotation=60) 

    plt.legend(cclist, loc=1, fontsize=8, ncol=9)
    plt.tight_layout()
    plt.savefig(f"primaryallcc_{xtype}.pdf", bbox_inches='tight')
    plt.clf()


main('default')
main('langpref')
