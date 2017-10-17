import sys
from glob import glob
import os
import re
import json
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
from colorgen import colorgen


def loadlangdata():
    line = 9
    with open("../alllang.txt") as infile:
        while line != 0:
            nextline = infile.readline()
            line -= 1
        return json.loads(nextline)


def plotit():
    langdata = loadlangdata()
    # array of arrays, nested array is numlangs,count
    langdata = [ t for t in langdata if t[0] != 'INVALID' ]
    langdata.sort(key=lambda t: t[1], reverse=True)

    lcode = [ldata[0] for ldata in langdata]
    counts = [ldata[1] for ldata in langdata]

    lcode = lcode[:50]
    counts = counts[:50]
    ranks = list(range(len(lcode)))

    fig = plt.figure(figsize=(7,2.5))
    ax = fig.add_subplot(111)

    ax.set_xlabel("language subtag")
    ax.set_ylabel("observed occurrences")
    plt.yscale('log')
    plt.ylim(1, 400000)

    cg = colorgen(2)
    ax.bar(ranks, counts) # , color=next(cg))

    plt.xticks(ranks, lcode, rotation=80)
    ax.tick_params(axis='x', labelsize=9)

    plt.tight_layout()
    plt.savefig("alllang.pdf", bbox_inches='tight')
    plt.clf()


def main():
    plotit()

main()
