import sys
from glob import glob
import os
import re
import json
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict


def loadlangdata():
    line = 3
    with open("../alllang.txt") as infile:
        while line != 0:
            nextline = infile.readline()
            line -= 1
        return json.loads(nextline)


def plotit():
    langdata = loadlangdata()
    # array of arrays, nested array is numlangs,count

    ranks = [ldata[0] for ldata in langdata]
    counts = [ldata[1] for ldata in langdata]

    fig = plt.figure(figsize=(7,3))
    ax = fig.add_subplot(111)
    ax.set_xlabel("number of languages offered by a given site")
    ax.set_ylabel("observed occurrences")
    # plt.title(f"{cc} primary language")
    ax.set_yscale('log')
    ax.set_xlim(-1,105)
    ax.set_ylim(1, 600000)

    ax.bar(ranks, counts)

    #for i in range(len(data[0])):
    #    y = [d[i] for d in data]
    #    b = plt.bar(x + i * dimw, y, dimw, bottom=0.001)

    # plt.xticks(ranks)

    plt.tight_layout()
    plt.savefig(f"langsoffered.pdf", bbox_inches='tight')


def main():
    plotit()

main()
