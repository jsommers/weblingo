import sys
import math
from json import loads, dumps
from collections import Counter, defaultdict
import itertools
import functools
import glob
import os
import re
from copy import copy
from urllib.parse import urlsplit

import langtags


def _ltag(s):
    if s.strip() == '':
        return ''

    try:
        t = langtags.Tag(s, True)
    except:
        return 'INVALID'
    else:
        if t.language is not None:
            if t.language.macrolanguage:
                return t.language.macrolanguage
            else:
                return t.language.tag
        else:
            if len(t) > 0 and t[0].preferred_value:
                return t[0].preferred_value
            else:
                return s.lower()  # not invalid, but no language
                                  # just return the whole thing, but downcased


def _getexc(rec):
    return set([_ltag(s) for s in rec['explicit']])


def _getinf(rec):
    return set([_ltag(s) for s in rec['inferred']])


def _getprimary(rec):
    return set([_ltag(s) for s in rec['primary']])


def _getcontent_detect(rec):
    langinf = rec['content_detect']
    det = set()
    if langinf['reliable']:
        for xlc, lli in langinf['languages'].items():
            if lli[1] >= 10 and xlc != 'un':
                lc = _ltag(xlc)
                if lc:
                    det.add(lc)
    return det


def _print_counter(c, outfile):
    print(dumps(sorted(c.items())), file=outfile)

def _analyze_rec(sayslang, islang, cc, xtype, rec):
    continf = _getcontent_detect(rec)
    prim = _getprimary(rec)
    if prim and 'INVALID' not in prim:
        for p in prim:
            sayslang[p] += 1
        for p in continf:
            islang[p] += 1


EXT = '_10k'
# EXT = ''

def main():
    xdir = "testdata"
    outfile = open('cdetect2.txt', 'w')
    for f in glob.glob(f"{xdir}/*_default_summary{EXT}.json"):
        path, ext = os.path.splitext(os.path.basename(f))
        mobj = re.match("^(\w{2})_(\w+)_summary"+EXT, path)
        cc = mobj.group(1)
        xtype = mobj.group(2)

        print(f"{cc}/{xtype}: ", end='',flush=True)
        sayslang = Counter()
        islang = Counter()
        with open(f) as inf:
            for i, recgroup in enumerate(map(loads, iter(inf))):
                if i % 100 == 0:
                    print(".", end='', flush=True)
                for rec in recgroup:
                    _analyze_rec(sayslang, islang, cc, xtype, rec)

        print(f"# {cc} {xtype} (given langtags next, detected next)", file=outfile)
        _print_counter(sayslang, outfile)
        _print_counter(islang, outfile)

main()
