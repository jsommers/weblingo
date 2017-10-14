#!/usr/bin/env python3

from glob import glob
import langtags
from collections import Counter
from json import dumps

def get_line(f, lnum):
    with open(f) as infile:
        while lnum > 0:
            line = infile.readline()
            lnum -= 1
    xopen = line.find('{')
    xclose = line.find('}', len(line)-10)
    return eval(line[xopen:(xclose+1)])

def get_all(f):
    return get_line(f, 3)

for f in glob("testdata/??_*_primarylang.txt"):
    alltags = get_all(f)
    numsub = Counter()
    script = Counter()
    private = Counter()
    grand = Counter()

    for rawt,ct in alltags.items():
        try:
            t = langtags.Tag(rawt)
            numsub[len(t)] += ct
            if t.private:
                private[t.private.subtag] += ct
            if t.script:
                script[t.script.subtag] += ct
            if t.grandfathered:
                grand[t.grandfathered.subtag] += ct
        except:
            pass
    print(f"# tag lengths {f}")
    print(dumps(dict(numsub)))
    print(f"# private {f}")
    print(dumps(dict(private)))
    print(f"# script {f}")
    print(dumps(dict(script)))
    print(f"# grand {f}")
    print(dumps(dict(grand)))
    
