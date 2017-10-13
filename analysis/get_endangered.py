import sys
import xml.etree.ElementTree as ET
from glob import glob
import json
from collections import defaultdict
import langtags

"""
Classification (least to most severe):

Vulnerable
Definitely endangered
Severely endangered
Critically endangered
Extinct
"""


lcode_remap = {}
with open('iso-639-3.tab.txt') as infile:
    infile.readline() # discard header line
    for line in infile:
        fields = line.split('\t')
        iso3 = fields[0].strip()
        iso1 = fields[3].strip()
        if not iso1:
            iso1 = iso3
        lcode_remap[iso3] = iso1


tree = ET.parse('unesco_atlas_languages_limited_dataset.xml')
root = tree.getroot()

status = {}
blacklist = set(['fr', 'hu', 'el', 'sv', 'sk', 'bg', 'sl', 'ca', 'lv', 'et', 'hy', 'lb'])

for child in root:
    # print(child.tag)
    endanger = ''
    t = None
    xname = None
    for subchild in child:
        if subchild.tag == 'Degree_of_endangerment':
            endanger = subchild.text 
        elif subchild.tag == 'ISO639-3_codes':
            if subchild.text is None:
                continue

            codes = [ t.strip() for t in subchild.text.split(',') ]
            for c in codes:
                remap = lcode_remap.get(c, '')
                if remap:
                    t = langtags.Tag(remap)
                    if str(t) in blacklist:
                        t = None
        elif subchild.tag == 'Name_in_English':
            xname = subchild.text.strip()

    if t and endanger:
        status[t.language.subtag] = (endanger, xname)


def getdata(f):
    with open(f) as infile:
        lines = 6
        while lines > 0:
            nextline = infile.readline()
            lines -= 1
    return json.loads(nextline)

for f in glob("testdata/??_default_alllang.txt"):
    endangerdict = defaultdict(list)
    d = getdata(f)
    for tag, ct in d.items():
        if tag in status:
            stat, name = status[tag]
            endangerdict[stat].append((tag, ct, name))

    print()
    print(f)
    for statustype, dlist in endangerdict.items():
        dlist.sort(key=lambda t: t[1], reverse=True)
        xsum = sum([t[1] for t in dlist])
        print(statustype, len(dlist), xsum, dlist[:20])


