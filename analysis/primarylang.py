from json import loads
import langtags
from collections import defaultdict

def _primary_lang(rec):
    return [(s,1) for s in rec['primary']]

def _cmp_primary_page_lang(rec):
    primary = set()
    for sup in rec['primary']:
        for s in sup.split(','):
            try:
                t = langtags.Tag(s)
                primary.add(t.language.tag)
            except:
                pass

    # "content_detect": {"lenplaintext": 304, "languages": {"en": ["ENGLISH", 99], "un": ["Unknown", 0]}, "reliable": true}},
    det = rec['content_detect']
    detlang = set()
    if det['reliable']:
        for k, xli in det['languages'].items():
            if xli[1] > 50 and k != 'un':
                try:
                    t = langtags.Tag(k)
                except:
                    pass
                else:
                    if t.language is not None:
                        detlang.add(t.language.tag)

    intx = primary & detlang
    if intx:
        return (tuple(intx), 1)
    else:
        return (tuple(primary-detlang), tuple(detlang-primary), 1)


def analyze(sc, files_in, file_out, **kwargs):
    counts = defaultdict(int)
    langcmp = defaultdict(int)
    allcount = 0

    for inf in files_in:
        wfile = sc.textFile(inf)
        allrecs = wfile.flatMap(loads)
        allrecs.persist()
        allcount += allrecs.count()
        primary_all = allrecs.flatMap(_primary_lang).countByKey()
        lcompare = allrecs.map(_cmp_primary_page_lang).countByKey()
        counts.update(primary_all)
        langcmp.update(lcompare)


    langonly = defaultdict(int)
    region = defaultdict(int)
    invalid = defaultdict(int)

    nonemptycount = 0
    for tuber,ct in counts.items():
        nonemptycount += ct
        for t in tuber.split(','):
            try:
                tag = langtags.Tag(t.strip(), normalize=True)
            except:
                invalid[t] += ct
            else:
                langonly[tag.language.tag] += ct
                if tag.region:
                    region[tag.region.tag] += ct

    emptycount = allcount - nonemptycount
    counts[''] = emptycount

    with open(file_out, 'w') as outfile:
        print("# total {} nonempty {} empty {}".format(
                allcount, nonemptycount, emptycount), file=outfile)
        print("# raw counts", file=outfile)
        print(counts, file=outfile)
        print("# langonly", file=outfile)
        print(langonly, file=outfile)
        print("# region", file=outfile)
        print(region, file=outfile)
        print("# invalid", file=outfile)
        print(invalid, file=outfile)
        print("# lcompare (match,) (says,is)", file=outfile)
        print(langcmp, file=outfile)

