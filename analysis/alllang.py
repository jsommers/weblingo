from json import loads, dumps
import langtags
from collections import Counter


def _ltag(s):
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


def _content_lang(rec):
    x = rec['content_detect']
    if x['reliable']:
        if rec['explicit']:
            return []
        else:
            xlang = x['languages']     
            return [(_ltag(k),1) for k,vl in xlang.items() if vl[1] > 50 ]
    else:
        return []

# use a set below to ensure no double-counting for lang tags in a 
# situation like en-US, en-GB, etc.


def _getexc(rec):
    return set([_ltag(s) for s in rec['explicit']])


def _getinf(rec):
    return set([_ltag(s) for s in rec['inferred']])


def _explicit_lang(rec):
    return [(t,1) for t in _getexc(rec)]


def _inferred_lang(rec):
    return [(t,1) for t in _getinf(rec)]


def _langcount(rec):
    return (len(_getexc(rec)|_getinf(rec)),1)


def _write_json(s, struct, fh):
    print("# {}".format(s), file=fh)
    print(dumps(struct), file=fh)


def analyze(sc, files_in, file_out, **kwargs):
    expl = Counter()
    infr = Counter()
    cont = Counter()
    lpersite = Counter()
    allcount = 0

    for inf in files_in:
        wfile = sc.textFile(inf)
        allrecs = wfile.flatMap(loads)
        allrecs.persist()
        allcount += allrecs.count()
        explicits = allrecs.flatMap(_explicit_lang).countByKey()
        expl.update(explicits)
        inferred = allrecs.flatMap(_inferred_lang).countByKey()
        infr.update(inferred)
        contx = allrecs.flatMap(_content_lang).countByKey()
        cont.update(contx)
        lcount = allrecs.map(_langcount).countByKey()
        lpersite.update(lcount)


    expl_infr = Counter()
    expl_infr.update(expl)
    expl_infr.update(infr)

    with open(file_out, 'w') as outfile:
        _write_json("explicit tags", expl, outfile)
        _write_json("inferred tags", infr, outfile)
        _write_json("explicit + inferred", expl_infr, outfile)
        _write_json("content inference", cont, outfile)
        _write_json("languages per site", lpersite, outfile)
