from json import loads, dumps
import langtags
from collections import Counter


def _extvary(rec):
    return [(vt,1) for vt in rec['vary']]


def analyze(sc, files_in, file_out, **kwargs):
    varytype = Counter()
    allcount = 0

    for inf in files_in:
        wfile = sc.textFile(inf)
        allrecs = wfile.flatMap(loads)
        allrecs.persist()
        allcount += allrecs.count()
        xvary = allrecs.flatMap(_extvary).countByKey()
        print(xvary)
