import json
from collections import Counter
from operator import add


def characterize_error(rec):
    """Return a summary string for the _last_ error on this record."""
    err = rec['errors'][-1].lower()
    if 'connection aborted' in err:
        estr = 'Connection aborted'
    elif 'eof occurred in violation of protocol (_ssl.c:645)' in err:
        estr = 'TLS protocol violation (1)'
    elif 'ssl: sslv3_alert_handshake_failure' in err:
        estr = 'TLS handshake error'
    elif 'ssl: tlsv1_alert_internal_error' in err:
        estr = 'TLS internal error'
    elif "104, 'connection reset by peer'" in err:
        estr = 'Connection reset by peer'
    elif 'temporary failure in name resolution' in err:
        estr = 'DNS failure (1)'
    elif 'no address associated with hostname' in err:
        estr = 'DNS failure (2)'
    elif 'caused by connecttimeouterror' in err:
        estr = 'Connection timeout'
    elif 'read timed out' in err:
        estr = 'Read timeout'
    elif 'connection refused' in err:
        estr = 'Connection refused'
    elif 'connection broken: incompleteread' in err:
        estr = 'Connection dropped (incomplete read)'
    elif 'name or service not known' in err:
        estr = 'DNS failure (3)'
    elif 'network is unreachable' in err:
        estr = 'Network unreachable'
    elif 'no route to host' in err:
        estr = 'Host unreachable'
    elif "encoding with 'idna' codec failed" in err:
        estr = "IDNA encoding error"
    elif 'received response with content-encoding: gzip, but failed' in err:
        estr = "Error decoding compressed content"
    elif 'received response with content-encoding: deflate, but failed':
        estr = "Error decoding compressed content"
    else:
        estr = err
    return estr


def analyze_nospark(files_in, file_out, **kwargs):
    errdict = Counter()
    allrec = 0
    xall = {}
    for inf in files_in:
        with open(inf) as infile:
            for recgroup in map(json.loads, iter(infile)):
                allrec += len(recgroup)
                xcount = Counter(
                    map(characterize_error,
                        filter(lambda rec: not rec['success'], recgroup)))
                errdict.update(xcount)
                xall[inf] = xcount
    numerrs = sum(errdict.values())
    print("# numerrs all errfrac")
    print("{} {} {:.3}".format(numerrs, allrec, numerrs/allrec))
    print("# all")
    print(",".join(":".join([k, str(v)]) for k, v in errdict.items()))
    for inf, xcount in xall.items():
        print("# {}".format(inf))
        print(",".join(":".join([k, str(v)]) for k, v in xcount.items()))


def analyze(sc, files_in, file_out, **kwargs):
    allrec = 0
    xall = {}
    allcount = Counter()
    for inf in files_in:
        wfile = sc.textFile(inf)
        records = wfile.flatMap(json.loads)
        records.cache()
        allrec += records.count()
        errdict = records.filter(
            lambda rec: not rec['success']).map(
                characterize_error).map(
                    lambda el: (el, 1)).countByKey()
        xall[inf] = errdict
        allcount.update(errdict)

    numerrs = sum(allcount.values())
    with open(file_out, "w") as outfile:
        print("# numerrs all errfrac", file=outfile)
        print("{} {} {:.3}".format(numerrs, allrec, numerrs/allrec),
              file=outfile)
        print("# all", file=outfile)
        print(",".join(":".join([k, str(v)]) for k, v in allcount.items()),
              file=outfile)
        for inf, counts in xall.items():
            print("# {}".format(inf), file=outfile)
            print(",".join(":".join([k, str(v)]) for k, v in counts.items()),
                  file=outfile)
