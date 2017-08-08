"""
Do web requests with different forms of Accept-Language content negotiation.

Allow null Accept-Language header, a specific value for A-L, and optionally
include Negotiate header to try to determine whether server does transparent
content negotiation.
"""
import sys
import argparse
import time
import json
import lzma
import multiprocessing as mp
import ctypes
import base64

import requests
requests.packages.urllib3.disable_warnings()  # disable ssl warnings

WRITEBUF = 100
DEFAULT_WORKERS = 8
MAX_RETRIES = 3


def _compressstr(s):
    """
    Take a single string and lzma compress it.

    Used for compressing response data from a web transaction.
    """
    return base64.b64encode(
        lzma.compress(
            s.encode('utf8'), preset=lzma.PRESET_EXTREME)).decode('utf8')


class _ReqConfig(object):
    def __init__(self):
        self._langpref = ''
        self._tcn = False

    @property
    def langpref(self):
        return self._langpref

    @langpref.setter
    def langpref(self, value):
        self._langpref = value

    @property
    def tcn(self):
        return self._tcn

    @tcn.setter
    def tcn(self, value):
        self._tcn = bool(value)


def _check_content_type(headers):
    xtype = headers.get('content-type', None)
    if xtype is None:
        return int(headers.get('content-length', 1e20)) < 1e20
    else:
        return xtype.startswith('text/') or \
               xtype.startswith('application/xml') or \
               xtype.startswith('application/xhtml')


def _make_req(hostname, cfg, verbose):
    results = {'reqhost': hostname}
    reqheaders = {
      "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) "
                    "AppleWebKit/601.3.9 (KHTML, like Gecko) "
                    "Version/9.0.2 Safari/601.3.9",
      "Accept": "text/html, application/xhtml+xml, "
                "application/xml;q=0.9, */*;q=0.8",
    }

    if cfg.tcn:
        reqheaders['Negotiate'] = '*'

    if cfg.langpref:
        reqheaders['Accept-Language'] = cfg.langpref

    results['start'] = time.time()
    if verbose > 1:
        print("Making request to {} with headers {}".format(
            hostname, reqheaders))

    errors = []
    results['errors'] = errors

    while True:
        response = None
        try:
            response = requests.get('http://{}'.format(hostname),
                                    allow_redirects=True, verify=False,
                                    headers=reqheaders, timeout=60,
                                    stream=True)
        except Exception as e:
            errors.append(str(e))
            if response is not None:
                response.close()
            results['success'] = False
            if len(errors) == MAX_RETRIES:
                return results
        else:
            results['success'] = True
            break


    # if here, then successful GET; but content is not yet retrieved
    if _check_content_type(response.headers):
        try:
            results['content'] = _compressstr(response.text)
        except Exception as e:
            errors.append(str(e))
            results['success'] = False
    else:
        results['content'] = ''

    response.close()

    results['end'] = time.time()
    if verbose > 1:
        print("Got response from {}: {} {}".format(
            hostname, response, response.headers))

    results['request_headers'] = {k.lower(): v for k, v in reqheaders.items()}
    results['response_headers'] = dict(response.headers.lower_items())
    results['url'] = response.url
    results['status_code'] = response.status_code
    results['status_reason'] = response.reason
    results['history'] = \
        [[xr.status_code, xr.url] for xr in response.history]
    return results


def _request_worker(start, done, workqueue, resultsqueue, verbose):
    start.wait()
    while not done.value:
        try:
            i, hostname, cfg = workqueue.get(True, 1)
        except:
            continue
        results = _make_req(hostname, cfg, verbose)
        resultsqueue.put((i, results))
    sys.exit()


def _read_input(infile):
    hostlist = []
    with lzma.open(infile, 'rt') as inp:
        for line in inp:
            rank, name = line.strip().split(',')
            hostlist.append(name)
    return hostlist


def _write_data(outfile, resultlist, totrec, intstart, start):
    if len(resultlist) == 0:
        return
    print(json.dumps(resultlist), file=outfile)
    now = time.time()
    secperrec = (now - start)/totrec
    secperrec_local = (now - intstart)/len(resultlist)
    print("Writing {} records (of {}); {:.2f} ({:.2f}) sec/rec".format(
          len(resultlist), totrec, secperrec_local, secperrec))


def _print_response(rdict):
    print("{} -> ".format(rdict['reqhost']), end='')
    if rdict['success']:
        print("{}/{}".format(
            rdict['status_code'], rdict['status_reason']))
    else:
        print("{}".format(','.join(rdict['errors'])))


def _manager(args, hostlist, langpref):
    numworkers = args.workers
    verbose = args.verbose
    outname = args.outfile

    reqcfg = _ReqConfig()
    reqcfg.tcn = args.tcn
    reqcfg.langpref = langpref

    doneval = mp.Value(ctypes.c_bool, 0)
    workqueue = mp.Queue()
    resultsqueue = mp.Queue()
    startbarrier = mp.Barrier(numworkers+1)
    workers = []

    for _ in range(numworkers):
        p = mp.Process(
            target=_request_worker,
            args=(startbarrier, doneval, workqueue, resultsqueue, verbose))
        workers.append(p)

    for i, p in enumerate(workers):
        p.start()
    print("{} workers started".format(i))

    print("Adding to work queue ... ", end='', flush=True)
    resultset = set()
    for i, host in enumerate(hostlist):
        workqueue.put((i, host, reqcfg))
        resultset.add(i)
        if i % 100 == 0 and i > 0:
            print("{} ".format(i), end='', flush=True)
    print("{} hosts to probe added; releasing workers.".format(i+1))
    startbarrier.wait()

    if outname == 'stdout':
        outfile = sys.stdout
    else:
        if not outname.endswith('.json'):
            outname = "{}.json".format(outname)
        openmode = 'w'
        if args.append:
            openmode = 'a'
        outfile = open("{}".format(outname), openmode)

    jointimeout = 5

    rlist = []
    totrec = 0
    intstart = start = time.time()
    try:
        while len(resultset) > 0:
            reqid, rdict = resultsqueue.get()
            if verbose:
                _print_response(rdict)
            resultset.discard(reqid)
            rlist.append(rdict)
            if len(rlist) >= WRITEBUF:
                totrec += len(rlist)
                _write_data(outfile, rlist, totrec, intstart, start)
                intstart = time.time()
                rlist = []
    except KeyboardInterrupt:
        print("Shutting down prematurely with keyboard interrupt")
        jointimeout = 1

    doneval.value = True

    totrec += len(rlist)
    _write_data(outfile, rlist, totrec, intstart, start)
    now = time.time()
    print("Total elapsed time {:.2f} sec".format(now-start))
    if outname != 'stdout':
        outfile.close()

    for p in workers:
        rv = p.join(jointimeout)
        if rv is None:
            p.terminate()
            p.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='The dance of Accept-Language content negotiation.')
    parser.add_argument('-H', '--host', dest='onehost', type=str,
                        default=None,
                        help='Specific host to test')
    parser.add_argument('-i', '--infile', dest='infile', type=str,
                        help='Input file to read with hostnames')
    parser.add_argument('-l', '--langpref', dest='langpref', type=str,
                        default='', help='Accept-Language header value')
    parser.add_argument('-m', '--maxreq', dest='maxreq', type=int, default=-1,
                        help='Max number of hosts to query')
    parser.add_argument('-o', '--outfile', dest='outfile', type=str,
                        default='results.json',
                        help='Output file (JSON format)')
    parser.add_argument('-s', '--start', dest='start', type=int,
                        default=0,
                        help='Index (line) of input file on which to start')
    parser.add_argument('-t', '--tcn', dest='tcn', action="store_true",
                        default=False,
                        help='Send Negotiate header for TCN.')
    parser.add_argument('-v', '--verbose', dest='verbose', action='count',
                        default=0, help='Turn on verbose output.')
    parser.add_argument('-w', '--workers', dest='workers', type=int,
                        default=DEFAULT_WORKERS,
                        help='Number of request workers')
    parser.add_argument('-W', '--writebuf', dest='writebuf', type=int,
                        default=25, help='Write buffer size.')
    parser.add_argument('-a', '--append', dest='append', action="store_true",
                        default=False, help="Append to output file")
    args = parser.parse_args()

    WRITEBUF = args.writebuf

    print("Making {} requests from {} using langpref '{}' "
          "with {} workers.".format(
            args.maxreq, args.infile, args.langpref, args.workers))
    langpref = args.langpref

    if args.infile is None and args.onehost is None:
        parser.print_usage()
        sys.exit()
    if args.onehost is not None:
        hostlist = [args.onehost]
    else:
        hostlist = _read_input(args.infile)
        if args.start > 0:
            hostlist = hostlist[args.start:]
    maxhosts = len(hostlist) if args.maxreq == -1 else args.maxreq
    hostlist = hostlist[:maxhosts]
    _manager(args, hostlist, langpref)
