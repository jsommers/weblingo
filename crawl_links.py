"""
Collect site names from starting at a particular site by crawling.  Don't
crawl more than X levels deep from each host
"""
import sys
import os
import argparse
import time
import json
import urllib.parse as up
import lzma
import base64
from collections import Counter
import random
import pickle
import multiprocessing as mp
import queue

from bs4 import BeautifulSoup as bs
import requests
requests.packages.urllib3.disable_warnings()  # disable ssl warnings

import langtags

MAX_RETRIES = 3
PARSER = "lxml"
global SEARCHLANG
SEARCHLANG = 'cy'
global WHITELIST
WHITELIST = ['.uk', '.cymru', '.wales']


def _compressstr(s):
    """
    Take a single string and lzma compress it.

    Used for compressing response data from a web transaction.
    """
    return base64.b64encode(
        lzma.compress(
            s.encode('utf8'), preset=lzma.PRESET_EXTREME)).decode('utf8')


def _check_content_type(headers):
    xtype = headers.get('content-type', None)
    if xtype is None:
        return int(headers.get('content-length', 1e20)) < 1e20
    else:
        return xtype.startswith('text/') or \
               xtype.startswith('application/xml') or \
               xtype.startswith('application/xhtml')


def _make_req(hostname, langpref, verbose, rqueue):
    results = {'reqhost': hostname}
    reqheaders = {
      "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) "
                    "AppleWebKit/601.3.9 (KHTML, like Gecko) "
                    "Version/9.0.2 Safari/601.3.9",
      "Accept": "text/html, application/xhtml+xml, "
                "application/xml;q=0.9, */*;q=0.8",
    }

    reqheaders['Accept-Language'] = langpref

    results['start'] = time.time()
    if verbose > 1:
        print("Making request to {} with headers {}".format(
            hostname, reqheaders))

    errors = []
    results['errors'] = errors

    url = hostname
    if not up.urlparse(hostname).scheme:
        url = 'http://{}'.format(hostname)

    while True:
        response = None
        try:
            response = requests.get(url, allow_redirects=True, verify=False,
                                    headers=reqheaders, timeout=60,
                                    stream=True)
        except Exception as e:
            errors.append(str(e))
            if response is not None:
                response.close()
            results['success'] = False
            if len(errors) == MAX_RETRIES:
                rqueue.put(results)
                sys.exit()
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

    if results['success']:
        try:
            results['soup'] = bs(response.text, PARSER)
        except:
            results['soup'] = bs('', PARSER)

    try:
        response.close()
    except:
        pass

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
    rqueue.put(results)
    sys.exit()


def _read_input(infile):
    hostlist = []
    with lzma.open(infile, 'rt') as inp:
        for line in inp:
            line = line.strip()
            firstcomma = line.find(',')
            name = line[(firstcomma+1):]
            hostlist.append(name)
    return hostlist


def _print_response(rdict, verbose):
    print("{} -> ".format(rdict['reqhost']), end='')
    if rdict['success']:
        print("{}/{}".format(
            rdict['status_code'], rdict['status_reason']))
    else:
        print("{}".format(','.join(rdict['errors'])))


def _check_langset(lset, lt=SEARCHLANG):
    for st in lset:
        try:
            t = langtags.Tag(st)
            if t.language.subtag == lt:
                return True
        except:
            pass
    return False


def _do_analysis(rec, verbose):
    def _offers_welsh(xd):
        if SEARCHLANG in xd['inferred'] or SEARCHLANG in xd['primary']:
            return True

        for lang,xli in xd['content_detect']['languages'].items():
            if lang == SEARCHLANG and xli[1] >= 33:
                return True

        return False


    from analysis import spark_lang_extract as analyze
    if verbose:
        print("Results for {} ({})".format(rec['reqhost'], rec['url']))
        print("\tAccept-Language: {}".format(rec['request_headers'].get('accept-language', 'No header')))
    try:
        xd = analyze._rec_analyze(rec)
    except Exception as e:
        print("Analysis exception: {}".format(str(e)))
        return False, None, [], []

    if verbose:
        print(xd)

    if xd is None:
        return False, None, [], [], {}
    elif not _offers_welsh(xd):
        return False, None, [], [], {}


    lang_links = set()
    other_links = set()
    link_lang_data = {}

    for el in rec['soup'].descendants:
        if el.name is None or el.name == 'script':
            continue

        if el.name == 'a' or el.name == 'link':
            xset = analyze._check_explicit_lang_tag(el)

            if not xset:
                try:
                    xset = analyze._analyze_link(el, rec['reqhost'])
                except:
                    xset = []

            addset = other_links
            if _check_langset(xset):
                addset = lang_links

            if 'href' in el.attrs:
                href = el.attrs['href']
                try:
                    components = up.urlsplit(href)
                except ValueError:
                    components = up.urlsplit('')

                _, querycode = analyze._analyze_qs(components.query)
                if querycode:
                    xset.add(str(querycode))
                _, netloccode = analyze._analyze_netloc(components.netloc)
                if netloccode:
                    xset.add(str(netloccode))
                _, pathcode = analyze._analyze_netpath(components.path)
                if pathcode:
                    xset.add(str(pathcode))

                if components.netloc:
                    addset.add(components.netloc)
                    link_lang_data[components.netloc] = tuple(xset)
                elif href.startswith('/'):
                    comp = up.urlsplit(rec['url'])
                    assert(comp.netloc)
                    addset.add(comp.netloc + href)
                    link_lang_data[comp.netloc + href] = tuple(xset)

    return True, xd, list(lang_links), list(other_links), link_lang_data


def _urlhost(xurl):
    if '/' not in xurl:
        return xurl
    if not xurl.startswith('http'):
        return xurl.split('/')[0]
    return up.urlparse(xurl).netloc


def _extract_rec_data(hrec):
    def _getfield(hrec, key):
        v = hrec.get(key, [])
        if v is None:
            v = []
        return v

    p = ','.join(_getfield(hrec, 'primary'))
    exp = ','.join(_getfield(hrec, 'explicit'))
    inf = ','.join(_getfield(hrec, 'inferred'))
    det = ''
    if hrec['content_detect']['reliable']:
        det = ','.join(hrec['content_detect']['languages'].keys())
    return '::'.join([exp, p, inf, det])


def _manager(args, hostlist, langpref):
    verbose = args.verbose

    sitecount = Counter()
    already_done = set()
    whitelisted = set()
    howmany = 0

    saved_state = _dounpickle(args.picklefile)
    if saved_state['hostlist']:
        hostlist += saved_state['hostlist']
    if saved_state['sitecount']:
        sitecount.update(saved_state['sitecount'])
    if saved_state['already_done']:
        already_done = already_done.union(saved_state['already_done'])
    if saved_state['whitelisted']:
        whitelisted = whitelisted.union(saved_state['whitelisted'])

    if not hostlist:
        print("No host list to crawl after resurrecting old state.")
        sys.exit()

    outfile = open(args.outfile, 'a')

    def _looks_like_text(link):
        blacklist = ['.jpg', '.png', '.css', '.js', '.zip',
                     '.doc', '.docx', '.xls', '.xlsx', '.csv']
        for ext in blacklist:
            if link.endswith(ext):
                return False
        return True

    def _blacklisted(url):
        host = _urlhost(url)
        for h in ['facebook', 'google', 'twitter', 'microsoft',
                  'bing', 'youtube', 'sharepoint']:
            if h in host:
                return True
        return False

    def _whitelisted(host):
        if host in whitelisted:
            return True

        for h in WHITELIST:
            if host.endswith(h):
                return True
        return False

    def _update_sitecount(url):
        host = _urlhost(url)
        sitecount[host] += 1

    def _clean_hostlist(hlist):
        newlist = []
        for href in hlist:
            if not _too_many_requests(href):
                newlist.append(href)
        return newlist

    def _too_many_requests(url):
        host = _urlhost(url)
        maxreq = args.maxreq_whitelist
        if not _whitelisted(host):
            maxreq = args.maxreq
        return sitecount[host] > maxreq

    def _get_next():
        while len(hostlist):
            url = hostlist.pop(0)
            print(len(hostlist), url)

            _update_sitecount(url)

            if _too_many_requests(url):
                continue

            if _blacklisted(url):
                continue

            if url in already_done:
                continue

            return url


    MAX_RUNNING = 10
    proclist = []
    resultsqueue = mp.Queue()

    while hostlist or proclist:
        # clean up proclist
        running = []
        for p in proclist:
            if not p.is_alive():
                p.join()
            else:
                running.append(p)
        proclist = running

        # spawn new process(es) to make new requests, if possible
        while len(proclist) < MAX_RUNNING:
            url = _get_next()
            if url is None:
                break

            already_done.add(url)
            howmany += 1

            p = mp.Process(target=_make_req, args=(url, langpref, verbose, resultsqueue))
            proclist.append(p)
            p.start()
            print("Spawning new process {} running".format(len(proclist)))


        # get/process any results available
        while True:
            xresp = None
            try:
                xresp = resultsqueue.get(block=True, timeout=1)
            except queue.Empty:
                break

            if xresp is not None:
                _print_response(xresp, verbose)
                cont, hrec, langlinks, otherlinks, lldata = _do_analysis(xresp, verbose)
                if 'soup' in xresp:
                    del xresp['soup']
                if 'content' in xresp:
                    del xresp['content']

                if cont:
                    if langlinks:
                        # dynamically whitelist any hosts that have SEARCHLANG lang tags
                        whitelisted.add(_urlhost(url))

                    print("{}".format(json.dumps([url, hrec, xresp])), file=outfile, flush=True)
                    print("{}".format(json.dumps([url, lldata])), file=outfile, flush=True)
                    print("#{} {}".format(url, _extract_rec_data(hrec)), file=outfile, flush=True)
                    if verbose:
                        print("links:", langlinks)

                    # put SEARCHLANG links on front
                    for link in langlinks[:args.maxreq_whitelist]:
                        if not _too_many_requests(link) and _looks_like_text(link):
                            hostlist.insert(0, link)

                    # put other links on back
                    for link in otherlinks[:args.maxreq_whitelist]:
                        if not _blacklisted(link) and not _too_many_requests(link) and _looks_like_text(link):
                            hostlist.append(link)

                # explicitly orphan these structures
                del xresp
                del hrec

        if args.maxtotal != -1 and howmany >= args.maxtotal:
            break

        hostlist = _clean_hostlist(hostlist)
        _dopickle(args.picklefile, hostlist, sitecount, already_done, whitelisted)

    outfile.close()


def _dopickle(outfile, hostlist, sitecount, already_done, whitelisted):
    xhash = {'hostlist': hostlist,
             'sitecount': sitecount,
             'already_done': already_done,
             'whitelisted': whitelisted}
    with open(outfile, 'wb') as outbin:
        pickle.dump(xhash, outbin)


def _dounpickle(infile):
    xd = {'hostlist':[], 'sitecount':[], 'already_done': set(), 'whitelisted': set()}
    if not os.path.exists(infile):
        print("No previous state to load from {}.".format(infile), file=sys.stderr)
        return xd

    with open(infile, 'rb') as inbin:
        xd = pickle.load(inbin)
    return xd


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Gather info about a site\'s language tags')
    parser.add_argument('-H', '--host', dest='onehost', type=str,
                        default=None,
                        help='Specific host to test')
    parser.add_argument('-i', '--infile', dest='infile', type=str,
                        help='Input file to read with hostnames')
    parser.add_argument('-l', '--langpref', dest='langpref', type=str,
                       default='*', help='Accept-Language header value (default=*)')
    parser.add_argument('-w', '--maxreqwl', dest='maxreq_whitelist', type=int, default=500,
        help='Max number of requests to make to the same domain for whitelisted domains (default=100)')
    parser.add_argument('-m', '--maxreq', dest='maxreq', type=int, default=10,
        help='Max number of requests to make to the same domain for non-whitelisted domains (default=5)')
    parser.add_argument('-v', '--verbose', dest='verbose', action='count',
                        default=0, help='Turn on verbose output (default=0)')
    parser.add_argument('-M', dest='maxtotal', type=int, default=-1,
                        help='Max number of total requests to make (default=unlimited)')
    parser.add_argument('-o', dest='outfile', type=str, default='crawl_results.json',
                        help='Name of output file to create (default=crawl_results.json)')
    parser.add_argument('-p', dest='picklefile', type=str, default='crawl_state.bin',
                        help='Name of state file to load on startup')
    parser.add_argument('-s', dest='searchlang', type=str, default='cy',
                        help='Name of language tag to generally search for')
    parser.add_argument('-W', dest='whitelist', action='append', 
                        help='Name of domains to whitelist; can be specified multiple times')

    args = parser.parse_args()

    print("Making requests from {} using langpref '{}' ".format(
            args.infile, args.langpref))
    langpref = args.langpref

    SEARCHLANG = args.searchlang

    if args.whitelist and SEARCHLANG != 'cy':
        WHITELIST = args.whitelist

    print("Searching the language universe of {}".format(SEARCHLANG))
    print("Whitelisted TLDs: {}".format(WHITELIST))

    if args.infile is None and args.onehost is None and \
            not os.path.exists(args.picklefile):
        parser.print_usage()
        sys.exit()

    hostlist = []
    if args.onehost is not None:
        hostlist += [args.onehost]
    elif args.infile:
        hostlist += _read_input(args.infile)

    _manager(args, hostlist, langpref)
