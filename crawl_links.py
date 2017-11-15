"""
Collect site names from starting at a particular site by crawling.  Don't
crawl more than X levels deep from each host
"""
import sys
import argparse
import time
import json
import urllib.parse as up
from bs4 import BeautifulSoup as bs
import lzma
import base64
from collections import Counter
import pdb
import random
import langtags

import requests
requests.packages.urllib3.disable_warnings()  # disable ssl warnings

MAX_RETRIES = 3
PARSER = "lxml"

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


def _make_req(hostname, langpref, verbose):
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

    results['soup'] = bs(response.text, PARSER)
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


def _read_input(infile):
    hostlist = []
    with lzma.open(infile, 'rt') as inp:
        for line in inp:
            rank, name = line.strip().split(',')
            hostlist.append(name)
    return hostlist


def _print_response(rdict, verbose):
    print("{} -> ".format(rdict['reqhost']), end='')
    if rdict['success']:
        print("{}/{}".format(
            rdict['status_code'], rdict['status_reason']))
    else:
        print("{}".format(','.join(rdict['errors'])))


def _check_langset(lset, lt='cy'):
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
        if 'cy' in xd['inferred'] or 'cy' in xd['primary']:
            return True

        for lang,xli in xd['content_detect']['languages'].items():
            if lang == 'cy' and xli[1] >= 50:
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
        return False, None, [], []
    elif not _offers_welsh(xd):
        return False, None, [], []


    cy_links = set()
    other_links = set()

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
                addset = cy_links

            if 'href' in el.attrs:
                href = el.attrs['href']
                components = up.urlsplit(href)
                if components.netloc:
                    addset.add(components.netloc)
                elif href.startswith('/'):
                    comp = up.urlsplit(rec['url'])
                    assert(comp.netloc)
                    addset.add(comp.netloc + href)

    return True, xd, list(cy_links), list(other_links)


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
    return '::'.join([exp,p,inf,det])


def _manager(args, hostlist, langpref):
    verbose = args.verbose

    sitecount = Counter()
    already_done = set()
    outfile = open(args.outfile, 'w')
    whitelisted = set()


    def _blacklisted(url):
        host = _urlhost(url)
        for h in ['facebook','google','twitter','microsoft','bing','youtube','sharepoint']:
            if h in host:
                return True
        return False


    def _whitelisted(host):
        if host in whitelisted:
            return True

        for h in ['.uk', '.cymru', '.wales']:
            if host.endswith(h):
                return True
        return False


    def _too_many_requests(url):
        host = _urlhost(url)
        maxreq = args.maxreq_whitelist
        if not _whitelisted(host):
            maxreq = args.maxreq
        return sitecount[host] > maxreq


    def _update_sitecount(url):
        host = _urlhost(url)
        sitecount[host] += 1


    while hostlist:
        url = hostlist.pop(0)
        print(len(hostlist), url)

        _update_sitecount(url)

        if _too_many_requests(url):
            continue

        if _blacklisted(url):
            continue

        if url in already_done:
            continue

        already_done.add(url)
        xresp = _make_req(url, langpref, verbose)
        _print_response(xresp, verbose)
        cont, hrec, cylinks, otherlinks = _do_analysis(xresp, verbose)
        if cont:
            if cylinks:
                # dynamically whitelist any hosts that have 'cy' lang tags
                whitelisted.add(_urlhost(url))

            print("{}".format(json.dumps([url, hrec])), file=outfile, flush=True)
            print("#{} {}".format(url, _extract_rec_data(hrec)), file=outfile, flush=True)
            if verbose:
                print("links:", links)
            for link in otherlinks:
                if not _blacklisted(link) and not _too_many_requests(link):
                    hostlist.append(link)
            random.shuffle(hostlist)

            hostlist.extend(cylinks) # always add all cylinks
            hostlist.reverse() # put any cylinks at the front


        if args.maxtotal != -1 and len(already_done) >= args.maxtotal:
            break

    outfile.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Gather info about a site\'s language tags')
    parser.add_argument('-H', '--host', dest='onehost', type=str,
                        default=None,
                        help='Specific host to test')
    parser.add_argument('-i', '--infile', dest='infile', type=str,
                        help='Input file to read with hostnames')
    parser.add_argument('-l', '--langpref', dest='langpref', type=str,
                        default='*', help='Accept-Language header value')
    parser.add_argument('-w', '--maxreqwl', dest='maxreq_whitelist', type=int, default=100,
        help='Max number of requests to make to the same domain for whitelisted domains')
    parser.add_argument('-m', '--maxreq', dest='maxreq', type=int, default=5,
        help='Max number of requests to make to the same domain for non-whitelisted domains')
    parser.add_argument('-v', '--verbose', dest='verbose', action='count',
                        default=0, help='Turn on verbose output.')
    parser.add_argument('-M', dest='maxtotal', type=int, default=-1,
                        help='Max number of total requests to make.')
    parser.add_argument('-o', dest='outfile', type=str, default='crawl_results.json',
                        help='Name of output file to create.')
    args = parser.parse_args()

    print("Making requests from {} using langpref '{}' ".format(
            args.infile, args.langpref))
    langpref = args.langpref

    if args.infile is None and args.onehost is None:
        parser.print_usage()
        sys.exit()
    if args.onehost is not None:
        hostlist = [args.onehost]
    else:
        hostlist = _read_input(args.infile)
    _manager(args, hostlist, langpref)
