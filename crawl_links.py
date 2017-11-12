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


def _do_analysis(rec, verbose):
    from analysis import spark_lang_extract as analyze
    if verbose:
        print("Results for {} ({})".format(rec['reqhost'], rec['url']))
        print("\tAccept-Language: {}".format(rec['request_headers'].get('accept-language', 'No header')))
    xd = analyze._rec_analyze(rec)
    if verbose:
        print(xd)

    if not ('cy' in xd['inferred'] or 'cy' in xd['primary'] or 'cy' in xd['content_detect']['languages'].keys()):
        return False, None, []

    links = set()
    for link in rec['soup'].find_all('a'):
        if 'href' in link.attrs:
            href = link.attrs['href']
            components = up.urlsplit(href)
            if components.netloc:
                links.add(components.netloc)
            elif href.startswith('/'):
                comp = up.urlsplit(rec['url'])
                assert(comp.netloc)
                links.add(comp.netloc + href)
    return True, xd, list(links)


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


    def _blacklisted(host):
        for h in ['facebook','google','twitter','microsoft','bing','youtube']:
            if h in host:
                return True
        return False


    while hostlist:
        url = hostlist.pop(0)
        print(len(hostlist), url)

        host = _urlhost(url)
        sitecount[host] += 1
        if sitecount[host] > args.maxreq:
            continue

        if _blacklisted(host):
            continue

        if url in already_done:
            continue

        already_done.add(url)
        xresp = _make_req(url, langpref, verbose)
        _print_response(xresp, verbose)
        cont, hrec, links = _do_analysis(xresp, verbose)
        if cont:
            print("{}".format(json.dumps([url, hrec])), file=outfile, flush=True)
            print("#{} {}".format(url, _extract_rec_data(hrec)), file=outfile, flush=True)
            if verbose:
                print("links:", links)
            hostlist.extend(links)

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
    parser.add_argument('-m', '--maxreq', dest='maxreq', type=int, default=50,
                        help='Max number of requests to make to the same domain')
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
        if args.start > 0:
            hostlist = hostlist[args.start:]
    _manager(args, hostlist, langpref)
