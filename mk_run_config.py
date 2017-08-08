"""Create text file input to do Accept-Language web measurements."""
from collections import defaultdict
import re

import langcodes

# https://en.wikipedia.org/wiki/List_of_territorial_entities_where_English_is_an_official_language
ENGLISH_CC = [
    # Countries where English is a de facto official language
    'US', 'UK', 'NZ', 'AU',

    # Countries where English is a de jure official language
    'AG', 'BS', 'BB', 'BZ', 'BW', 'BI',
    'CM', 'CA', 'CK', 'DM',
    'FM', 'FJ', 'GM', 'GH', 'GD', 'GF',
    'IN', 'IE', 'JM', 'KE', 'KI', 'LS', 'LR',
    'MW', 'MT', 'MH', 'MU', 'NA', 'NR', 'NG', 'NU',
    'PK', 'PW', 'PG', 'PH', 'RW', 'KN', 'LC', 'VC',
    'WS', 'SC', 'SL', 'SG', 'SB', 'ZA', 'SS', 'SZ',
    'TZ', 'TO', 'TT', 'TV', 'UG', 'VU', 'ZM', 'ZW',


    # Non-sovereign entities where English is a de jure official language
    'AS', 'AI', 'BM', 'VG', 'VI', 'KY',
    'CW', 'FK', 'GI', 'GU', 'HK', 'IM',
    'JE', 'NF', 'MP', 'PN', 'PR', 'SX', 'TC',

    # Non-sovereign entities where English is a de facto official language
    'IO', 'GG', 'MS', 'SH',
]


def _mk_qlist(langlist):
    qspread = 1 / len(langlist)
    return ["{};q={:.1f}".format(lang, (len(langlist) - i) * qspread)
            for i, lang in enumerate(langlist)]


def _check_english(cc, fulllang):
    i = len(fulllang) - 1
    while i >= 0:
        # put en at the end, but only if it is in CC list
        tag = fulllang[i]
        if langcodes.get(tag).language == 'en':
            tag = fulllang.pop(i)
            if cc in ENGLISH_CC:
                fulllang.append(tag)
        i -= 1
    return fulllang


def _merge_lang_lists(cc, xlist1, xlist2):
    langlist1 = xlist1.split(',')
    langlist2 = xlist2.split(',')
    langlist1 = _check_english(cc.upper(), langlist1)
    langlist2 = _check_english(cc.upper(), langlist2)
    fulllang = []
    for lang in langlist1:
        if lang not in fulllang:
            fulllang.append(lang)
        subtag = langcodes.get(lang).language
        if subtag != lang and subtag not in fulllang:
            fulllang.append(subtag)
    for lang in langlist2:
        if lang not in fulllang:
            fulllang.append(lang)
    specific = [tag for tag in fulllang
                if '-' in tag and not tag.startswith('en')]
    general = [tag for tag in fulllang
               if '-' not in tag or tag.startswith('en')]
    fulllang = specific + general
    return fulllang


def _parse_expresslist():
    ccregex = re.compile(r'\((?P<cc>[A-Z]{2})\)')
    ccdict = defaultdict(list)
    cc = None
    with open('expresslist.txt') as infile:
        infile.readline()  # discard header
        for line in infile:
            if line.startswith('smart') or line.startswith('---'):
                continue
            locname = line.split()[0]
            mobj = ccregex.search(line)
            if mobj:
                cc = mobj.group('cc')
            ccdict[cc].append(locname)
    return ccdict


def _load_countryinfo():
    ccdict = {}
    with open('countryinfo.txt') as infile:
        for line in infile:
            cc, tld, cname, langs = line.strip().split('::')
            ccdict[cc] = [tld, cname, langs]
    return ccdict


def _load_tldmap():
    tldmap = {}
    with open('tldmap.txt') as infile:
        for line in infile:
            if line.startswith('#'):
                continue
            line = line.strip()
            idx = line.find('#')
            if idx > -1:
                line = line[:(idx-1)]
            fields = line.split()
            tld = ".{}".format(fields[0])
            tldmap[tld] = ','.join(fields[1:])
    return tldmap


if __name__ == '__main__':
    vpns = _parse_expresslist()
    ccdict = _load_countryinfo()
    tldmap = _load_tldmap()
    with open('runconfig.txt', 'w') as outfile:
        for cc, vpnlist in sorted(vpns.items()):
            ccdata = ccdict[cc]
            tld, cname, langlist1 = ccdata
            langlist2 = tldmap[tld]
            vpn1 = vpnlist.pop()
            fulllang = _merge_lang_lists(cc, langlist1, langlist2)
            qlist = _mk_qlist(fulllang)
            print("::".join([vpn1, cc, tld, ', '.join(qlist)]), file=outfile)
            if vpnlist:
                print("# {}".format(','.join(vpnlist)), file=outfile)
