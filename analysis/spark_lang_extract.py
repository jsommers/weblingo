import sys
from json import loads, dumps
from lzma import decompress
from base64 import b64decode
from collections import defaultdict, Counter
import urllib.parse as uparse
import re
import os

from bs4 import BeautifulSoup as bs
import bs4
import langtags
import pycld2 as cld2
import langcodes

# PARSER = "html.parser"
# PARSER = "html5lib"
PARSER = "lxml"

def _decompress(s):
    if not s:
        return ''
    return decompress(b64decode(s.encode('utf8'))).decode('utf8')


def _get_content(rec):
    content = _decompress(rec.get('content', ''))
    try:
        soup = bs(content, PARSER)
    except:
        soup = bs("", PARSER)
    return soup


def _in_iframe(el):
    if el is None:
        return False
    if el.name is None:
        return False
    if el.name.lower() == 'iframe':
        return True
    return _in_iframe(el.parent)


def _check_explicit_lang_tag(el):
    lfound = set()

    if hasattr(el, 'attrs'):
        if 'lang' in el.attrs:
            ltag = el.attrs['lang'].strip()
            lfound.add(ltag)
        if 'xml:lang' in el.attrs:
            ltag = el.attrs['xml:lang'].strip()
            lfound.add(ltag)
        if 'hreflang' in el.attrs:
            ltag = el.attrs['hreflang'].strip()
            lfound.add(ltag)
    if el.name == 'meta':
        xdict = {key.lower(): val for key, val in el.attrs.items()}
        if xdict.get('http-equiv', '') == 'content-language':
            meta_attr = xdict.get('content', '').strip()
            lfound.add(meta_attr)
        if xdict.get('http-equiv', '') in ['og:locale', 'locale']:
            meta_attr = xdict.get('content', '').strip()
            lfound.add(meta_attr)
    return lfound


def _infer_lang(headers, soupy):
    def filterfn(el):
        return el.__class__ is bs4.NavigableString \
            and '{' not in el.string and '}' not in el.string \
            and '<' not in el.string and '>' not in el.string \
            and '==' not in el.string and '()' not in el.string
    # only choose NavigableStrings, not any subclasses, i.e.,
    # CData, Coment, ProcessingInstruction, Doctype, Declaration.
    # + some heuristics that eliminate text that might yet be code.

    #if soupy.body is None:
    #    return []
    #els = [el for el in filter(filterfn, soupy.body.descendants)]
    #plaintext = ' '.join([el.string for el in els])
    #lpt = len(plaintext)
    #try:
    #    llist = detect_langs(plaintext)
    #except:
    #    llist = []
    #xli = []
    #for l in llist:
    #    xli.append(langcodes.get(l.lang).describe('en'))
    xd = {}
    try:
        isreliable, bytesfound, details = cld2.detect(str(soupy).encode('utf8'))
        for tup in details:
            xd[tup[1]] = (tup[0], tup[2])
    except:
        isreliable = False
        bytesfound = 0

        # , hintTopLevelDomain=X, hintLanguageCode=X)
    # details is a tuple of up to three detected languages, where each is
    #       tuple is (languageName, languageCode, percent, score).  percent is
    #       what percentage of the original text was detected as this language
    #       and score is the confidence score for that language.

    return {'lenplaintext': bytesfound, 'languages': xd, 'reliable': isreliable}


def _primary_language(headers, soupy):
    """Extract the primary language from the response content."""
    http_attr = ''
    meta_attr = ''
    html_attr = ''

    # standard case-ify
    for header, value in headers.items():
        if header.lower() == 'content-language':
            http_attr = value
            break

    if soupy.html is not None:
        html = soupy.html
        if html.attrs is not None:
            if 'lang' in soupy.html.attrs:
                html_attr = soupy.html.attrs['lang']
            elif 'xml:lang' in soupy.html.attrs:
                html_attr = soupy.html.attrs['xml:lang']

            # standard case-ify
        for meta in soupy.find_all('meta'):
            xdict = {key.lower(): val for key, val in meta.attrs.items()}
            if xdict.get('http-equiv', '') == 'content-language':
                meta_attr = xdict.get('content', '')
                break

    return {'htmlattr': html_attr,
            'metaattr': meta_attr,
            'httpattr': http_attr}


def _parsetag(langtag):
    langtag = langtag.strip()
    try:
        parsed_tag = langtags.Tag(langtag)
    except Exception as e:
        parsed_tag = str(e)
    try:
        lt = langcodes.Language.get(langtag).describe('en')
    except:
        lt = ''
    return langtag, str(parsed_tag), lt


def _infer_language_from_link_text(s, wordlimit=True):
    if not isinstance(s, str):
        return None

    s = s.strip().lower()

    if not s:
        return None

    # if > 2 words, assume that this isn't a language link
    if wordlimit and len(s.split()) > 2:
        return None

    # first, try the text as a language tag
    try:
        lt = langtags.Tag(s)
    except:
        pass
    else:
        return str(lt)

    # next, try the text as the name of a language
    try:
        lc = langcodes.find(s)
        # if the above doesn't fail, the text may still give a false indication.
        # Examples: news headline text starting "Russian ..."
        #           Music -> gets inferred as 'musi' lang
        # heuristics: either we have an exact match between the link text
        # and a language name (using langcodes.code_to_names), or the
        # inferred language
        name_match = any(map(lambda lname: s == lname.lower(),
            langcodes.code_to_names('language', lc.language).values()))
        # print(lc, name_match, s)
        if name_match or len(lc.language) == 2:
            return str(lc)
    except:
        pass

    return None


def _same_site(linkloc, host):
    def strip_www(name):
        mobj = re.match('^www\.(.*)$', name)
        if mobj:
            return mobj.groups()[0]
        return name

    host = strip_www(host).lower()
    linkloc = strip_www(linkloc).lower()
    if host == linkloc:
        return True

    if len(linkloc) == 0:
        return True

    assert(len(host) > 0)

    # bbc.com, bbc.co.uk
    if host[0] == linkloc[0]:
        return True

    if host in linkloc:
        # ikea.com.cy, ikea.com
        return True

    if linkloc.endswith(host):
        # cy.ikea.com, ikea.com
        return True

    return False


def _analyze_link(alink, host):
    # check that link goes to same provider
    try:
        components = uparse.urlsplit(alink.attrs.get('href', ''))
    except:
        components = uparse.urlsplit('')

    if components.netloc != '' and not _same_site(components.netloc, host):
        #     print("Current host doesn't match link; ignoring {}/{}".format(host, components.netloc))
        return set()

    # if verbose:
    #     print("LINKINFER: ", end='')

    # special cases: attributes data-countrycode, data-languagecode, data-language
    indicator0 = set()
    if hasattr(alink, "attrs"):
        if 'data-languagecode' in alink.attrs:
            try:
                t = langtags.Tag(alink.attrs['data-languagecode'])
                indicator0.add(str(t))
            except:
                pass
        if 'data-language' in alink.attrs:
            try:
                t = langtags.Tag(alink.attrs['data-language'])
                indicator0.add(str(t))
            except:
                pass
        if 'data-languagecode' in alink.attrs and 'data-countrycode' in alink.attrs:
            try:
                t = langtags.Tag("{}-{}".format(alink.attrs['data-languagecode'],
                                                alink.attrs['data-countrycode']))
                indicator0.add(str(t))
            except:
                pass
        if 'data-mkt' in alink.attrs:
            mobj = re.match('^([a-z]{2}-[a-z]{2})$', alink.attrs['data-mkt'], re.I)
            if mobj:
                try:
                    t = langtags.Tag(mobj.group(1))
                    indicator0.add(str(t))
                except:
                    pass

    # lc-CC as first path component
    if len(components.path) > 0:
        mobj = re.match('^([a-z]{2}-[a-z]{2})$', components.path[0], re.I)
        if mobj:
            try:
                t = langtags.Tag(mobj.group(1))
                indicator0.add(str(t))
            except:
                pass

    # lc-CC as first component in domain
    if len(components.netloc) > 0:
        mobj = re.match('^([a-z]{2}-[a-z]{2})$', components.netloc.split('.')[0], re.I)
        if mobj:
            try:
                t = langtags.Tag(mobj.group(1))
                indicator0.add(str(t))
            except:
                pass

    if indicator0:
        # if verbose:
        #     print(indicator0)
        return indicator0


    # indicator 1: language name or code in link text
    indicator1 = defaultdict(set)
    lc = _infer_language_from_link_text(alink.string)
    if lc is not None:
        indicator1[lc].add(alink.string)
    for el in alink.descendants:
        lc = _infer_language_from_link_text(el.string)
        if lc is not None:
            indicator1[lc].add(el.string)

    # indicator 2: "lang" within link
    indicator2 = re.search("lang", re.sub(r"\W", r" ", str(alink)), re.I) \
        is not None

    # indicator3: href analysis
    try:
        components = uparse.urlsplit(alink.attrs.get('href', ''))
    except:
        components = uparse.urlsplit('')

    pathsrc = ''
    pathcode = None
    netlocsrc = ''
    netloccode = None
    if len(components.path) > 0:
        pathlist = components.path.strip('/').split('/')

        # /countrycode/langcode/rest of path
        if len(pathlist) > 1:
            try:
                pathsrc ="{}-{}".format(pathlist[1], pathlist[0])
                pathcode = langtags.Tag(pathsrc)
            except:
                pass

        # /intl/langcode/rest of path
        if pathcode is None and len(pathlist) > 1 and pathlist[0].lower() == 'intl':
            try:
                pathsrc = pathlist[1]
                pathcode = langtags.Tag(pathsrc)
            except:
                pass

        # /languagecode/rest of path
        if pathcode is None and len(pathlist) > 0:
            try:
                pathsrc = pathlist[0]
                pathcode = langtags.Tag(pathsrc)
            except:
                pass

        # 'lang' or 'language' starting
        # in one of the class names for the a tag or a nested tag
        if pathcode is None:
            try:
                pathsrc = pathlist[0]
                pathcode = langcodes.find(pathsrc)
            except:
                pass

    if len(components.netloc) > 0:
        dlist = components.netloc.split('.')
        try:
            netlocsrc = dlist[0]
            netloccode = langtags.Tag(netlocsrc)
        except:
            pass

    querysrc = ''
    querycode = None
    if components.query:
        qd = uparse.parse_qs(components.query)
        for key, vallist in qd.items():
            if re.match("^lang", key, re.I) and len(vallist) == 1:
                try:
                    querysrc = vallist[0]
                    querycode = langtags.Tag(querysrc)
                except:
                    pass
            else:
                # if a query value is an exact match for language tag
                for v in vallist:
                    try:
                        querycode = langtags.Tag(v)
                    except:
                        pass

    indicator3 = defaultdict(set)
    if pathcode:
        indicator3[str(pathcode)].add(pathsrc)
    if netloccode:
        indicator3[str(netloccode)].add(netlocsrc)
    if querycode:
        indicator3[str(querycode)].add(querysrc)


    # indicator 4: text within attribute values (not attr keys)
    i4 = defaultdict(set)
    if hasattr(alink, "attrs") and isinstance(alink.attrs, dict):
        for k, v in alink.attrs.items():
            if k.lower() == 'href':
                continue
            # print("KV:", k, v)
            if isinstance(v, list):
                v = ' '.join(v)
            lc = _infer_language_from_link_text(v, wordlimit=False)
            if lc is not None:
                i4[str(lc)].add(v)

            for el in alink.descendants:
                if hasattr(el, "attrs") and isinstance(el.attrs, dict):
                    for k, v in el.attrs.items():
                        if k.lower() == 'href':
                            continue
                        if isinstance(v, list):
                            v = ' '.join(v)
                        # print("KV:", k, v)
                        lc = _infer_language_from_link_text(v)
                        if lc is not None:
                            i4[str(lc)].add(v)

    indicator4 = i4

    c = Counter()
    xmap = defaultdict(set)

    for d in [indicator1, indicator3, indicator4]:
        xkeys = [k for k in d.keys() if len(k) == 2]
        c.update(xkeys)
        for k in xkeys:
            xset = d[k]
            xmap[k].update([s.lower() for s in xset])
    result = set()
    for k, count in c.items():
        if count > 1 and len(xmap[k]) > 1:
            result.add(k)

    return result


def _infer_langselector(soup):
    ''' :-) '''
    return set()


def _rec_analyze(rec):
    if not rec['success']:
        # print("Error record")
        return
    rec['content'] = _get_content(rec)
    # print(rec['reqhost'])
    target_host = rec['reqhost']
    xd = {
        'reqhost': rec['reqhost'],
        'url': rec['url'],
        'history': rec['history'],
        'start': rec['start'],
        'end': rec['end'],
        'primary': [],
        'explicit': [],
        'inferred': [],
        'content_detect': []
    }

    # primary language tag(s)
    primary_langs = set()
    for atype, langtag in _primary_language(
            rec['response_headers'], rec['content']).items():
        langtag, parsed_tag, lname = _parsetag(langtag)
        primary_langs.add(parsed_tag)
    primary_langs.discard('')
    xd['primary'] = list(primary_langs)

    inferred_set = set()
    explicit_set = set()

    for el in rec['content'].descendants:
        if el.name is None or el.name == 'script':
            continue
        xset = _check_explicit_lang_tag(el)
        if xset:
            explicit_set |= xset
        elif el.name == 'a' or el.name == 'link' or el.name == 'option':
            try:
                xset = _analyze_link(el, target_host)
            except Exception as e:
                # print("Exception: {}".format(e))
                xset = set()
            inferred_set |= xset

    other_inferred = _infer_langselector(rec['content'])
    inferred_set |= other_inferred

    xd['explicit'] = list(explicit_set)
    xd['inferred'] = list(inferred_set)

    xd['content_detect'] = _infer_lang(rec['response_headers'], rec['content'])
    return xd


def analyze(sc, files_in, file_out, **kwargs):
    """
    ignore file_out; base output file name on input name.
    """
    outfile = open(file_out, 'w')
    stride = int(kwargs.get('stride', 100))
    
    for inf in files_in:
        wfile = sc.textFile(inf)
        records = wfile.flatMap(loads)\
            .filter(lambda rec: rec['success'])\
            .map(_rec_analyze).collect()

        for i in range(0, len(records), stride):
            print(dumps(records[i:(i+stride)]), file=outfile)

    outfile.close()
