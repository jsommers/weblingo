import sys
from json import loads
from lzma import decompress
from base64 import b64decode
from collections import Counter
from itertools import starmap

from bs4 import BeautifulSoup as bs
import bs4
import langtags
# from langdetect import detect, detect_langs


def _decompress(s):
    if not s:
        return ''
    return decompress(b64decode(s.encode('utf8'))).decode('utf8')


def _get_content(rec):
    content = _decompress(rec.get('content', ''))
    try:
        soup = bs(content, "html.parser")
    except:
        soup = bs("", "html.parser")
    return soup


def _infer_lang(headers, soupy):
    def filterfn(el):
        return el.__class__ is bs4.NavigableString \
            and '{' not in el.string and '}' not in el.string \
            and '<' not in el.string and '>' not in el.string \
            and '==' not in el.string and '()' not in el.string
    # only choose NavigableStrings, not any subclasses, i.e.,
    # CData, Coment, ProcessingInstruction, Doctype, Declaration.
    # + some heuristics that eliminate text that might yet be code.

    if soupy.body is None:
        return []

    els = [el for el in filter(filterfn, soupy.body.descendants)]
    plaintext = ' '.join([el.string for el in els])
    try:
        llist = detect_langs(plaintext)
        return llist
    except:
        return []
    

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

    return html_attr, meta_attr, http_attr


def _best_lang_tag(tup):
    for i in range(3):
        if tup[i]:
            return tup[i]
    return ''


def _extract_lang(langtag):
    if langtag == '':
        return ''
    try:
        tag = langtags.Tag(langtag, True)
        if tag.language is not None:
            return tag.language.subtag
        return 'ERR'
    except:
        return 'ERR'


def _invalid_langtag(langtag):
    if langtag == '':
        return False
    try:
        langtags.Tag(langtag, True)
        return False
    except:
        return True


def _compare_infer_extract(bestlang, inferred_langs):
    inflang = ''
    if inferred_langs:
        l = inferred_langs[0]
        if l.prob >= .9:
            inflang = l.lang.split('-')[0]
    return (bestlang, inflang)


def analyze_nospark(files_in, file_out, **kwargs):
    xall = Counter()
    xerr = Counter()
    iall = Counter()
    xfullall = Counter()

    for inf in files_in:
        with open(inf) as infile:
            adict = Counter()
            ldict = Counter()
            errdict = Counter()
            idict = Counter()
            for recgroup in map(loads, iter(infile)):
                success_recs = filter(lambda rec: rec['success'], recgroup)
                headers_content = map(
                    lambda rec: (rec['response_headers'], _get_content(rec)),
                    success_recs)
                primarylangs = list(starmap(_primary_language, headers_content))
                bestlangs = map(_best_lang_tag, primarylangs)
                adict.update(bestlangs)

                bestlangs = list(map(_extract_lang,
                                map(_best_lang_tag, primarylangs)))
                ldict.update(bestlangs)

                success_recs = filter(lambda rec: rec['success'], recgroup)
                headers_content = map(
                    lambda rec: (rec['response_headers'], _get_content(rec)),
                    success_recs)
                inferred_langs = starmap(_infer_lang, headers_content)
                infer_results = starmap(_compare_infer_extract, zip(bestlangs, inferred_langs))
                only_disagree = filter(lambda tup: tup[0] != tup[1], infer_results)
                idict.update(only_disagree)

                errtags = filter(_invalid_langtag,
                                 map(_best_lang_tag, primarylangs))
                errdict.update(errtags)

            xfullall.update(adict)
            xall.update(ldict)
            iall.update(idict)
            xerr.update(errdict)

    print(xfullall)
    print(xall)
    print(iall)
    print(xerr)


def analyze(sc, files_in, file_out, **kwargs):
    langdict = {}
    litems = []

    for inf in files_in:
        wfile = sc.textFile(inf)
        ldict = wfile.flatMap(
                    loads).filter(
                    lambda rec: rec['success']).map(
                    _primary_language).countByKey()
        langdict[inf] = ldict
        litems.extend(list(ldict.items()))

    lrdd = sc.parallelize(litems)
    lsummary = lrdd.countByKey()

    with open(file_out, "w") as outfile:
        print(list(lsummary.items()), file=outfile)
        for inf, ldict in langdict.items():
            print("{}: {}".format(inf, list(ldict.items())), file=outfile)

