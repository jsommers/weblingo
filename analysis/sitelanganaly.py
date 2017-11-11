import sys
import math
from json import loads, dumps
from collections import Counter, defaultdict
import itertools
import functools
import glob
import os
import re
from copy import copy
from urllib.parse import urlsplit

import langtags


def _load_langprefs():
    lprefs = {}
    with open("../countryinfo.txt") as infile:
        for line in infile:
            cc, tld, cname, langs = line.split('::')
            lprefs[cc] = [ s.split('-')[0] for s in langs.strip().split(',') ]
    return lprefs


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


def _getexc(rec):
    return set([_ltag(s) for s in rec['explicit']])


def _getinf(rec):
    return set([_ltag(s) for s in rec['inferred']])


def _getprimary(rec):
    return set([_ltag(s) for s in rec['primary']])


def _getcontent_detect(rec):
    langinf = rec['content_detect']
    xset = set()
    if langinf['reliable']:
        for xlc, lli in langinf['languages'].items():
            if lli[1] >= 10 and xlc != 'un':
                lc = _ltag(xlc)
                if lc:
                    xset.add(lc)
    else:
        xset.add('unknown')
    return xset


class SiteData(object):
    LPREF = _load_langprefs()

    def __init__(self, site):
        self._langs = {}
        self._prim = {}
        self._site = site
        self._hosts = {}
        self._fullset = set()
        self._fullset_def = set()
        self._fullset_pref = set()
        self._contdet_def = {}
        self._contdet_pref = {}
        self._different_langs_by_cc = False
        self._vary = {}

    def fullset(self, xtype=''):
        if not xtype:
            return self._fullset
        elif xtype == 'langpref':
            return self._fullset_pref
        elif xtype == 'default':
            return self._fullset_def
        else:
            raise RuntimeError("Invalid data type")

    def add_vary_rec(self, cc, xtype, varylang):
        self._vary[(cc,xtype)] = varylang

    def get_vary(self):
        return self._vary

    def get_prim(self):
        return self._prim

    def get_langdetect(self, xtype):
        if xtype == 'default':
            return self._contdet_def
        elif xtype == 'langpref':
            return self._contdet_pref
        else:
            raise RuntimeError("No no no")

    def add_lang_rec(self, cc, xtype, prim, langs, contdet, url):
        xlen = len(self._fullset)
        self._fullset |= set(langs)
        if xlen > 0 and len(self._fullset) != xlen:
            self._different_langs_by_cc = True

        if xtype == 'default':
            self._fullset_def |= set(langs)
            self._contdet_def[(cc,xtype)] = contdet
        elif xtype == 'langpref':
            self._fullset_pref |= set(langs)
            self._contdet_pref[(cc,xtype)] = contdet

        self._prim[(cc,xtype)] = set(prim)
        self._langs[(cc,xtype)] = set(langs)

        up = urlsplit(url)
        host = up.netloc
        if up.path:
            xpath = up.path
            if xpath.startswith('/'):
                xpath = xpath[1:] 
            isltag = _ltag(xpath.split('/')[0]) != 'INVALID'
            if isltag:
                host += up.path
        self._hosts[(cc,xtype)] = host

    def cmp_all_langs(self):
        diffs = {}
        same = True
        diff_of_one = []
        for key, xset in self._langs.items():
            d = self._fullset - xset
            diffs[key] = d
            diff_of_one.append(((len(self._fullset) - len(d)) == 1) and len(self._fullset) > 2)
            if len(d) and len(self._fullset) - len(d) > 1:
                same = False

        return (same, all(diff_of_one), self._fullset, self._fullset_def, self._fullset_pref, diffs)

    def cmp_url_vary(self):
        hset = set()
        onlyhost = set()
        for key, pset in self._prim.items():
            h = self._hosts[key]
            hset.add( (h, frozenset(pset)) )
            onlyhost.add(h)
        return hset, len(onlyhost)

    def cmp_primary(self, xtype):
        if not self.is_multilingual_site():
            raise RuntimeError("Only call this method for multilingual sites")
        primset = set()
        for key, p in self._prim.items():
            if key[1] == xtype:
                primset |= p
        if xtype == 'default':
            xfullset = self._fullset_def
        else:
            xfullset = self._fullset_pref
        return primset, len(xfullset)

    def is_multilingual_site(self):
        return len(self._fullset) > 1
        #contdet = False
        #for xtup, xset in self._contdet_pref.items():
        #    if len(xset) > 1:
        #        contdet = True
        #return tags and contdet

    def has_data(self, xtype):
        return any([ktup[1]==xtype for ktup in self._prim])

    def provides_preferred_language(self, cc):
        def _prim_intersect_preferred(prim, preflang_in_fullset):
            for p in prim:
                if p in preflang_in_fullset:
                    return True
            return False

        provides_it = False
        really_provides_it = False
        offers_preferred = False
        key = (cc, 'langpref')
        primset = self._prim.get(key, set())
        
        provides_preferred = []
        for preflang in SiteData.LPREF[cc]:
            if preflang in self._fullset_pref:
                provides_preferred.append(preflang)

        if provides_preferred:
            offers_preferred = True
            if _prim_intersect_preferred(primset, provides_preferred):
                provides_it = True
            contdet = self._contdet_pref.get(key, set())
            if _prim_intersect_preferred(contdet, provides_preferred):
                really_provides_it = True
            
        # if p intersects with provides_preferred, yes
        # do we want to check order in provides_preferred??? (yes)
        return offers_preferred, len(provides_preferred), provides_it, really_provides_it


class SiteMap(object):
    def __init__(self):
        self._xmap = {}
        self._ccs = set()

    def update(self, site, url, cc, xtype, prim, langs, contdet):
        self._ccs.add(cc)
        if site not in self._xmap:
            self._xmap[site] = SiteData(site)
        sdata = self._xmap[site]
        sdata.add_lang_rec(cc, xtype, prim, langs, contdet, url)

    def update_vary(self, site, cc, xtype, varylang):
        sdata = self._xmap[site]
        sdata.add_vary_rec(cc, xtype, varylang)

    def _vary_analysis(self, outname):
        outfile = open(outname, 'w')
        vtypes = Counter()
        n = 0
        for site, sd in self._xmap.items():
            varydata = sd.get_vary()
            for xtup, vlang in varydata.items():
                # vtypes[xtup[1]][vlang] += 1
                if vlang:
                    vtypes[xtup] += 1
            n += 1

        print(f"# number of sites out of {n} that give Vary: Accept-Language response", file=outfile)
        _print_counter(vtypes, outfile)

    def _content_detection_analysis(self, outname):
        outfile = open(outname, "w")
        ldetccdef = defaultdict(Counter)
        ldetccpref = defaultdict(Counter)
        ldetdef = {}
        ldetpref = {}
        for site, sd in self._xmap.items():
            defdet = sd.get_langdetect('default') # set per cc,xtype pair
            for xtup, xset in defdet.items():
                if sd.is_multilingual_site():
                    ldetccdef[xtup[0]].update(xset)
                if xtup in ldetdef:
                    ct = ldetdef[xtup]
                else:
                    ct = Counter()
                    ldetdef[xtup] = ct
                ct.update(xset)
            prefdet = sd.get_langdetect('langpref')
            for xtup, xset in prefdet.items():
                if sd.is_multilingual_site():
                    ldetccpref[xtup[0]].update(xset)
                if xtup in ldetpref:
                    ct = ldetpref[xtup]
                else:
                    ct = Counter()
                    ldetpref[xtup] = ct
                ct.update(xset)
            
        print("# observed lang freq for multilingual sites only, content detection, default", file=outfile)
        _print_counter(ldetccdef, outfile)
        print("# observed lang freq for multilingual sites only, content detection, langpref", file=outfile)
        _print_counter(ldetccpref, outfile)
        for xhash in [ldetdef, ldetpref]:
            for xtup, xset in sorted(xhash.items()):
                print(f"# {xtup[0]} {xtup[1]}", file=outfile)
                _print_counter(xset, outfile)

    def analyze_site_differences(self, suffix):
        self._all_lang_compare(f"alllang{suffix}.txt")
        self._dnssite_compare(f"dnssite{suffix}.txt")
        self._primlang_compare(f"primlang{suffix}.txt")
        self._prim_lang_matches_preference(f"primlangmatch{suffix}.txt")
        self._vary_analysis(f"varycheck{suffix}.txt")
        self._content_detection_analysis(f"contentdetect{suffix}.txt")

    def _all_lang_compare(self, fname):
        outfile = open(fname, 'w')
        outfile_sites = open(f"sitesall_{fname}", 'w')

        n = 0
        nsame = 0
        nbadtags = 0
        alllang_counts = Counter()
        alllang_counts_def = Counter()
        alllang_counts_pref = Counter()
        langs_offered_freq = Counter()
        langs_offered_freq_default = Counter()
        langs_offered_freq_langpref = Counter()

        alllang_counts_default_multilingual = Counter()
        alllang_counts_default_unilingual = Counter()
        langs_offered_freq_default_multilingual = Counter()
        langs_offered_freq_default_unilingual = Counter()
        alllang_counts_langpref_multilingual = Counter()
        alllang_counts_langpref_unilingual = Counter()
        langs_offered_freq_langpref_multilingual = Counter()
        langs_offered_freq_langpref_unilingual = Counter()

        for site, sd in self._xmap.items():
            n += 1
            same, badtags, fset, fsetdef, fsetpref, diffs = sd.cmp_all_langs()
            nsame += int(same)
            nbadtags += int(badtags)
            alllang_counts[len(fset)] += 1

            # FIXME: also dump out primary langtags + content detected
            # fmt: site: {CC: {primary:X, detect:Y}, ...}
            print("{}-all: {}".format(site, ','.join(sorted(fset))), file=outfile_sites)

            xprim = defaultdict(set)
            xdet = defaultdict(set)
            for xtup, xset in sd.get_prim().items():
                xprim[xtup[0]].update(xset)
            for cc, xset in xprim.items():
                xprim[cc] = sorted(xset)
            xprim = dumps(xprim)
            print("{}-prim: {}".format(site, xprim), file=outfile_sites)

            for xtype in ['default', 'langpref']: 
                for xtup, xset in sd.get_langdetect(xtype).items():            
                    xdet[xtup[0]].update(xset) 
            for cc, xset in xdet.items():
                xdet[cc] = sorted(xset)
            xdet = dumps(xdet)
            print("{}-det: {}".format(site, xdet), file=outfile_sites)

            alllang_counts_def[len(fsetdef)] += 1
            alllang_counts_pref[len(fsetpref)] += 1
            langs_offered_freq.update(fset)
            langs_offered_freq_default.update(fsetdef)
            langs_offered_freq_langpref.update(fsetpref)
            if sd.is_multilingual_site():
                alllang_counts_default_multilingual[len(fsetdef)] += 1
                langs_offered_freq_default_multilingual.update(fsetdef)
                alllang_counts_langpref_multilingual[len(fsetpref)] += 1
                langs_offered_freq_langpref_multilingual.update(fsetpref)
            else:
                alllang_counts_default_unilingual[len(fsetdef)] += 1
                langs_offered_freq_default_unilingual.update(fsetdef)
                alllang_counts_langpref_unilingual[len(fsetpref)] += 1
                langs_offered_freq_langpref_unilingual.update(fsetpref)



        print(f"# same: {nsame}/{n}={nsame/n}; badtags: {nbadtags}/{n}={nbadtags/n}", file=outfile)
        print("# langs supported (fset)", file=outfile)
        _print_counter(alllang_counts, outfile)
        print("# lang freq offered (fset)", file=outfile)
        _print_counter(langs_offered_freq, outfile)
        print("# langs supported (default)", file=outfile)
        _print_counter(alllang_counts_def, outfile)
        print("# lang freq offered (default)", file=outfile)
        _print_counter(langs_offered_freq_default, outfile)
        print("# langs supported (langpref)", file=outfile)
        _print_counter(alllang_counts_pref, outfile)
        print("# lang freq offered (langpref)", file=outfile)
        _print_counter(langs_offered_freq_langpref, outfile)
        print("# lang supported (default, unilingual sites)", file=outfile)
        _print_counter(alllang_counts_default_unilingual, outfile)
        _print_counter(langs_offered_freq_default_unilingual, outfile)
        print("# lang supported (default, multilingual sites)", file=outfile)
        _print_counter(alllang_counts_default_multilingual, outfile)
        _print_counter(langs_offered_freq_default_multilingual, outfile)
        print("# lang supported (langpref, unilingual sites)", file=outfile)
        _print_counter(alllang_counts_langpref_unilingual, outfile)
        _print_counter(langs_offered_freq_langpref_unilingual, outfile)
        print("# lang supported (langpref, multilingual sites)", file=outfile)
        _print_counter(alllang_counts_langpref_multilingual, outfile)
        _print_counter(langs_offered_freq_langpref_multilingual, outfile)

    def _dnssite_compare(self, outname):
        outfile = open(outname, "w")
        onehost = 0
        n = 0
        dnsnames_per_site = Counter()
        lang_host_combos = Counter()
        for site, sd in self._xmap.items():
            n += 1
            xset, nhosts = sd.cmp_url_vary()
            if nhosts == 1:
                onehost += 1
            dnsnames_per_site[nhosts] += 1
            lang_host_combos[len(xset)] += 1
            #if len(xset) >= 10:
            #    print(xset)
        print(f"# sites with one dns host {onehost}/{n}={onehost/n}", file=outfile)
        print("dnsnames per site", file=outfile)
        _print_counter(dnsnames_per_site, outfile)
        print("lang per host combinations", file=outfile)
        _print_counter(lang_host_combos, outfile)

    def _primlang_compare(self, outname):
        outfile = open(outname, "w")
        self._dump_primlang('default', outfile)
        self._dump_primlang('langpref', outfile)
        self._check_primlang('default', outfile)
        self._check_primlang('langpref', outfile)

    def _dump_primlang(self, xtype, outfile):
        langctr = {}
        n = 0
        for site, sd in self._xmap.items():
            n += 1
            for xtup, xset in sd.get_prim().items():
                if xtup[1] == xtype:
                    if xtup[0] in langctr:
                        ctr = langctr[xtup[0]]
                    else:
                        ctr = Counter()
                        langctr[xtup[0]] = ctr
                    ctr.update(xset)
        for cc in sorted(langctr.keys()):
            print(f"# {cc} primary language {xtype} {n} sites {sum(ctr.values())} tags", file=outfile)
            ctr = langctr[cc]
            _print_counter(ctr, outfile)


    def _check_primlang(self, xtype, outfile):
        num_prim_observed = Counter()
        lang_observed_vs_total_langs = Counter()
        n = 0
        for site, sd in self._xmap.items():
            if sd.is_multilingual_site() and sd.has_data(xtype):
                n += 1
                pset, nlangs = sd.cmp_primary(xtype)
                num_prim_observed[len(pset)] += 1
                lang_observed_vs_total_langs[(len(pset), nlangs)] += 1
        print(f"# num of primary {xtype} languages observed ({n} multilingual sites)", file=outfile)
        _print_counter(num_prim_observed, outfile)
        print(f"# primary {xtype} lang tags observed vs total langs, across all CCs", file=outfile)
        print("# [[primlang, alllang], occurrences]", file=outfile)
        _print_counter(lang_observed_vs_total_langs, outfile)

    def _prim_lang_matches_preference(self, outname):
        outfile = open(outname, "w")
        for cc in self._ccs:
            noffers = nprovides = nreallyprovides = 0
            n = 0
            providecounts = Counter()
            for site, sd in self._xmap.items():
                if sd.is_multilingual_site() and sd.has_data('langpref'):
                    n += 1
                    offers, noffered, provides, reallyprovides = sd.provides_preferred_language(cc)
                    if offers:
                        noffers += 1
                        if provides:
                            nprovides += 1
                        if reallyprovides:
                            nreallyprovides += 1
                        providecounts[provides] += 1

            print(f"# {cc} offers preferred lang: {noffers} out of {n}", file=outfile)
            print(f"# {cc} provides preferred lang: {nprovides} {nprovides/noffers}", file=outfile)
            print(f"# {cc} reallyprovides preferred lang: {nreallyprovides} {nreallyprovides/noffers}", file=outfile)
            print(f"# {cc} provides counts", file=outfile)
            _print_counter(providecounts, outfile)



def _print_counter(c, outfile):
    print(dumps(sorted(c.items())), file=outfile)


def _analyze_rec(sitemap, cc, xtype, rec):
    alllangs = _getexc(rec)|_getinf(rec)|_getprimary(rec)
    prim = _getprimary(rec)
    continf = _getcontent_detect(rec)
    sitemap.update(rec['reqhost'], rec['url'], cc, xtype, prim, alllangs, continf)


def _add_vary_info(sitemap, cc, xtype, rec):
    vary_on_lang = 'accept-language' in rec['vary']
    sitemap.update_vary(rec['reqhost'], cc, xtype, vary_on_lang)


# EXT = '_10k'
EXT = ''

def _collect_cc(xdir):
    ccset = defaultdict(dict)
    for f in glob.glob(f"{xdir}/*_default_summary{EXT}.json"):
    # for f in glob.glob(f"{xdir}/*_*_summary{EXT}.json"):
        path, ext = os.path.splitext(os.path.basename(f))
        mobj = re.match("^(\w{2})_(\w+)_summary"+EXT, path)
        cc = mobj.group(1)
        xtype = mobj.group(2)
        ccset[cc][xtype] = f

        f2 = f"{xdir}/{cc}_langpref_summary{EXT}.json"
        ccset[cc]['langpref'] = f2
    return ccset


def main():
    sitemap = SiteMap()

    xdir = "testdata"
    for cc, fdict in _collect_cc(xdir).items():
        for xtype, fname in fdict.items():
            print(f"{cc}/{xtype}: ", end='',flush=True)
            with open(fname) as inf:
                for i, recgroup in enumerate(map(loads, iter(inf))):
                    if i % 100 == 0:
                        print(".", end='', flush=True)
                    for rec in recgroup:
                        _analyze_rec(sitemap, cc, xtype, rec)
            varyfile = f"{xdir}/{cc}_{xtype}_vary{EXT}.json"
            with open(varyfile) as inf:
                for i, recgroup in enumerate(map(loads, iter(inf))):
                    if i % 100 == 0:
                        print(".", end="", flush=True)
                    for rec in recgroup:
                        _add_vary_info(sitemap, cc, xtype, rec)

    sitemap.analyze_site_differences(EXT)

main()
