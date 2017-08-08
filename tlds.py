import enum
from collections import defaultdict
import idna


class TldType(enum.IntEnum):
    Generic = 1
    GenericRestricted = 2
    Cctld = 3
    Sponsored = 4
    Infrastructure = 5
    Test = 6

    @staticmethod
    def fromstr(s):
        if s == 'sponsored':
            return TldType.Sponsored
        elif s == 'generic':
            return TldType.Generic
        elif s == 'generic-restricted':
            return TldType.GenericRestricted
        elif s == 'country-code':
            return TldType.Cctld
        elif s == 'test':
            return TldType.Test
        elif s == 'infrastructure':
            return TldType.Infrastructure
        else:
            raise ValueError("Invalid tld type {}".format(s))


class Tld(object):
    """Encapsulates info about a tld"""
    def __init__(self, domain, xtype, desc):
        self._native_domain = domain
        domain = domain.lower().replace('.', '').replace(
            '\u200e', '').replace('\u200f', '')
        self._domain = idna.encode(domain).decode('ascii')
        self._desc = desc
        if isinstance(xtype, TldType):
            self._xtype = xtype
        else:
            self._xtype = TldType.fromstr(xtype)
        self._langset = set()

    @property
    def domain(self):
        """Return the tld, possibly punycode-encoded"""
        return self._domain

    @property
    def native_domain(self):
        """Return the tld in all its unicode gory"""
        return self._native_domain

    @property
    def desc(self):
        return self._desc

    @property
    def tldtype(self):
        return self._xtype

    @property
    def related_languages(self):
        return self._langset

    def add_language_subtag(self, s):
        self._langset.add(s)

    def has_language_subtag(self, s):
        return s in self._langset

    def __str__(self):
        return "{} ({}): {}".format(self._domain, self._native_domain,
                                    list(self._langset))


class TldMap(object):
    def __init__(self, tldbase='activetlds.txt', tld2lang='tldmap.txt'):
        self._fwdmap = {}
        self._load_active_tlds(tldbase)
        self._load_langmap(tld2lang)
        self._revmap = self._invert_dict(self._fwdmap)

    def _load_active_tlds(self, fname):
        self._fwdmap = {}
        with open(fname) as infile:
            for line in infile:
                domain, xtype, desc = line.strip().split(':::')
                xtype = xtype.strip()
                desc = desc.strip()
                t = Tld(domain, xtype, desc)
                self._fwdmap[t.domain] = t

    def _load_langmap(self, fname):
        with open(fname) as inpfile:
            for line in inpfile:
                line = line.strip()
                if line.startswith('#') or not line:
                    continue
                idx = line.find('#')
                if idx != -1:
                    line = line[:idx]
                fields = line.split()
                domain = fields[0]
                langtags = fields[1:]
                for s in langtags:
                    self._fwdmap[domain].add_language_subtag(s)

    @staticmethod
    def _invert_dict(d):
        xd = defaultdict(list)
        for tld, tldobj in d.items():
            for lt in tldobj.related_languages:
                xd[lt].append(tldobj)
        return xd

    def dump(self):
        for dom, tld in self._fwdmap.items():
            print(tld)


if __name__ == '__main__':
    t = TldMap()
    t.dump()
