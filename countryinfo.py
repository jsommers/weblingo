import requests
from bs4 import BeautifulSoup as bs
import sys


def _geonames():
    langmap = {}
    r = requests.get(
        'http://download.geonames.org/export/dump/countryInfo.txt')
    content = r.content.decode('utf8')
    for line in content.split('\n'):
        if line.startswith('#'):
            continue
        fields = line.split('\t')
        if len(fields) < 9:
            continue
        countrycode = fields[0]
        cctld = fields[9]
        langcodes = fields[15]
        langmap[countrycode.upper()] = [ cctld, langcodes.split(',') ]
    return langmap


def _openstreetmap():
    langmap = {}
    r = requests.get(
        'http://wiki.openstreetmap.org/wiki/Nominatim/Country_Codes')
    soup = bs(r.text, 'html.parser')
    tlist = []
    for row in soup.find_all('tr'):
        if row.find('td') is None:
            continue
        data = [td.string.strip() for td in row.find_all('td')]
        assert(len(data) == 3)
        langmap[data[0].upper()] = [data[1], data[2].split(',')]
    return langmap


def _merge_lists(xl1, xl2):
    return_list = xl1[:]
    for val in xl2:
        if val not in return_list:
            return_list.append(val)
    return return_list


def main():
    map1 = _geonames()
    map2 = _openstreetmap()
    combined = {}
    for cc, data in map2.items():
        if cc in map1:
            xlist = map1.pop(cc)  # cctld, langs
            langs = _merge_lists(xlist[1], data[1])
            combined[cc] = [xlist[0], data[0], langs]  # cctld, name, langs
        else:
            # only in map2
            # print("Only on openstreetmap: {}".format(cc))
            combined[cc] = ['', data[0], data[1]]

    # only in map1
    for cc, data in map1.items():
    #    print("Only on geonames: {}".format(cc))
        combined[cc] = [data[0], '', data[1]]

    # print combined
    with open('countryinfo.txt', 'w') as outfile:
        for cc in sorted(combined.keys()):
            data = combined[cc]
            print("{}::{}::{}::{}".format(
                cc, data[0], data[1], ','.join(data[2])), file=outfile)


if __name__ == '__main__':
    main()
