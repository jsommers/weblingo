import requests
from bs4 import BeautifulSoup as bs
import codecs


def gettlds():
    r = requests.get('https://www.iana.org/domains/root/db')
    soup = bs(r.text, 'html.parser')
    tlist = []
    for row in soup.select('tbody tr'):
        tdlist = row.find_all('td')
        assert(len(tdlist) == 3)
        domain = tdlist[0].find('a').string
        ascii_domain = domain.lower().replace('.', '').replace(
            '\u200e', '').replace('\u200f', '')
        ascii_domain = codecs.encode(ascii_domain, 'idna').decode('ascii')
        xtype = tdlist[1].string.replace('\n', '')  # type
        desc = tdlist[2].string.replace('\n', '')  # description
        tlist.append((domain, ascii_domain, xtype, desc))
    return tlist


def updatetldmap(activelist):
    outx = open('outxmap.txt', 'w')
    with open("tldmap.txt") as infile:
        for line in infile:
            origline = line.strip()
            if origline.startswith('#') or not origline:
                print(origline, file=outx)
                continue
            idx = origline.find('#')
            if idx != -1:
                line = origline[:idx]
            else:
                line = origline
            fields = line.split()
            domain = fields[0]
            langtags = fields[1:]
            while len(activelist) and activelist[0][1] <= domain:
                newrec = activelist.pop(0)
                if newrec[1] == domain:
                    break
                print("{}  # NEW {} {} {}".format(
                    newrec[1], newrec[2], newrec[0], newrec[3]), file=outx)
            print(origline, file=outx)
    outx.close()


if __name__ == '__main__':
    tlist = gettlds()
    outname = 'activetlds.txt'
    with open(outname, 'w') as outfile:
        for domain, ascii_domain, xtype, desc in tlist:
            print("{}:::{}:::{}".format(domain, xtype, desc), file=outfile)
    print("Wrote {} tlds to {}".format(len(tlist), outname))
    updatetldmap(tlist)
