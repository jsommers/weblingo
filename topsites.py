import sys
import lzma
from zipfile import ZipFile
import time


def _read_input_majestic(infile):
    hostlist = []
    with open(infile) as inp:
        for line in inp:
            fields = line.strip().split(',')
            if len(fields) == 12:  # majestic million format
                if fields[0] == 'GlobalRank':
                    continue
                rank, name = int(fields[0]), fields[2]
            else:
                raise ValueError("Unrecognized file format")
            hostlist.append((rank, name))
    return hostlist


def _read_input2(infile, innerfile):
    hostlist = []
    with ZipFile(infile) as zipin:
        with zipin.open(innerfile) as inp:
            for line in inp:
                line = line.decode('utf8')
                fields = line.strip().split(',')
                if len(fields) == 2:  # rank,name format
                    rank, name = int(fields[0]), fields[1]
                else:
                    raise ValueError("Unrecognized file format")
                hostlist.append((rank, name))
    return hostlist


def main(majestic, alexazip, statvoozip):
    majestic = _read_input_majestic(majestic)
    alexa = _read_input2(alexazip, 'top-1m.csv')
    statvoo = _read_input2(statvoozip, 'top-1million-sites.csv')
    alexa_set = set([t[1] for t in alexa])
    statvoo_set = set([t[1] for t in statvoo])
    majestic_set = set([t[1] for t in majestic])
    print("alexa/statvoo overlap", len(alexa_set.intersection(statvoo_set)))
    print("alexa/majestic overlap", len(alexa_set.intersection(majestic_set)))
    statvoo_extra = statvoo_set.difference(alexa_set)
    statvoo_filtered = [t for t in statvoo if t[1] in statvoo_extra]
    majestic_extra = majestic_set.difference(alexa_set)
    majestic_filtered = [t for t in majestic if t[1] in majestic_extra]
    alexa.extend(statvoo_filtered)
    alexa.extend(majestic_filtered)
    alexa.sort()
    print("Writing output")
    tstr = time.strftime("%Y%m%d")
    with lzma.open('combined_{}.csv.xz'.format(tstr), 'wt') as outfile:
        for t in alexa:
            print("{},{}".format(*t), file=outfile)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
