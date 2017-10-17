"""Driver program for langnego."""

import subprocess
import argparse
import sys
import re
import signal
import time
import os
import os.path

import langcodes

INFILE = "alexa1m.csv.xz"
WORKERS = 128
TUNDEV = "tun0"
PYEXEC = "/usr/bin/python3"
WRITEBUF = 100
CHUNKSIZE = 10000
OUTDIR = "/data/weblingo"
MAXHOSTS = 1000000
STATUSFILE = "measurement_status_{}.txt"
# ugh; global variable to handle SIGALRM watchdog
_alarmed = False


def _load_run_config():
    cfg = {}
    with open("runconfig.txt") as infile:
        for line in infile:
            line = line.strip()
            if line.startswith('#'):
                continue
            vpn, cc, tld, langpref = line.split("::")
            cfg[cc] = [vpn, tld, langpref]
    return cfg


def _downvpn():
    while True:
        try:
            proc = subprocess.run(["/usr/bin/expressvpn", "disconnect"],
                                  timeout=30, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT,
                                  universal_newlines=True)
        except subprocess.TimeoutExpired:
            print("VPN disconnect timed out.  Trying again.")
        else:
            if proc.returncode == 0 and re.search("Disconnect", proc.stdout):
                break
            elif re.search("Not connected", proc.stdout):
                break
    print("Disconnected from VPN")


def _upvpn(loc):
    while True:
        try:
            proc = subprocess.run(["/usr/bin/expressvpn", "connect", loc],
                                  timeout=60, stdout=subprocess.PIPE,
                                  universal_newlines=True)
        except subprocess.TimeoutExpired:
            print("VPN connect timed out.  Trying again.")
        else:
            if proc.returncode == 0 and re.search("Connected", proc.stdout):
                break
            elif re.search("Already connected!", proc.stdout):
                _downvpn()


def _statvpn():
    proc = subprocess.run(
        ["/usr/bin/expressvpn", "status"],
        stdout=subprocess.PIPE,
        universal_newlines=True)
    print(proc.stdout)
    vpnstatus = proc.stdout
    proc = subprocess.run(["/sbin/ifconfig", TUNDEV], stdout=subprocess.PIPE,
                          universal_newlines=True)
    if re.search("UP POINTOPOINT RUNNING", proc.stdout, flags=re.MULTILINE):
        print("VPN tunnel is up: ", end='')
        rxp = re.search("RX packets:\s*(\d+)", proc.stdout, flags=re.MULTILINE)
        txp = re.search("TX packets:\s*(\d+)", proc.stdout, flags=re.MULTILINE)
        rxb = re.search("RX bytes:\s*(\d+)", proc.stdout, flags=re.MULTILINE)
        txb = re.search("TX bytes:\s*(\d+)", proc.stdout, flags=re.MULTILINE)
        if rxp and txp and rxb and txb:
            print("RX: {} pkt {} bytes;".format(
                rxp.groups()[0], rxb.groups()[0]), end='')
            print("TX: {} pkt {} bytes".format(
                txp.groups()[0], txb.groups()[0]))
        else:
            print("(counters not found)")
    else:
        print("VPN tunnel is not up?: {}".format(proc.stdout))
    return vpnstatus


def _watchdog_alarm(*args):
    global _alarmed
    _alarmed = True
    print("Got watchdog alarm")


def _run_langnego(cc, langpref, startseq, outname):
    subdir = os.path.join(OUTDIR, cc)
    OUTNAME = os.path.join(
        subdir, "{}_{}_{:06d}".format(cc, outname, startseq))

    cmd = [PYEXEC, "langtag_crawl.py",
           "-s", str(startseq),
           "-m", str(CHUNKSIZE),
           "-w", str(WORKERS),
           "-i", INFILE,
           "-o", OUTNAME,
           "-W", str(WRITEBUF),
           "-l", langpref]

    print("Starting measurement process")
    proc = None
    signal.signal(signal.SIGALRM, _watchdog_alarm)
    signal.alarm(3600*6)    # 6h; should be tuned for the chunk size
    global _alarmed
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL,
                                universal_newlines=True)
        print("langnego process {} started".format(proc.pid))
        while not _alarmed:
            line = proc.stdout.readline()
            if not line:
                break
            print(line)

        if _alarmed:
            proc.terminate()

    except KeyboardInterrupt:
        print("Got ^C: going down in a ball of flames")
        proc.terminate()
        _downvpn()
    except subprocess.TimeoutExpired:
        print("Subprocess took __way__ too long.  Terminating it.")
        proc.terminate()
        _downvpn()

    proc.wait()


def _initialize_status(loclist, name):
    lochash = {}
    for loc in loclist:
        lochash[loc] = _read_done_status(loc, name)
        print("{} starting with {}".format(loc, lochash[loc]))
    return lochash


def _measurements_complete(lochash):
    for loc, count in lochash.items():
        if count < MAXHOSTS:
            return False
    return True


def _get_next_location(lochash):
    lowest = min(lochash.values())
    for loc, count in lochash.items():
        if count == lowest:
            return loc


def _write_start_status(loc, vpnstatus, name):
    outname = os.path.join(OUTDIR, loc, STATUSFILE.format(name))
    mode = "a"
    if not os.path.exists:
        mode = "w"
    outfile = open(outname, mode)
    tstr = time.strftime("%Y%m%d%H%M%S")
    print("{} START {}".format(tstr, vpnstatus), file=outfile)
    outfile.flush()
    outfile.close()


def _write_done_status(loc, success, seq, vpnstatus, name):
    outname = os.path.join(OUTDIR, loc, STATUSFILE.format(name))
    mode = "a"
    if not os.path.exists:
        mode = "w"
    outfile = open(outname, mode)
    tstr = time.strftime("%Y%m%d%H%M%S")
    print("{} {} {} {}".format(tstr, success, seq, vpnstatus), file=outfile)
    outfile.flush()
    outfile.close()


def _read_done_status(loc, name):
    seq = 0
    infile = None
    try:
        infile = open(os.path.join(OUTDIR, loc, STATUSFILE.format(name)), "r")
    except FileNotFoundError:
        return seq

    for line in infile:
        mobj = re.match("\d{14} SUCCESS (?P<seq>\d+)", line)
        if mobj:
            seq = int(mobj.groupdict()['seq'])
    infile.close()
    return seq


def _ensure_directories(loclist):
    for loc in loclist:
        dirname = os.path.join(OUTDIR, loc)
        if not os.path.exists(dirname):
            os.mkdir(dirname)


def _launch_measurements(runcfg, locations, name, langpref):
    location_status = _initialize_status(locations, name)
    _ensure_directories(locations)

    while not _measurements_complete(location_status):
        loc = _get_next_location(location_status)
        start_seq = location_status[loc]

        vpn, tld, loclangs = runcfg[loc]
        locrec = langcodes.Language.make(region=loc)
        print("Collecting measurements from {}".format(locrec.region_name()))

        if langpref == "loc":
            accept_language = loclangs
        else:
            accept_language = langpref

        _downvpn()
        _upvpn(loc)
        status = _statvpn()
        assert(re.match("Connected to", status))
        _write_start_status(loc, status.strip(), name)

        _run_langnego(loc, accept_language, start_seq, name)
        status = _statvpn()
        if re.match("Connected to", status):
            location_status[loc] += CHUNKSIZE
            _write_done_status(
                loc, "SUCCESS", location_status[loc], status.strip(), name)
        else:
            print("VPN disconnected; not updating counter")
            _write_done_status(
                loc, "FAIL", location_status[loc], status.strip(), name)
        _downvpn()


if __name__ == '__main__':
    runcfg = _load_run_config()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l", "--loc", dest="location",
        action="append",
        help="Specify CC location from which to launch measurements")
    parser.add_argument(
        "-p", "--printloc", action="store_true",
        help="Print all known CC locations, then exit.")
    parser.add_argument(
        "-n", "--name", default="", dest="name",
        help="Name to include in output files")
    parser.add_argument(
        "-a", "--acceptlang", default="", dest="acceptlang",
        help="Set Accept-Language header.  Special value 'loc' can be used.")
    args = parser.parse_args()

    if args.printloc:
        print("Locations: {}".format(",".join(list(runcfg.keys()))))
        sys.exit()

    if not args.location:
        parser.print_usage()
        sys.exit()

    for loc in args.location:
        if loc not in runcfg:
            print("Location {} unknown".format(loc))
            sys.exit()

    _launch_measurements(runcfg, args.location, args.name, args.acceptlang)
