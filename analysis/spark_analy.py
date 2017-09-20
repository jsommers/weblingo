import sys
import os.path
import glob
import importlib

try:
    import pyspark
    USE_SPARK = True
except:
    USE_SPARK = False


def _parse_arguments():
    adict = {}
    for arg in sys.argv[1:]:
        key, val = arg.split('=')
        adict[key] = val
    return adict


def main(argdict):
    datadir = argdict.pop('dir', '/data/weblingo')
    filepat = "{}.json".format(argdict.pop('pattern', '*'))
    files_in = glob.glob(os.path.join(datadir, filepat))
    file_out = os.path.join(datadir, argdict.pop('out', 'outfile.txt'))
    if 'analyze' not in argdict:
        print("No analysis module specified (analyze=)")
        sys.exit()
    analysis_module = argdict.pop('analyze')

    print("Files to analyze: {}".format(','.join(files_in)))
    print("Output file: {}".format(file_out))
    if analysis_module.endswith('.py'):
        analysis_module = analysis_module.rstrip('.py')
    print("Running analysis with {}".format(analysis_module))
    amod = importlib.import_module(analysis_module)
    if not hasattr(amod, 'analyze'):
        print("No analyze symbol in {}; exiting".format(analysis_module))
        sys.exit()

    if not USE_SPARK:
        if hasattr(amod, 'analyze_nospark'):
            amod.analyze_nospark(files_in, file_out, **argdict)
        else:
            print("No analyze_nospark symbol in {}; not doing anything".format(
                analysis_module))
    else:
        conf = pyspark.SparkConf()
        sc = pyspark.SparkContext(appName=analysis_module, conf=conf)
        amod.analyze(sc, files_in, file_out, **argdict)


def _usage():
    print("Usage: {} analyze=<amodule> [kwoptions]".format(sys.argv[0]))
    print("\t\t[dir=/data/weblingo]")
    print("\t\t[pattern=*]json")
    print("\t\t[out=outfile.txt]")
    print("\t\tOther kwargs are passed into analysis module.")
    sys.exit()


if __name__ == '__main__':
    if len(sys.argv) > 1 and (sys.argv[1] == '-h' or sys.argv[1] == '--help'):
        _usage()
    argdict = _parse_arguments()
    main(argdict)
