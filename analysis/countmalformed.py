import langcodes
import langtags

with open('malformed.txt') as infile:
    d = eval(infile.read().strip())

totalct = sum(d.values())
interpolate_count = 0
langname = 0
others = 0
encoding = 0
isregion = 0
xfalse = 0

regions = [t.subtag for t in langtags.LanguageSubtagRegistry.itertags(langtags.SubtagRecordType.Region)]

for key, ct in d.items():
    if '{' in key or '}' in key or '$' in key or key.startswith('<%= ') or key.startswith('#') or key.startswith('[[') or key.startswith('???'):
        interpolate_count += ct
    elif 'utf' in key.lower():
        encoding += ct
        print(key)
    elif key.lower() == 'false':
        xfalse += ct
    else:
        stillcheck = True
        try:
            lc = langcodes.find(key)
            langname += ct
            stillcheck = False
        except:
            pass

        if stillcheck:
            if key.upper().split('-')[0] in regions:
                isregion += ct
            else:
                print(key)
                others += ct



print(f"Total errors: {totalct}")
print(f"Interpolate errors {interpolate_count} {interpolate_count/totalct}")
print(f"Language name errors {langname} {langname/totalct}")
print(f"is region {isregion} {isregion/totalct}")
print(f"Encoding {encoding} {encoding/totalct}")
print(f"False {xfalse} {xfalse/totalct}")
print(f"{others} {others/totalct}")
