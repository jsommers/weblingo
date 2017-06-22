"""
validlang.py.

Utility module with one function to check whether a language tag is
well-formed according to BCP47 (https://tools.ietf.org/html/bcp47)
but not if it is valid.
"""
import re

# based on http://schneegans.de/lv/

_bcp47_regex = re.compile("""
^
(
  (
    (
      (
        (?P<language23>
          [a-z]{2,3}
        )
        (-
          (?P<extlang>
            [a-z]{3}
          )
        ){0,3}
      )
      |
      (?P<language4>
        [a-z]{4}
      )
      |
      (?P<language58>
        [a-z]{5,8}
      )
    )

    (-(?P<script>
      [a-z]{4}
    ))?

    (-(?P<region>
      [a-z]{2}
      |
      [0-9]{3}
    ))?

    (-
      (?P<variant>
        [a-z0-9]{5,8}
        |
        [0-9][a-z0-9]{3}
      )
    )*

    (-
      (?P<extensions>
        [a-wy-z0-9]
        (-
          [a-z0-9]{2,8}
        )+
      )
    )*

    (-
      x(?P<privateusesubtags>-
        (
          [a-z0-9]{1,8}
        )
      )+
    )?
  )
  |
  (?P<privateusetags>
    x(-
      (
        [a-z0-9]{1,8}
      )
    )+
  )
  |
  (?P<grandfathered>
    (?P<irregular>
      en-GB-oed |
      i-ami |
      i-bnn |
      i-default |
      i-enochian |
      i-hak |
      i-klingon |
      i-lux |
      i-mingo |
      i-navajo |
      i-pwn |
      i-tao |
      i-tay |
      i-tsu |
      sgn-BE-FR |
      sgn-BE-NL |
      sgn-CH-DE
    )
    |
    (?P<regular>
      art-lojban |
      cel-gaulish |
      no-bok |
      no-nyn |
      zh-guoyu |
      zh-hakka |
      zh-min |
      zh-min-nan |
      zh-xiang
    )
  )
)
$
""", re.VERBOSE|re.IGNORECASE)


def well_formed_bcp47(s):
    """Check whether a language tag is well-formed according to bcp47"""
    mobj = _bcp47_regex.match(s)
    if mobj is not None:
        return mobj.groupdict()
    return None


if __name__ == '__main__':
    import sys
    print("> ", end='', flush=True)
    line = sys.stdin.readline()
    while line:
        tag = line.strip()
        result = well_formed_bcp47(tag)
        print(result)
        print("> ", end='', flush=True)
        line = sys.stdin.readline()
