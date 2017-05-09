import sys
import os

import requests
from bs4 import BeautifulSoup as bs

'''
NB: code below doesn't do a lot (yet).  it simply checks whether the
server implements content negotiation alternates (with a hard-coded 
language alternates list).  will need to explicitly identify the types
of behaviors we intend to explore on a server and expand this program
substantially.

very likely need to get an actual file (or a few) and then probe for 
alternates, rather than attempting the top-level.

pretty disappointing to immediately get a bunch of 'no alternates' from
all servers below (save localhost, that is).

may be that very few sites use this (my sense is that it's quite a pain
to configure and then keep up to date), but with modern web applications
delivering data, it is also likely that these mechanisms simply don't work
well in dynamic web context, i.e., they were conceived when static content
was still king.
'''

# host = 'apache.org'
# host = 'www.cnn.com'
# host = 'www.wikipedia.org'
# host = 'localhost'

if len(sys.argv) != 2:
    print("usage: {} hostname".format(sys.argv[0]))
    sys.exit()

host = sys.argv[1]

h = {
    'Host':host,
    'User-Agent':'happy happy, joy joy 1.0',
    'Accept': '*',
    'Accept-Charset': '*',
    'Accept-Encoding': '*',
    'Accept-Language': 'cy; q=1.0, es; q=1.0, de; q=1.0, it ; q=1.0, en ; q=0.01',
    'Negotiate': 'trans',
}
response = requests.get('http://{}'.format(host), allow_redirects=True, headers=h)
print("Response code {}: {}".format(response.status_code, response.reason))
print(response.headers.get('Server', 'No server tag?'))
print(response.headers.get('Alternates', 'No alternates'))
print(response.headers)
soup = bs(response.text, 'html.parser')
response.close()
print(soup.html.attrs)
print("History: ",response.history)
if 'lang' in soup.html.attrs:
    print("Document language: {}".format(soup.html['lang']))
else:
    print("No specified document language")
for atag in soup.find_all('a'):
    if 'hreflang' in atag.attrs:
        print("hreflang on link: {} -> {}".format(atag['hreflang'], atag['href']))

