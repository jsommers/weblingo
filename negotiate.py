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
host = 'www.cnn.com'
# host = 'www.wikipedia.org'
# host = 'localhost'

h = {
    'Host':host,
    'User-Agent':'happy happy, joy joy 1.0',
    'Accept-Language': 'cy; q=1.0, es; q=1.0, de; q=1.0, it ; q=1.0, en ; q=0.01',
    'Negotiate': 'trans',
}
response = requests.get('http://{}'.format(host), allow_redirects=True, headers=h)
print("Response code {}: {}".format(response.status_code, response.reason))
print("Server: {}".format(response.headers['Server']))
print(response.headers.get('Alternates', 'No alternates'))
print(response.headers)
soup = bs(response.text, 'html.parser')
response.close()
print(soup.html.attrs)

