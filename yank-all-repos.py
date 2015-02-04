#import github
import requests
import re
import codecs
from os.path import expanduser, isfile
from time import sleep
import logging

logging.basicConfig(filename='yank-all-repos.log',level=logging.INFO)

STATE_FILE = expanduser('~/Data/github/yank-all-repos.state')

LINKRE = re.compile(r'(?:<(.*?)>; rel="(.*?)")+')

#@profile
def main():
    with open(expanduser('~/Data/github/token')) as fin:
        token = fin.read().strip()

    counter = 0
    if isfile(STATE_FILE):
        with open(STATE_FILE) as fin:
            text = fin.read().split()
            next_link = text[0]
            counter = int(text[1])
        if not next_link:
            next_link = 'https://api.github.com/repositories'
            counter = 0
    else:
        next_link = 'https://api.github.com/repositories'

    page = None
    page_len = 0 
    # stop at page_len <= 2: when we run out of pages, github returns: "[]"
    #while not page or page_len > 0:
    while not page or page_len > 2:
        need_request = True
        while need_request:
            try:
                page = requests.get(next_link,auth=(token,''))
                need_request = False
            except requests.ConnectionError as err:
                logging.error('Connection error at <%s>: %s' % (next_link, str(err)))
                print 'Retry on:\n%s' % str(err)
                

        #page_json = page.json()
        #page_len = len(page_json)
        page_len = len(page.text)

        # issues
        if not page.ok:
            logging.error('Bad request (%d): <%s>' % (page.status_code, next_link))
        else:
            logging.info('(%d): <%s>' % (page.status_code, next_link))

        # write json
        with codecs.open(
                expanduser('~/Data/github/all-repos/%0.8d.json' % counter),
                mode='w',
                encoding='utf8') as fout:
            #fout.write(page_json)
            fout.write(page.text)

        counter = counter + 1
        
        # parse next link
        link = page.headers['link']
        matches = LINKRE.findall(link)
        try:
            next_link = [x for (x,y) in matches if y == 'next'][0]
        except IndexError as err:
            logging.error('No next link')
            raise
        #if not next_link:
            #raise ValueError('No next link: ' + link)

        # write state
        with open(STATE_FILE, mode='w') as fout:
            fout.write('%s\n%d\n' % (next_link, counter))

        # get remaining rate limit
        rate_limit = int(page.headers['x-ratelimit-remaining'])

        print '%s :: %d (%d)' % (next_link,rate_limit,page.status_code)

        while rate_limit == 0:
            print "Rate limited"
            sleep(30)
            page = requests.get('https://api.github.com/rate_limit',
                    auth=(token,''))
            rate_limit = int(page.headers['x-ratelimit-remaining'])

if __name__ == "__main__":
    main()


