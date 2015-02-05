import ujson as json
import requests
import re
import codecs
import logging
from os.path import expanduser, isfile
from time import sleep, time, ctime, timezone

logging.basicConfig(filename='yank-user-samples.log',level=logging.INFO)

STATE_FILE = expanduser('~/Data/github/yank-user-samples.state')
OWNER_FILE = expanduser('~/Data/github/owner-sample-order.csv')

LINKRE = re.compile(r'(?:<(.*?)>; rel="(.*?)")+')
LINKBASE = 'https://api.github.com/users/%s/repos?per_page=100'

def get_page(next_link,token):
    need_request = True
    while need_request:
        try:
            page = requests.get(next_link,
                    auth=(token,''))
            need_request = False
        except requests.ConnectionError as err:
            logging.error('Connection error at <%s>: %s',
                    next_link,
                    err)
            print 'Retry on:\n%s' % str(err)
    return page

def next_page(page):
    if 'link' in page.headers:
        link = page.headers['link']
        matches = LINKRE.findall(link)
        try:
            next_link = [x for (x,y) in matches if y == 'next'][0]
        except IndexError as err:
            next_link = None
    else:
        next_link = None
    return next_link

def rate_limit_pause(token,pause=30):
    rate_limit=0
    while rate_limit == 0:
        #print "Rate limited",
        #sleep(pause)
        page = requests.get('https://api.github.com/rate_limit',
                auth=(token,''))
        rate_limit = int(page.headers['x-ratelimit-remaining'])
        rate_reset = int(page.headers['x-ratelimit-reset'])
        wait_time = rate_reset - int(time())
        if rate_limit == 0:
            print "Rate limited, reset %s, sleeping %d" % (ctime(rate_reset), wait_time)
            sleep(wait_time + 1)


def main(umax=5):
    with open(expanduser('~/Data/github/token')) as fin:
        token = fin.read().strip()

    # pull state
    skip = 0
    if isfile(STATE_FILE):
        with open(STATE_FILE, mode='r') as fin:
            text = fin.read().split()
            if text:
                skip = int(text[0]) - 1
                print 'skipping %d' % skip

    # can probably do this at a more opportune time,
    # that lets me skip through skipped users
    rate_limit_pause(token)

    with open(OWNER_FILE, mode='r') as fin:
        counter = 1
        for line in fin:
            # skip retrieved users
            if counter <= skip:
                counter += 1
                continue

            # break at sampling limit
            if counter > umax:
                break

            owner = line.strip()
            next_link = LINKBASE % owner

            print "User %d <%s>" % (counter, owner)

            # pull all user's repo pages
            retrieving = True
            repos = []
            while retrieving:
                page = get_page(next_link, token)

                # issues
                if not page.ok:
                    logging.error('Bad request (%d): <%s>',
                            page.status_code,
                            next_link)
                else:
                    logging.info('(%d): <%s>', page.status_code, next_link)

                # get remaining rate limit
                rate_limit = int(page.headers['x-ratelimit-remaining'])

                print '%s :: %d (%d)' % (next_link,rate_limit,page.status_code)

                # get data
                repos.extend(page.json())

                # look for pagination
                next_link = next_page(page)

                if next_link:
                    logging.info('%s has more pages', owner)
                else:
                    retrieving = False

                # wait while rate-limited
                if rate_limit == 0:
                    rate_limit_pause(token)

            # write out user repos
            with codecs.open(
                    expanduser('~/Data/github/user-repos/%0.8d.json' % counter),
                    mode='w',
                    encoding='utf8') as fout:
                for repo in repos:
                    fout.write(json.dumps(repo) + '\n')

            # finalize the transaction
            counter += 1
            with open(STATE_FILE,mode='w') as fout:
                fout.write("%d\n" % counter)


if __name__=="__main__":
    main(umax=15000)




