import ujson as json
import requests
import re
import codecs
import logging
from os.path import expanduser, isfile
from time import sleep, time, ctime, timezone

#logging.basicConfig(filename='yank-repo-issues.log',level=logging.INFO)

STATE_FILE = expanduser('~/Data/github/yank-repo-issues.state')
REPO_FILE = expanduser('~/Data/github/processed_user_samples.json')

LINKRE = re.compile(r'(?:<(.*?)>; rel="(.*?)")+')
LINKBASE = 'https://api.github.com/users/%s/repos?per_page=100'

PAYLOAD = {'state':'all'}

def get_page(link, token, bad_request_max=0):
    """
    Request a page from Github.

    Requests a page from Github, retrying on failure (ConnectionError
    or bad request), up to a limit.

    link -- requested link
    token -- authentication token
    bad_request_max -- # of retries after bad requests
    """
    need_request = True
    bad_request_count = 0
    while need_request and (bad_request_count <= bad_request_max):
        try:
            page = requests.get(link,
                    auth=(token,''))
            need_request = False
        except requests.ConnectionError as err:
            logging.error('Connection error at <%s>: %s',
                    link,
                    err)
            print 'Retry on:\n%s' % str(err)

        # issues
        if not page.ok:
            bad_request_count += 1
            need_request = True
            logging.error('Bad request %d (%d): <%s>',
                    page.status_code,
                    link)
        else:
            logging.info('(%d): <%s>', page.status_code, link)

    return page

def next_page(page):
    """
    Find next page link from the current page, if possible.
    """
    if 'link' in page.headers:
        link = page.headers['link']
        matches = LINKRE.findall(link)
        try:
            next_link = [x for (x, y) in matches if y == 'next'][0]
        except IndexError:
            next_link = None
    else:
        next_link = None
    return next_link

def rate_limit_pause(token):
    """
    Pause until rate limit reset.
    """
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

def yank_driver(item_file,
        item_func,
        out_base,
        itemmax=5,
        token=None,
        token_file='~/Data/github/token',
        state_file=None):
    """
    Drives few-page, many-item yanking operations.

    item_file -- file with one "item" per line, in the yanking order
    item_func -- function returning item id and first link to pull
        given item string as input
    out_base -- base directory to write output to
    itemmax -- number of items to sample
    token -- authentication token (or:)
    token_file -- file containing authentication token
    state_file -- file describing yanking state
    """

    if not token:
        with open(expanduser(token_file)) as fin:
            token = fin.read().strip()

    # pull state
    skip = 0
    if state_file and isfile(state_file):
        with open(state_file, mode='r') as fin:
            text = fin.read().split()
            if text:
                skip = int(text[0]) - 1
                print 'skipping %d' % skip

    # can probably do this at a more opportune time,
    # that lets me skip through skipped users
    rate_limit_pause(token)

    with open(item_file, mode='r') as fin:
        counter = 1
        for line in fin:
            # skip retrieved users
            if counter <= skip:
                counter += 1
                continue

            # break at sampling limit
            if counter > itemmax:
                break

            item = item_func(line)
            itemid = item['id']
            next_link = item['link']

            #jsonitem = json.loads(line)

            #itemid = jsonitem['full_name']
            #next_link = '%s/issues?state=all&per_page=100' % jsonitem['url']

            #owner = line.strip()
            #next_link = LINKBASE % owner

            print "Item %d <%s>" % (counter, itemid)

            # pull all item pages, lumping jsons
            retrieving = True
            itemjsons = []
            while retrieving:
                page = get_page(next_link, token, bad_request_max=5)

                # get remaining rate limit
                rate_limit = int(page.headers['x-ratelimit-remaining'])

                print('%s :: %d (%d)' % 
                        (next_link,rate_limit,page.status_code))

                # append current page of shit
                itemjsons.extend(page.json())

                # look for pagination
                next_link = next_page(page)

                if next_link:
                    logging.info('%s has more pages', itemid)
                else:
                    retrieving = False

                # wait while rate-limited
                if rate_limit == 0:
                    rate_limit_pause(token)

            # write out item jsons
            with codecs.open(
                    expanduser('%s/%0.8d.json' % (out_base, counter)),
                    mode='w',
                    encoding='utf8') as fout:
                fout.write('%s\n' % itemid)
                for itemjson in itemjsons:
                    fout.write(json.dumps(itemjson) + '\n')

            # finalize the transaction
            counter += 1
            with open(state_file, mode='w') as fout:
                fout.write("%d\n" % counter)


