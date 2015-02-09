import ujson as json
#import requests
#import re
#import codecs
import ghp
import logging
from os.path import expanduser, isfile
#from time import sleep, time, ctime, timezone


STATE_FILE = expanduser('~/Data/github/yank-repo-issues.state')
REPO_FILE = expanduser('~/Data/github/processed_user_samples.json')

#LINKRE = re.compile(r'(?:<(.*?)>; rel="(.*?)")+')
LINKBASE = 'https://api.github.com/users/%s/repos?per_page=100'

#PAYLOAD = {'state':'all'}

#def get_page(next_link,token):
    #need_request = True
    #while need_request:
        #try:
            #page = requests.get(next_link,
                    #auth=(token,''))
            #need_request = False
        #except requests.ConnectionError as err:
            #logging.error('Connection error at <%s>: %s',
                    #next_link,
                    #err)
            #print 'Retry on:\n%s' % str(err)
    #return page

#def next_page(page):
    #if 'link' in page.headers:
        #link = page.headers['link']
        #matches = LINKRE.findall(link)
        #try:
            #next_link = [x for (x,y) in matches if y == 'next'][0]
        #except IndexError as err:
            #next_link = None
    #else:
        #next_link = None
    #return next_link

#def rate_limit_pause(token,pause=30):
    #rate_limit=0
    #while rate_limit == 0:
        ##print "Rate limited",
        ##sleep(pause)
        #page = requests.get('https://api.github.com/rate_limit',
                #auth=(token,''))
        #rate_limit = int(page.headers['x-ratelimit-remaining'])
        #rate_reset = int(page.headers['x-ratelimit-reset'])
        #wait_time = rate_reset - int(time())
        #if rate_limit == 0:
            #print "Rate limited, reset %s, sleeping %d" % (ctime(rate_reset), wait_time)
            #sleep(wait_time + 1)

#def main(umax=5):
    #with open(expanduser('~/Data/github/token')) as fin:
        #token = fin.read().strip()

    ## pull state
    #skip = 0
    #if isfile(STATE_FILE):
        #with open(STATE_FILE, mode='r') as fin:
            #text = fin.read().split()
            #if text:
                #skip = int(text[0]) - 1
                #print 'skipping %d' % skip

    ## can probably do this at a more opportune time,
    ## that lets me skip through skipped users
    #rate_limit_pause(token)

    #with open(REPO_FILE, mode='r') as fin:
        #counter = 1
        #for line in fin:
            ## skip retrieved users
            #if counter <= skip:
                #counter += 1
                #continue

            ## break at sampling limit
            #if counter > umax:
                #break

            #jsonitem = json.loads(line)

            #itemid = jsonitem['full_name']
            #next_link = '%s/issues?state=all&per_page=100' % jsonitem['url']

            ##owner = line.strip()
            ##next_link = LINKBASE % owner

            #print "Item %d <%s>" % (counter, itemid)

            ## pull all user's repo pages
            #retrieving = True
            #issues = []
            #while retrieving:
                #page = get_page(next_link, token)

                ## issues
                #if not page.ok:
                    #logging.error('Bad request (%d): <%s>',
                            #page.status_code,
                            #next_link)
                #else:
                    #logging.info('(%d): <%s>', page.status_code, next_link)

                ## get remaining rate limit
                #rate_limit = int(page.headers['x-ratelimit-remaining'])

                #print '%s :: %d (%d)' % (next_link,rate_limit,page.status_code)

                ## get data, insert repo identifier
                ##for issue in page.json():
                    ##issue['__full_name__'] = itemid
                    ##issues.append(issue)
                ##issues.extend(pagedata)
                #issues.extend(page.json())

                ## look for pagination
                #next_link = next_page(page)

                #if next_link:
                    #logging.info('%s has more pages', itemid)
                #else:
                    #retrieving = False

                ## wait while rate-limited
                #if rate_limit == 0:
                    #rate_limit_pause(token)

            ## write out user repos
            #with codecs.open(
                    #expanduser('~/Data/github/repo-issues/%0.8d.json' % counter),
                    #mode='w',
                    #encoding='utf8') as fout:
                #fout.write('%s\n' % itemid)
                #for issue in issues:
                    #fout.write(json.dumps(issue) + '\n')

            ## finalize the transaction
            #counter += 1
            #with open(STATE_FILE,mode='w') as fout:
                #fout.write("%d\n" % counter)

def main(umax=5):
    def repo_func(line):
        jsonitem = json.loads(line)
        res = {'id' : jsonitem['full_name'],
                'link' : '%s/issues?state=all&per_page=100' % jsonitem['url']}
        return res

    ghp.yank_driver(item_file=REPO_FILE,
            item_func=repo_func,
            out_base='~/Data/github/repo-issues',
            itemmax=umax,
            token_file='~/Data/github/token',
            state_file=STATE_FILE)

if __name__=="__main__":
    logging.basicConfig(filename='yank-repo-issues.log',
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            level=logging.INFO)

    main(umax=435792)

    logging.shutdown()



