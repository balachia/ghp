import ujson as json
import requests
import re
import codecs
import logging
from os.path import expanduser, isfile
from time import sleep, time, ctime, timezone
from Queue import Queue, Empty
from threading import Thread
#import Queue as basequeue
#from multiprocessing import Process, Queue

#logging.basicConfig(filename='yank-repo-issues.log',level=logging.INFO)

_LINKRE = re.compile(r'(?:<(.*?)>; rel="(.*?)")+')

class YankProblem:
    def __init__(self):
        pass

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
                    auth=(token,''),
                    timeout=1)

            # issues
            if not page.ok:
                bad_request_count += 1
                logging.error('Bad request %d (%d): <%s>',
                        bad_request_count,
                        page.status_code,
                        link)
                # check for rate limit
                rate_limit = int(page.headers['x-ratelimit-remaining'])
                # wait while rate-limited
                if rate_limit == 0:
                    rate_limit_pause(token)
            else:
                need_request = False
                logging.info('(%d): <%s>', page.status_code, link)
        except requests.exceptions.Timeout as err:
            logging.error('Timeout at <%s>: %s',
                    link,
                    err)
            print 'Retry on:\n%s' % str(err)
        except requests.ConnectionError as err:
            logging.error('Connection error at <%s>: %s',
                    link,
                    err)
            print 'Retry on:\n%s' % str(err)

    return page

def next_page(page):
    """
    Find next page link from the current page, if possible.
    """
    if 'link' in page.headers:
        link = page.headers['link']
        matches = _LINKRE.findall(link)
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
        page = None
        while not page:
            try:
                page = requests.get('https://api.github.com/rate_limit',
                        auth=(token,''),
                        timeout=1)
            except requests.exceptions.Timeout as err:
                logging.error('Timeout at rate limiter: %s',
                        err)
        rate_limit = int(page.headers['x-ratelimit-remaining'])
        rate_reset = int(page.headers['x-ratelimit-reset'])
        wait_time = rate_reset - int(time())
        if rate_limit == 0:
            print "Rate limited, reset %s, sleeping %d" % (ctime(rate_reset), wait_time)
            sleep(wait_time + 1)

def get_item(item, token):
    """
    Yank a few-page item.
    """
    itemid = item['id']
    next_link = item['link']

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

    return itemjsons

def write_item(itemjsons, outfile, mode='w'):
    """
    Write out item jsons, one per line.
    """
    # write out item jsons
    with codecs.open(
            #expanduser('%s/%0.8d.json' % (out_base, counter)),
            outfile,
            mode=mode,
            encoding='utf8') as fout:
        for itemjson in itemjsons:
            fout.write(json.dumps(itemjson) + '\n')

def yank_process(itemqueue, writequeue, token):
    rate_limit_pause(token)

    has_queued = True
    while has_queued:
        try:
            (counter, item) = itemqueue.get_nowait()
        except Empty:
            # no more stuff
            has_queued = False
            continue
        #(counter, item) = itemqueue.get()

        itemid = item['id']
        next_link = item['link']

        print "Item %d <%s>" % (counter, itemid)

        itemjsons = get_item(item, token)

        writequeue.put((counter, itemid, itemjsons))

        itemqueue.task_done()

    logging.info('Yank process completed queue')


def write_process(writequeue, donequeue, outbase):
    waiting = True
    while waiting:
        # block until item receipt
        # die on special item (counter = None)
        (counter, itemid, itemjsons) = writequeue.get()
        if not counter:
            logging.info('Write process completed')
            waiting = False
            continue
        logging.info('Write process retrieved %s, (%d)',
                itemid, counter)

        outfile = expanduser('%s/%0.8d.json' % (outbase, counter))
        with codecs.open(outfile, mode='w', encoding='utf8') as fout:
            fout.write('%s\n' % itemid)
        write_item(itemjsons, outfile, mode='a')

        donequeue.put(counter)
        writequeue.task_done()



def state_process(donequeue, statefile, minimum=0):
    minseen = minimum
    waitingline = []

    waiting = True
    while waiting:
        # block until item receipt
        # die on special item (counter = None)
        counter = donequeue.get()
        if not counter:
            if len(waitingline) > 0:
                logging.error('State queue not empty at termination: %s',
                        waitingline)
                print 'State queue not empty at termination: %s' % waitingline
            logging.info('State process completed')
            waiting = False
            continue
        logging.info('State process retrieved %d', counter)
        if waitingline:
            logging.info('State process waiting line (min %d): %s',
                    minseen, waitingline)
        
        # resort the waitingline
        waitingline.append(counter)
        waitingline.sort(reverse=True)

        # write state so long as we have sequential counters
        newminseen = minseen
        while waitingline and waitingline[-1] - newminseen == 1:
            newminseen = waitingline.pop()
        if newminseen > minseen:
            minseen = newminseen
            with open(statefile, mode='w') as fout:
                fout.write("%d\n" % (minseen + 1))

        donequeue.task_done()


def yank_driver(item_file,
        item_func,
        out_base,
        itemmax=5,
        token=None,
        token_file='~/Data/github/token',
        state_file=None,
        thread_per_token=1):
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
            tokens = [line.strip() for line in fin]
            tokens = tokens * thread_per_token
            #token = fin.read().strip()
    else:
        tokens = token * thread_per_token

    print '{0} tokens, {1} threads each'.format(
            len(tokens) / thread_per_token, thread_per_token)

    # pull state
    skip = 0
    if state_file and isfile(state_file):
        with open(state_file, mode='r') as fin:
            text = fin.read().split()
            if text:
                skip = int(text[0]) - 1
                print 'skipping %d' % skip

    itemqueue = Queue()
    writequeue = Queue()
    donequeue = Queue()
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
            itemqueue.put((counter, item))
            counter += 1

    # start processes
    #stateproc = Process(target=state_process,
    stateproc = Thread(target=state_process,
            name='STATE-PROCESS',
            kwargs={'donequeue': donequeue,
                'statefile': state_file,
                'minimum': skip})
    stateproc.daemon = True
    stateproc.start()
    #writeproc = Process(target=write_process,
    writeproc = Thread(target=write_process,
            name='WRITE-PROCESS',
            kwargs={'writequeue':writequeue,
                'donequeue':donequeue,
                'outbase':out_base})
    writeproc.daemon = True
    writeproc.start()

    # yank processes
    yankprocs = []
    yankprocid = 1
    for tkn in tokens:
        #yankprocs.append(Process(target=yank_process,
        yankprocs.append(Thread(target=yank_process,
            name='YANK-PROCESS-%d' % yankprocid,
            kwargs={'itemqueue':itemqueue,
                'writequeue':writequeue,
                'token':tkn}))
        yankprocid += 1
    for yankproc in yankprocs:
        yankproc.daemon = True
        yankproc.start()

    # now wait for them to finish...
    print('Waiting on yank processes')
    #itemqueue.join()
    # no-timeout join prevents keyboard interrupt
    # V('.')V
    for yankproc in yankprocs:
        while yankproc.is_alive():
            yankproc.join(3600)

    print('Yank processes done. Waiting on write process.')
    #writequeue.join()
    writequeue.put((None,None,None))
    writeproc.join()

    print('Write process done. Waiting on state process.')
    #donequeue.join()
    donequeue.put(None)
    stateproc.join()


    # can probably do this at a more opportune time,
    # that lets me skip through skipped users
    #rate_limit_pause(token)

    #has_queued = True
    #while has_queued:
        #try:
            #(counter,item) = itemqueue.get_nowait()
        #except basequeue.Empty:
            ## no more stuff
            #has_queued = False
            #continue

        #itemid = item['id']
        #next_link = item['link']

        #print "Item %d <%s>" % (counter, itemid)

        #itemjsons = get_item(item, token)

        #outfile = expanduser('%s/%0.8d.json' % (out_base, counter))
        #with codecs.open(outfile, mode='w', encoding='utf8') as fout:
            #fout.write('%s\n' % itemid)
        #write_item(itemjsons, outfile, mode='a')

        ## finalize the transaction
        #counter += 1
        #with open(state_file, mode='w') as fout:
            #fout.write("%d\n" % counter)






            # pull all item pages, lumping jsons
            #retrieving = True
            #itemjsons = []
            #while retrieving:
                #page = get_page(next_link, token, bad_request_max=5)

                ## get remaining rate limit
                #rate_limit = int(page.headers['x-ratelimit-remaining'])

                #print('%s :: %d (%d)' % 
                        #(next_link,rate_limit,page.status_code))

                ## append current page of shit
                #itemjsons.extend(page.json())

                ## look for pagination
                #next_link = next_page(page)

                #if next_link:
                    #logging.info('%s has more pages', itemid)
                #else:
                    #retrieving = False

                ## wait while rate-limited
                #if rate_limit == 0:
                    #rate_limit_pause(token)

            # write out item jsons
            #with codecs.open(
                    #expanduser('%s/%0.8d.json' % (out_base, counter)),
                    #mode='w',
                    #encoding='utf8') as fout:
                #fout.write('%s\n' % itemid)
                #for itemjson in itemjsons:
                    #fout.write(json.dumps(itemjson) + '\n')
