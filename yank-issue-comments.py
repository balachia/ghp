import ujson as json
#import requests
import re
#import codecs
import ghp
import logging
from os.path import expanduser, isfile
from time import sleep, time, ctime, timezone


STATE_FILE = expanduser('~/Data/github/yank-issue-comments.state')
ISSUE_FILE = expanduser('~/Data/github/issue_samples.json')

#LINKRE = re.compile(r'(?:<(.*?)>; rel="(.*?)")+')
NAME_RE = re.compile(r'https://api.github.com/repos/(.+/.+)/issues/.*')

def main(umax=5):
    def issue_func(line):
        jsonitem = json.loads(line)
        res = {'id' : NAME_RE.match(jsonitem['url']).group(1),
               'link' : '%s/comments' % jsonitem['url']}
        return res

    ghp.yank_driver(item_file=ISSUE_FILE,
            item_func=issue_func,
            out_base='~/Data/github/issue-comments',
            itemmax=umax,
            token_file='~/Data/github/token',
            state_file=STATE_FILE)

    pass

if __name__=="__main__":
    logging.basicConfig(filename='yank-issue-comments.log',
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            level=logging.INFO)
    main(umax=50000)
    logging.shutdown()





