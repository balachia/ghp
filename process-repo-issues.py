import ujson as json
import codecs
import sys
import re
from collections import defaultdict
from os.path import expanduser

NAME_RE = re.compile(r'https://api.github.com/repos/(.+/.+)/issues/.*')

KEEPKEYS = [
        'body','locked','title','url',
        'created_at','labels','comments','number',
        'updated_at','assignee','state','user',
        'milestone','closed_at','id','pull_request'
    ]

def measure_key_usage(maxfile = 1000, report = False):
    keydict = defaultdict(int)
    ownerkeydict = defaultdict(int)

    keys = []
    okeys = []
    allkeys = []
    allokeys = []
    for i in range(maxfile):
        i += 1
        with codecs.open(
                expanduser('~/Data/github/repo-issues/%0.8d.json' % i),
                mode='r',
                encoding='utf8') as fin:
            reponame = fin.readline().strip()
            for line in fin:
                jsonitem = json.loads(line)

                # verify that repo names match
                try:
                    jsonname = NAME_RE.match(jsonitem['url']).group(1)
                    if jsonname != reponame:
                        print '%s != %s' % (jsonname, reponame)
                except:
                    print "ERROR IN %d (%s): %s" % (i, reponame, line.strip())

                try:
                    newkeys = jsonitem.keys()
                    newokeys = jsonitem['user'].keys()
                except:
                    print "ERROR IN %d (%s): %s" % (i, reponame, line.strip())

                if keys != newkeys:
                    keys = newkeys
                    if keys not in allkeys:
                        allkeys.append(keys)
                        print "Key break at %d" % i
                        print keys
                if okeys != newokeys:
                    okeys = newokeys
                    if okeys not in allokeys:
                        allokeys.append(okeys)
                        print "User key break at %d" % i
                        print okeys

        if i % 500 == 0:
            print i

    if report:
        badkeys = [(k,v) for (k,v) in keydict.items() if v != maxfile * 100]
        badkeys.extend([(k + ' (OWNER)',v) for (k,v) in ownerkeydict.items() if v != maxfile * 100])
        if badkeys:
            print badkeys
        else:
            print "All keys good"

        print keydict
        print ownerkeydict

    print 'keys:'
    for key in allkeys:
        print "%s\n" % key
    print 'okeys:'
    for key in allokeys:
        print "%s\n" % key

    return (allkeys, allokeys)


def strip_json(maxfile = 1000, outfile = None, issuefile=None):
    if outfile:
        fout = open(outfile,mode='w')
    if issuefile:
        issout = open(issuefile,mode='w')

    repos_with_issues = set()
    repos_with_comments = set()
    
    for i in range(maxfile):
        i += 1
        with codecs.open(
                expanduser('~/Data/github/repo-issues/%0.8d.json' % i),
                mode='r',
                encoding='utf8') as fin:
            reponame = fin.readline().strip()
            for line in fin:
                jsonitem = json.loads(line)

                try:
                    jsonout = { key: jsonitem[key] for key in KEEPKEYS if key in jsonitem}
                    #jsonout['owner'] = { key: jsonitem['owner'][key] for key in OWNERKEEPKEYS}
                    #print jsonout
                    if outfile:
                        fout.write(json.dumps(jsonout) + '\n')
                    else:
                        sys.stdout.write(json.dumps(jsonout) + '\n')

                    repos_with_issues.add(reponame)

                    if int(jsonout['comments']) > 0:
                        repos_with_comments.add(reponame)
                        if issuefile:
                            issout.write(json.dumps(jsonout) + '\n')
                        else:
                            sys.stdout.write(json.dumps(jsonout) + '\n')
                except:
                    #print "ERROR IN %d: %s" % (i, line.strip())
                    print "ERROR IN %d (%s): %s" % (i, reponame, line.strip())

        if i % 500 == 0:
            print i

    print 'repos with issues: %d' % len(repos_with_issues)
    print 'repos with comments: %d' % len(repos_with_comments)

    if outfile:
        fout.close
    if issuefile:
        issout.close()


def main(umax=1000, measure=True):
    if measure:
        measure_key_usage(maxfile=umax)
    else:
        strip_json(maxfile=umax,
                outfile=expanduser('~/Data/github/processed_issues.json'),
                issuefile=expanduser('~/Data/github/issue_samples.json'))


if __name__ == "__main__":
    main(umax=66860, measure=False)


