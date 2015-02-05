import ujson as json
import codecs
import sys
from collections import defaultdict
from os.path import expanduser

KEEPKEYS = [
        'has_wiki','updated_at','private','full_name',
        'id','size','has_downloads','has_pages',
        'watchers_count','stargazers_count','permissions','homepage',
        'fork','description','forks','has_issues',
        'open_issues_count','watchers','name','language',
        'url','created_at','pushed_at','forks_count',
        'default_branch','open_issues'
    ]
OWNERKEEPKEYS = [
        'url','login','type','id','site_admin','gravatar_id'
    ]

#@profile
def measure_key_usage(maxfile = 1000, report = True):
    keydict = defaultdict(int)
    ownerkeydict = defaultdict(int)

    keys = []
    okeys = []
    for i in range(maxfile):
        i += 1
        with codecs.open(
                expanduser('~/Data/github/user-repos/%0.8d.json' % i),
                mode='r',
                encoding='utf8') as fin:
            for line in fin:
                jsonitem = json.loads(line)

                try:
                    newkeys = jsonitem.keys()
                    newokeys = jsonitem['owner'].keys()
                except:
                    print "ERROR IN %d: %s" % (i, line.strip())

                if keys != newkeys:
                    keys = newkeys
                    print "Key break:\n%s" % keys
                if okeys != newokeys:
                    okeys = newokeys
                    print "Owner key break:\n%s" % okeys

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

    return (keydict, ownerkeydict)

def strip_json(maxfile = 1000, outfile = None):
    if outfile:
        fout = open(outfile,mode='w')
    
    for i in range(maxfile):
        i += 1
        with codecs.open(
                expanduser('~/Data/github/user-repos/%0.8d.json' % i),
                mode='r',
                encoding='utf8') as fin:
            for line in fin:
                jsonitem = json.loads(line)

                try:
                    jsonout = { key: jsonitem[key] for key in KEEPKEYS}
                    jsonout['owner'] = { key: jsonitem['owner'][key] for key in OWNERKEEPKEYS}
                    #print jsonout
                    if outfile:
                        fout.write(json.dumps(jsonout) + '\n')
                    else:
                        sys.stdout.write(json.dumps(jsonout) + '\n')
                except:
                    print "ERROR IN %d: %s" % (i, line.strip())

        if i % 500 == 0:
            print i

    if outfile:
        fout.close

def main(umax=1000):
    #measure_key_usage(maxfile=umax)

    strip_json(maxfile=umax,
            outfile=expanduser('~/Data/github/processed_user_samples.json'))


if __name__=="__main__":
    main(umax=15000)


