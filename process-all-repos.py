import json
import codecs
import sys
from collections import defaultdict
from os.path import expanduser

KEEPKEYS = [
        'private','id','fork',
        'name','full_name','description','url'
    ]
OWNERKEEPKEYS = [
        'url','login','type','id','site_admin','gravatar_id'
    ]

def measure_key_usage(maxfile = 1000, report = True):
    keydict = defaultdict(int)
    ownerkeydict = defaultdict(int)

    for i in range(maxfile):
        with codecs.open(
                expanduser('~/Data/github/all-repos/%0.8d.json' % i),
                mode='r',
                encoding='utf8') as fin:
            jsonlist = json.loads(fin.read())

        for jsonitem in jsonlist:
            for key in jsonitem.keys():
                keydict[key] = keydict[key] + 1
            for key in jsonitem['owner'].keys():
                ownerkeydict[key] = ownerkeydict[key] + 1

        if (i+1) % 500 == 0:
            print (i+1)

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
        with codecs.open(
                expanduser('~/Data/github/all-repos/%0.8d.json' % i),
                mode='r',
                encoding='utf8') as fin:
            jsonlist = json.loads(fin.read())

        for jsonitem in jsonlist:
            jsonout = { key: jsonitem[key] for key in KEEPKEYS}
            jsonout['owner'] = { key: jsonitem['owner'][key] for key in OWNERKEEPKEYS}
            #print jsonout
            if outfile:
                fout.write(json.dumps(jsonout) + '\n')
            else:
                sys.stdout.write(json.dumps(jsonout) + '\n')

        if (i+1) % 500 == 0:
            print (i+1)

    if outfile:
        fout.close

    

    pass

def main():
    # first measure key usage
    maxfile = 50000
    #(keydict,ownerkeydict) = measure_key_usage(maxfile, report=True)

    strip_json(maxfile=maxfile,
            outfile=expanduser('~/Data/github/processed-repos.json'))




if __name__=="__main__":
    main()


