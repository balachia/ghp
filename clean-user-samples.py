import codecs
from os.path import expanduser

# the first batch of downloads didn't include the owner's name
# as the first line of the file.
# here, we insert that name back in.
with open(expanduser('~/Data/github/owner-sample-order.csv')) as fin:
    for i in range(1,15001):
        with codecs.open(expanduser('~/Data/github/user-repos/%0.8d.json' % i),
                mode='r',
                encoding='utf8') as jin:
            text = jin.read()
        with codecs.open(expanduser('~/Data/github/user-repos/%0.8d.json.bak' % i),
                mode='w',
                encoding='utf8') as fout:
            fout.write(fin.readline())
            fout.write(text)


