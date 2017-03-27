
import glob
import re


def count(file_name):
    with open(file_name) as f:
        return sum(1 for _ in f)

p = '/mnt/volume/dataset/'

class Dataset(object):
    def __init__(self):
        self.schemas = dict()

datasets = dict()

for file_name in glob.glob(p + '*'):
    name = file_name.split('/')[-1]
    m = re.match('([^_]+)[_](\d+)[.]csv', name)
    try:
        schema = m.group(1)
        chunk = int(m.group(2))
    except:
        continue

    # c = count(file_name)
    c = 0

    if chunk in datasets:
        ds = datasets[chunk]
        ds.schemas[schema] = c
    else:
        ds = Dataset()
        ds.schemas[schema] = c

    datasets[chunk] = ds

    # print(chunk, sorted(ds.schemas.keys()))


for chunk in sorted(datasets.keys()):
    ds = datasets[chunk]
    print('Chunk {:05d} dbs: {} {}'.format(chunk, len(ds.schemas.keys()), ', '.join(sorted(ds.schemas.keys()))))


