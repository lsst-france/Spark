
from pymongo import MongoClient

from urllib.parse import quote_plus

uri = r'mongodb://%s:%s@%s' % (quote_plus('lsst'), quote_plus('c.a@lal.200'), '134.158.75.222:27017/lsst')


client = MongoClient(uri)

lsst = client.lsst
dataset = lsst.Object

count = dataset.count()
print('Objects', count)

#  134.158.75.222:27017/lsst --username='lsst' --password='c.a@lal.200'


