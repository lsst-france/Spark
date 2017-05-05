#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os, glob
import random
import pymongo
import bson
import decimal
import re
from pymongo.errors import BulkWriteError
from bson.objectid import ObjectId


GALACTICA = False
WINDOWS = False
LAL = True
ATLAS = False

if GALACTICA:
    MONGO_URL = r'mongodb://192.168.56.233:27117'
elif WINDOWS:
    MONGO_URL = r'mongodb://localhost:27017'
elif LAL:
    MONGO_URL = r'mongodb://134.158.75.222:27017'
elif ATLAS:
    MONGO_URL = r'mongodb://arnault:arnault7977$@cluster0-shard-00-00-wd0pq.mongodb.net:27017,cluster0-shard-00-01-wd0pq.mongodb.net:27017,cluster0-shard-00-02-wd0pq.mongodb.net:27017/<DATABASE>?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin'


client = pymongo.MongoClient(MONGO_URL)
y = client.lsst.y


print('find') 

result = y.find({ '_id': ObjectId("5908e63cd15fa104356eaf64") }, {'_id':1})

for i, o in enumerate(result):
    print(i, o)

print('aggregate match direct')

result = y.aggregate( [ {'$match': {'_id': ObjectId("5908e63cd15fa104356eaf64") } }, {'$project': {'_id': 1} } ] )

for i, o in enumerate(result):
    print(i, o)

print('aggregate match with $eq')

result = y.aggregate( [ {'$match': {'_id': {'$eq': ObjectId("5908e63cd15fa104356eaf64") } } }, {'$project': {'_id': 1} } ] )

for i, o in enumerate(result):
    print(i, o)

print('aggregate match with $ne')

pipeline = [ {'$match': {'_id': {'$ne': ObjectId("5908e63cd15fa104356eaf64") } } }, {'$project': {'_id': 1} }, {'$limit': 5} ] 

result = y.aggregate(pipeline)
# result = client.lsst.command('aggregate', 'y', pipeline=pipeline  , explain=True)

for i, o in enumerate(result):
    print(i, o)


