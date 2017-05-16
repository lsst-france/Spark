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

import configure_mongo

client = pymongo.MongoClient(configure_mongo.MONGO_URL)
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


