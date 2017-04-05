#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os, glob
import random
import pymongo
import bson
import decimal
import re
from pymongo.errors import BulkWriteError

MONGO_URL = r'mongodb://192.168.56.233:27017'

print(pymongo.version)

"""
{ "_id" : ObjectId("58d9027fa1c96cadd46c471c"), "id" : 1 }
{ "_id" : ObjectId("58d90284a1c96cadd46c471d"), "id" : 2 }
{ "_id" : ObjectId("58d90287a1c96cadd46c471e"), "id" : 3 }
{ "_id" : ObjectId("58d90289a1c96cadd46c471f"), "id" : 4 }
{ "_id" : ObjectId("58d9028ba1c96cadd46c4720"), "id" : 5 }
{ "_id" : ObjectId("58d9028ea1c96cadd46c4721"), "id" : 6 }
{ "_id" : ObjectId("58d90ffd2548868845a6aefd"), "id" : 12, "id2" : 23 }
{ "_id" : ObjectId("58d910012548868845a6aefe"), "id" : 13, "id2" : 23 }
{ "_id" : ObjectId("58d910162548868845a6aeff"), "id" : 14, "id2" : 24 }
{ "_id" : ObjectId("58d9101d2548868845a6af00"), "id" : 15, "id2" : 25 }
"""

if __name__ == '__main__':
    client = pymongo.MongoClient(MONGO_URL)
    lsst = client.lsst

    col = lsst.test

    col.create_index( [ ('id', pymongo.ASCENDING), ('id2', pymongo.ASCENDING) ] , unique=True)

    try:
        status = col.find_one_and_update({'id':134, 'id2':25}, { '$set': {'id':134, 'id2':244466}}, upsert=True)
        print('created')
    except:
        print('object already there')

