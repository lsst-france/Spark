#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os, glob
import random
import pymongo
import bson
import decimal
import re
from pymongo.errors import BulkWriteError

import stepper as st

MONGO_URL = r'mongodb://192.168.56.233:27117'
VIEW = {'_id': 0, 'ra': 1, 'decl': 1, 'loc': 1}

print(pymongo.version)

if __name__ == '__main__':
    client = pymongo.MongoClient(MONGO_URL)
    lsst = client.lsst

    try:
        lsst.drop_collection('y')
        print('y created')
    except:
        pass

    lsst.Object.aggregate( [ {'$match' : {'chunkId': 516} }, { '$project': { 'loc': [ '$ra', '$decl' ] } }, {'$limit': 1000}, {'$out': 'y'} ] )
    print(lsst.y.count())

    try:
        lsst.y.create_index([('loc', pymongo.GEO2D)])
    except pymongo.errors.PyMongoError as e:
        print('error create_geo_index', e)

    result = lsst.y.aggregate( [  
        {'$lookup': {'from':'y', 'localField':'y.loc', 'foreignField':'y.loc', 'as':'ns'} },
        {'$unwind': '$ns'},
        {'$project': {'_id': 0, 'loc':1, 'ns.loc':1}},
        {'$limit':10} ] )


    for o in result:
        print(o)

