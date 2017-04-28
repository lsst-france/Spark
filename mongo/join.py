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

GALACTICA = False
WINDOWS = False
LAL = False
ATLAS = True

if GALACTICA:
    MONGO_URL = r'mongodb://192.168.56.233:27117'
elif WINDOWS:
    MONGO_URL = r'mongodb://localhost:27017'
elif LAL:
    MONGO_URL = r'mongodb://134.158.75.222:27017'
elif ATLAS:
    MONGO_URL = r'mongodb://arnault:arnault7977$@cluster0-shard-00-00-wd0pq.mongodb.net:27017,cluster0-shard-00-01-wd0pq.mongodb.net:27017,cluster0-shard-00-02-wd0pq.mongodb.net:27017/<DATABASE>?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin'

VIEW = {'_id': 0, 'ra': 1, 'decl': 1, 'loc': 1}

print(pymongo.version)

def test9(dataset):
    stepper = st.Stepper()

    try:
        min_ra = dataset.find( {}, {'_id':0, 'loc':1}).sort( 'loc.0', 1 ).limit(1)[0]['loc'][0]
        max_ra = dataset.find( {}, {'_id':0, 'loc':1}).sort( 'loc.0', -1 ).limit(1)[0]['loc'][0]
        min_decl = dataset.find( {}, {'_id':0, 'loc':1}).sort( 'loc.1', 1 ).limit(1)[0]['loc'][1]
        max_decl = dataset.find( {}, {'_id':0, 'loc':1}).sort( 'loc.1', -1 ).limit(1)[0]['loc'][1]
    except pymongo.errors.PyMongoError as e:
        print('error min, max', e)

    print('ra= [', min_ra, ',', max_ra, ']')
    print('decl= [', min_decl, ',', max_decl, ']')

    stepper.show_step('select min(ra), max(ra), min(decl), max(decl) from Object;')

if __name__ == '__main__':
    client = pymongo.MongoClient(MONGO_URL)
    lsst = client.lsst

    try:
        lsst.drop_collection('y')
        print('y created')
    except:
        pass


    ra = 0
    decl = 0
    window = 180.

    stepper = st.Stepper()
    requests = []
    for i in range(10000):
        obj = {'loc': [ (random.random()*2*window - window), (random.random()*2*window - window) ] }
        # lsst.y.insert( obj )
        requests.append(pymongo.InsertOne(obj))

    try:
        lsst.y.bulk_write(requests)
    except BulkWriteError as bwe:
        print('error in bulk write', bwe.details)
        exit()

    stepper.show_step('creation')

    # lsst.Object.aggregate( [ {'$match' : {'chunkId': 516} }, { '$project': { 'loc': [ '$ra', '$decl' ] } }, {'$limit': 1000}, {'$out': 'y'} ] )
    print(lsst.y.count())

    result = lsst.y.find()
    for i, o in enumerate(result):
        print(o)
        if i > 10:
            break

    stepper = st.Stepper()
    try:
        lsst.y.create_index([('loc.0', pymongo.ASCENDING)])
    except pymongo.errors.PyMongoError as e:
        print('error create index on ra', e)
    stepper.show_step('index creation')

    stepper = st.Stepper()
    try:
        lsst.y.create_index([('loc.1', pymongo.ASCENDING)])
    except pymongo.errors.PyMongoError as e:
        print('error create index on decl', e)
    stepper.show_step('index creation')

    stepper = st.Stepper()
    try:
        lsst.y.create_index([('loc', pymongo.GEO2D)])
    except pymongo.errors.PyMongoError as e:
        print('error create_geo_index', e)
    stepper.show_step('index creation')

    test9(lsst.y)

    dra =    { '$abs': {'$subtract': [ {'$arrayElemAt': ['$ns.loc', 0]}, {'$arrayElemAt': ['$loc', 0]}] } }
    dra2 =   { '$multiply': [dra, dra] }

    ddecl =  { '$abs': {'$subtract': [ {'$arrayElemAt': ['$ns.loc', 1]}, {'$arrayElemAt': ['$loc', 1]}] } }
    ddecl2 = { '$multiply': [ddecl, ddecl] }

    dist =   { '$sqrt':  { '$add': [ dra2, ddecl2] } }


    ra = 0.
    decl = 0.
    ext = 10.
    bottomleft = [ ra - ext, decl - ext ]
    topright = [ ra + ext, decl + ext ]


    stepper = st.Stepper()

    result = lsst.y.aggregate( [
        {'$geoNear': {
            'near': [0, 0],
            'query': { 'loc': { '$geoWithin': {'$box': [bottomleft, topright] }  } },
            'distanceField': 'dist',
        } },
        {'$lookup': {'from':'y', 'localField':'y.loc', 'foreignField':'y.loc', 'as':'ns'} },
        {'$unwind': '$ns'},
        # {'$addFields': {'dra':dra, 'dra2': dra2, 'ddecl':ddecl, 'ddecl2': ddecl2, 'dist': dist} },
        {'$addFields': {'dist': dist} },
        {'$match': { '$and': [ { 'dist': { '$gt': 0 } }, { 'dist': { '$lt': 1 } } ] } },
        # {'$project': {'_id': 0, 'loc':1, 'ns.loc':1, 'dra': 1, 'ddecl': 1, 'dist': 1}},
        {'$project': {'_id': 0, 'loc':1, 'ns.loc':1, 'dist': 1}},
        # {'$sort': {'dist': 1 } }
        # {'$limit':10},
        # {'$count': 'objects'},
    ] )
    stepper.show_step('aggregate')

    # print(result)

    for i, o in enumerate(result):
        print(i, o)

