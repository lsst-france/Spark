#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os, glob
import random
import pymongo
import bson
import decimal
import re
import argparse
from bson.objectid import ObjectId

from pymongo.errors import BulkWriteError

import stepper as st

import configure_mongo

VIEW = {'_id': 0, 'ra': 1, 'decl': 1, 'loc': 1}

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

def do_create(lsst, nobjects=1000):
    try:
        lsst.drop_collection('y')
        print('y dropped')
    except:
        pass


    ra = 0
    decl = 0
    window = 180.

    stepper = st.Stepper()
    requests = []
    for i in range(nobjects):
        obj = {'loc': [ (random.random()*2*window - window), (random.random()*2*window - window) ] }
        # lsst.y.insert( obj )
        requests.append(pymongo.InsertOne(obj))

    try:
        lsst.y.bulk_write(requests)
    except BulkWriteError as bwe:
        print('error in bulk write', bwe.details)
        exit()

    stepper.show_step('y created')

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
    stepper.show_step('index loc.0 creation')

    stepper = st.Stepper()
    try:
        lsst.y.create_index([('loc.1', pymongo.ASCENDING)])
    except pymongo.errors.PyMongoError as e:
        print('error create index on decl', e)
    stepper.show_step('index loc.1 creation')

    stepper = st.Stepper()
    try:
        lsst.y.create_index([('loc', pymongo.GEO2D)])
    except pymongo.errors.PyMongoError as e:
        print('error create_geo_index', e)
    stepper.show_step('index loc creation')

    test9(lsst.y)

def do_select(lsst, limit, window):
    ra = 0.
    decl = 0.
    ext = window
    bottomleft = [ ra - ext, decl - ext ]
    topright = [ ra + ext, decl + ext ]

    try:
        lsst.drop_collection('z')
        print('z dropped')
    except:
        pass


    p1 = [
        {'$geoNear':
            {
                'near': [0, 0],
                'query': { 'loc': { '$geoWithin': {'$box': [bottomleft, topright] } } },
                'limit': limit,
                'distanceField': 'dist',
            }
        },
        {'$out': 'z'},
    ]

    stepper = st.Stepper()
    result = lsst.y.aggregate(p1, allowDiskUse=True)
    t = stepper.show_step('aggregate create z')

    c = lsst.z.count()
    print('Objects in the selection:', c, 'window:', bottomleft, topright)

    return bottomleft, topright, t, c 

def do_join(lsst, nobjects, bottomleft, topright, max_dist):
    dra =    { '$abs': {'$subtract': [ {'$arrayElemAt': ['$ns.loc', 0]}, {'$arrayElemAt': ['$loc', 0]}] } }
    dra2 =   { '$multiply': [dra, dra] }

    ddecl =  { '$abs': {'$subtract': [ {'$arrayElemAt': ['$ns.loc', 1]}, {'$arrayElemAt': ['$loc', 1]}] } }
    ddecl2 = { '$multiply': [ddecl, ddecl] }

    dist =   { '$sqrt':  { '$add': [ dra2, ddecl2] } }

    """
quand on rencontre [$_id1, $ns._id2] est-ce qu'il existe le couple [$_id2, $ns._id1] ?
    """

    p2 = [
        {'$geoNear':
            {
                'near': [0, 0],
                'query': { 'loc': { '$geoWithin': {'$box': [bottomleft, topright] }  } },
                'limit': nobjects,
                'distanceField': 'dist',
            }
        },
        {'$lookup': {'from':'z', 'localField':'y.loc', 'foreignField':'z.loc', 'as':'ns'} },
        {'$unwind': '$ns'},
        {'$redact': { '$cond': [{ '$eq': ["$_id", "$ns._id"] }, "$$PRUNE", "$$KEEP" ] } },
        # {'$redact': { '$cond': [{ '$eq': ["$_id", "$ns._id"] }, "$$PRUNE", "$$KEEP" ] } },
        {'$addFields': {'dist': dist} },
        # {'$match': { '$and': [ { 'dist': { '$gt': 0 } }, { 'dist': { '$lt': max_dist } } ] } },
        # {'$project': {'_id': 1, 'loc':1, 'ns.loc':1, 'dist': 1}},
        {'$project': {'_id': 1, 'ns._id':1, 'dist':1}},
        # {'$project': {'_id': 1}},
        # {'$sort': {'dist': 1 } }
        # {'$limit': 5},
    ]

    stepper = st.Stepper()
    result = lsst.y.aggregate(p2, allowDiskUse=True)
    t = stepper.show_step('aggregate join')

    i = 0
    for i, o in enumerate(result):
        print(i, o)

    print(i, 'neighbours')

    return t, i



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--create', action="store_true")
    parser.add_argument('-n', '--nobjects', type=int, default=1000)
    parser.add_argument('-w', '--window', type=float, default=1)
    parser.add_argument('-d', '--dist', type=float, default=0.1)
    parser.add_argument('-s', '--step', type=float, default=0.1)

    args = parser.parse_args()

    client = pymongo.MongoClient(configure_mongo.MONGO_URL)
    lsst = client.lsst

    create = args.create

    if create:
        print('create nobjects=', args.nobjects)
        do_create(lsst, nobjects=args.nobjects)
        nobjects = args.nobjects
    else:
        nobjects = lsst.y.count()

    dist0 = args.dist
    steps = 1

    dist = dist0

    for step in range(steps):
        print('nobjects=', nobjects, 'window=', args.window, 'distance=', dist)

        bottomleft, topright, t1, c1 = do_select(lsst, nobjects, args.window)
        t2, c2 = do_join(lsst, nobjects, bottomleft, topright, dist)

        print('results {:.3f} {} {:.3f} {}'.format(t1, c1, t2, c2))

        dist += args.step
