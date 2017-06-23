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

def do_select(lsst, limit, ra, decl, window):

    try:
        lsst.drop_collection('z')
        print('z dropped')
    except:
        pass

    ext = window
    bottomleft = [ ra - ext, decl - ext ]
    topright = [ ra + ext, decl + ext ]

    p = [
        {'$geoNear':
            {
                'near': [ra, decl],
                'query': { 'loc': { '$geoWithin': {'$box': [bottomleft, topright] } } },
                'limit': limit,
                'distanceField': 'dist',
            }
        },
        {'$project': {'_id':1, 'ra':1, 'decl':1} },
        {'$out': 'z'},
    ]

    stepper = st.Stepper()
    result = lsst.Object.aggregate(p, allowDiskUse=True)
    t = stepper.show_step('aggregate create z')

    c = lsst.z.count()
    print('Objects in the selection:', c, 'window:', bottomleft, topright)

    return bottomleft, topright, t, c


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-R', '--ra', type=float, default=1)
    parser.add_argument('-D', '--decl', type=float, default=1)

    parser.add_argument('-w', '--window', type=float, default=1)
    parser.add_argument('-d', '--dist', type=float, default=0.1)

    args = parser.parse_args()

    client = pymongo.MongoClient(configure_mongo.MONGO_URL)
    lsst = client.lsst

    nobjects = lsst.Object.count()

    print('nobjects=', nobjects, 'window=', args.window, 'distance max=', args.dist)

    bottomleft, topright, t1, c1 = do_select(lsst, nobjects, args.ra, args.decl, args.window)
    # t2, c2 = do_join(lsst, nobjects, bottomleft, topright, dist)

    zcount = lsst.z.count()

    stepper = st.Stepper()
    try:
        lsst.z.create_index([('loc', pymongo.GEO2D)])
    except pymongo.errors.PyMongoError as e:
        print('error create_geo_index', e)
    stepper.show_step('index loc creation')

    stepper = st.Stepper()

    result = lsst.z.find( {}, {'_id':1, 'ra':1, 'decl':1} )

    total = 0
    i = 0
    for i, o in enumerate(result):
        # print("object #", i, "=", o)

        # if i > 10:
        #    break

        id = o['_id']
        ra = o['ra'] 
        decl = o['decl']

        p = [
          { '$geoNear':
            {
                'near': [ra, decl],
                'maxDistance': args.dist,
                'limit': zcount,
                'distanceField': 'dist',
            }
          },
          {'$match': { '_id': { '$lte': id} } },
        ]

        result = lsst.Object.aggregate(p, allowDiskUse=True)

        sub = 0
        for j, o in enumerate(result):
            sub += 1
            total += 1
            # print(j, 'from object', id, ' to object', o['_id'], ' ra=', o['ra'], ' decl=', o['decl'])
        if sub > 0:
            # print(i, sub)
            pass

    stepper.show_step('get neighbours')
    print(total, 'neighbours')


