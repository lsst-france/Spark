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

import configure_mongo

if __name__ == '__main__':
    client = pymongo.MongoClient(configure_mongo.MONGO_URL)
    lsst = client.lsst

    dra =    { '$abs': {'$subtract': [ '$ns.ra', '$ra' ] } }
    dra2 =   { '$multiply': [dra, dra] }

    ddecl =    { '$abs': {'$subtract': [ '$ns.decl', '$decl' ] } }
    ddecl2 = { '$multiply': [ddecl, ddecl] }

    dist =   { '$sqrt':  { '$add': [ dra2, ddecl2] } }


    ra = 51.0
    decl = -88.0
    ext = 0.05 

    bottomleft = [ ra - ext, decl - ext ]
    topright = [ ra + ext, decl + ext ]


    stepper = st.Stepper()

    result = None
    try:
      result = lsst.Object.aggregate( [
        {'$geoNear': {
            'near': [ra, decl],
            'query': { 'loc': { '$geoWithin': {'$box': [bottomleft, topright] }  } },
            'distanceField': 'dist',
        } },
        {'$lookup': {'from':'Object', 'localField':'Object.loc', 'foreignField':'Object.loc', 'as':'ns'} },
        {'$unwind': '$ns'},
        # {'$addFields': {'dra':dra, 'dra2': dra2, 'ddecl':ddecl, 'ddecl2': ddecl2, 'dist': dist} },
        {'$addFields': {'dist': dist} },
        {'$match': { '$and': [ { 'dist': { '$gt': 0 } }, { 'dist': { '$lt': 0.001 } } ] } },
        # {'$project': {'_id': 0, 'loc':1, 'ns.loc':1, 'dra': 1, 'ddecl': 1, 'dist': 1}},
        {'$project': {'_id': 0, 'loc':1, 'dist': 1}},
        # {'$project': {'_id': 0, 'ra':1, 'decl':1, 'loc':1, 'ns.loc':1}},
        # {'$count': 'objects'}, 
        # {'$group': {'_id': '', 'min_ra': {'$min': '$ra'}, 'max_ra': {'$max': '$ra'}, 'min_decl': {'$min': '$decl'}, 'max_decl': {'$max': '$decl'} } },
        # {'$sort': {'dist': 1 } }
        {'$limit':100},
        {'$count': 'objects'},
      ], allowDiskUse=True )
    except pymongo.errors.PyMongoError as e:
      print(e)

    stepper.show_step('aggregate')

    # print(result)

    if result is not None:
      # print(result.count())
      for i, o in enumerate(result):
        print(i, o)



