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

print(pymongo.version)

"""
select count(*) from Object;
"""

if __name__ == '__main__':
    client = pymongo.MongoClient(MONGO_URL)
    lsst = client.lsst

    #---------------------------------------------------------------------------------
    dataset = lsst.Object

    stepper = st.Stepper()

    try:
        result = dataset.count()
        print(result)
    except:
        print('error')

    stepper.show_step('select count(*) from Object')

    #---------------------------------------------------------------------------------
    dataset = lsst.Source

    stepper = st.Stepper()

    try:
        result = dataset.count()
        print(result)
    except:
        print('error')

    stepper.show_step('select count(*) from Source')

    #---------------------------------------------------------------------------------
    dataset = lsst.ForcedSource

    stepper = st.Stepper()

    try:
        result = dataset.count()
        print(result)
    except:
        print('error')

    stepper.show_step('select count(*) from ForcedSource')

    #---------------------------------------------------------------------------------
    dataset = lsst.Object

    stepper = st.Stepper()

    id = 2322374716295173
    try:
        result = dataset.find( {'deepSourceId': id}, {'_id': 0, 'ra': 1, 'decl': 1})
        for o in result:
            print(o)
    except:
        print('error')

    stepper.show_step('SELECT ra, decl FROM Object WHERE deepSourceId = {};'.format(id))

    #---------------------------------------------------------------------------------
    dataset = lsst.Object

    stepper = st.Stepper()

    try:
        result = dataset.find({ "loc": {"$geoWithin":{"$box":[[-103,10.1],[-80.43,30.232]]}} }
        result = dataset.find( {'deepSourceId': id}, {'_id': 0, 'ra': 1, 'decl': 1})
        result = dataset.find({'center': {'$geoWithin': {'$centerSphere': [[cluster.ra(), cluster.dec()], radius]}}}
        for o in result:
            print(o)
    except:
        print('error')

    stepper.show_step('SELECT ra, decl FROM Object WHERE qserv_areaspec_box(0.95, 19.171, 1.0, 19.175);')


