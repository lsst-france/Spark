#!/USR/bin/env python
# -*- coding: utf-8 -*-

import random
import pymongo
import bson
import decimal
from pymongo.errors import BulkWriteError
import stepper as st

import configure_mongo

VIEW = {'_id':0, 'ra': 1, 'decl': 1, 'loc': 1}

def aggregate(dataset, col):
    stepper = st.Stepper()

    try:
        dataset.aggregate( [ { '$addFields': { 'loc': ['$ra', '$decl'] } }, {'$out': col} ] )
    except:
        print('error aggregate')

    stepper.show_step('aggregate')

def create_geo_index(dataset):
    stepper = st.Stepper()

    try:
        dataset.create_index([('loc', pymongo.GEO2D)])
        # dataset.create_index( {'loc': 1}, '2d' )
    except pymongo.errors.PyMongoError as e:
        print('error create_geo_index', e)

    stepper.show_step('create_geo_index')
    infos = dataset.index_information()

    for i in infos:
        print('  > ', i, ': ',  infos[i])

def creation(geo, objects=100):

    stepper = st.Stepper()

    for i in range(objects):
        obj = dict()

        obj['ra'] = random.random()*360.0
        obj['decl'] = random.random()*360.0
        obj['deepSourceId'] = int(random.random()*100000000)

        try:
            geo.insert_one(obj)
            if i < 5:
                print('object created', obj)
        except:
            print('error')

    stepper.show_step('objects created')

def try_geo(lsst, objects=10):

    print('============================================ try_geo')

    recreate = True

    if recreate:
        try:
            lsst.drop_collection('geo')
            print('geo created')
        except:
            pass

        geo = lsst.geo
        creation(geo, objects)
        aggregate(geo, 'geo')

        geo.update_many({}, { '$inc': {'loc.0': -180, 'loc.1': -180} })

        stepper = st.Stepper()
        geo.create_index('deepSourceId', unique=True)
        stepper.show_step('index created' + str(geo.index_information()))

        # create_geo_index(geo)

    else:

        geo = lsst.geo

    # stepper = Stepper()

    result = geo.find({}, VIEW )
    for i, o in enumerate(result):
        print('object read', o)
        if i > 5:
            break

    ra = 45.
    decl = 0.
    ext = 10.
    bottomleft = [ ra - ext, decl - ext ]
    topright = [ ra + ext, decl + ext ]

    stepper = st.Stepper()
    result = geo.find( { 'loc': { '$geoWithin': {'$box': [bottomleft, topright] }  } }, VIEW )
    stepper.show_step('geo find')

    for i, o in enumerate(result):
        print(o)
        if i > 5:
            break


def prepare_Object(lsst):

    print('============================================ prepare_Object')

    dataset = lsst.Object

    result = dataset.count()
    print(result)

    # aggregate(dataset, 'Object')

    # stepper = st.Stepper()
    # dataset.update_many({}, { '$inc': {'loc.0': -180} })
    # stepper.show_step('convert RA')

    result = dataset.find( {}, VIEW )
    for i, o in enumerate(result):
        if (i % 10000) == 0:
            print('object read', o)

    # create_geo_index(dataset)


if __name__ == '__main__':

    geo = None
    client = pymongo.MongoClient(configure_mongo.MONGO_URL)
    lsst = client.lsst

    # try_geo(lsst, 1000)

    prepare_Object(lsst)

