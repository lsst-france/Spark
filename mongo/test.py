#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
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


def test1(dataset):
    stepper = st.Stepper()

    try:
        result = dataset.count()
        print(result)
    except pymongo.errors.PyMongoError as e:
        print('error', e)

    stepper.show_step('select count(*) from Object')

def test2(dataset):
    stepper = st.Stepper()

    try:
        result = dataset.count()
        print(result)
    except pymongo.errors.PyMongoError as e:
        print('error', e)

    stepper.show_step('select count(*) from Source')

def test3(dataset):
    stepper = st.Stepper()

    try:
        result = dataset.count()
        print(result)
    except pymongo.errors.PyMongoError as e:
        print('error', e)

    stepper.show_step('select count(*) from ForcedSource')

def test4(dataset):
    stepper = st.Stepper()

    id = 2322374716295173
    try:
        result = dataset.find( {'deepSourceId': id}, VIEW)
        for o in result:
            print(o)
    except pymongo.errors.PyMongoError as e:
        print('error', e)

    stepper.show_step('SELECT ra, decl FROM Object WHERE deepSourceId = {};'.format(id))

def test5(dataset):
    stepper = st.Stepper()

    ra_left = -4.0
    decl_bottom = -3.2
    ra_right = -3.99
    decl_top = -3.16

    try:
        query = { 'loc': { '$geoWithin': {'$box': [ [ra_left, decl_bottom], [ra_right, decl_top]] }  } }
        result = dataset.find( query, VIEW )
        for o in result:
            print(o)
    except pymongo.errors.PyMongoError as e:
        print('error', e)

    stepper.show_step('SELECT ra, decl FROM Object WHERE qserv_areaspec_box({}, {}, {}, {});'.format(ra_left + 180., decl_bottom, ra_right + 180., decl_top))

def test6(dataset):
    stepper = st.Stepper()

    try:
        result = dataset.create_index( [ ('y_instFlux', pymongo.ASCENDING) ] )
    except pymongo.errors.PyMongoError as e:
        print('error create index on y_instFlux', e)

    stepper.show_step('create index on y_instFlux')

def test7(dataset):
    stepper = st.Stepper()

    try:
        filter = { 'y_instFlux': { '$gt': 5.0 } }
        result = dataset.count( filter )
        print(result)
    except pymongo.errors.PyMongoError as e:
        print('error', e)

    stepper.show_step('select count(*) from Object where y_instFlux > 5;')

def test8(dataset):
    stepper = st.Stepper()

    try:
        result = dataset.create_index( [ ('ra', pymongo.ASCENDING) ] )
    except pymongo.errors.PyMongoError as e:
        print('error create index on ra', e)

    try:
        result = dataset.create_index( [ ('decl', pymongo.ASCENDING) ] )
    except pymongo.errors.PyMongoError as e:
        print('error create index on decl', e)

    stepper.show_step('create indexes on ra, decl')

def test9(dataset):
    stepper = st.Stepper()

    try:
        result = dataset.aggregate( [ { '$group': { '_id': '', 'min_ra': { '$min': '$ra' }, 'max_ra': { '$max': '$ra' }, 'min_decl': { '$min': '$decl' }, 'max_decl': { '$max': '$decl' } } } ] )
    except pymongo.errors.PyMongoError as e:
        print('error aggregate', e)

    stepper.show_step('select min(ra), max(ra), min(decl), max(decl) from Object;')

def test10(dataset):
    stepper = st.Stepper()

    try:
        result = dataset.create_index( [ ('flux_sinc', pymongo.ASCENDING) ] )
    except pymongo.errors.PyMongoError as e:
        print('error create index on flux_sinc', e)

    stepper.show_step('create index on flux_sinc')

def test11(dataset):
    stepper = st.Stepper()

    try:
        filter = { '$and': [ { 'flux_sinc': { '$gt': 1 } }, { 'flux_sinc': { '$lt': 2 } } ] }
        result = dataset.count( filter )
        print(result)
    except pymongo.errors.PyMongoError as e:
        print('error', e)

    stepper.show_step('select count(*) from Source where flux_sinc between 1 and 2;')

def test12(dataset):
    stepper = st.Stepper()

    try:
        filter = { '$and': [ { 'flux_sinc': { '$gt': 2 } }, { 'flux_sinc': { '$lt': 3 } } ] }
        result = dataset.count( filter )
        print(result)
    except pymongo.errors.PyMongoError as e:
        print('error', e)

    stepper.show_step('select count(*) from Source where flux_sinc between 2 and 3;')

def test13(dataset):
    stepper = st.Stepper()

    try:
        result = dataset.create_index( [ ('psfFlux', pymongo.ASCENDING) ] )
    except pymongo.errors.PyMongoError as e:
        print('error create index on psfFlux', e)

    stepper.show_step('create index on psfFlux')

def test14(dataset):
    stepper = st.Stepper()

    try:
        filter = { '$and': [ { 'psfFlux': { '$gt': 0.1 } }, { 'psfFlux': { '$lt': 0.2 } } ] }
        result = dataset.count( filter )
        print(result)
    except pymongo.errors.PyMongoError as e:
        print('error', e)

    stepper.show_step('select count(*) from ForcedSource where psfFlux between 0.1 and 0.2;')



if __name__ == '__main__':

    args = len(sys.argv)
    if args < 2:
        print('give the test numbers')
        exit()

    client = pymongo.MongoClient(MONGO_URL)
    lsst = client.lsst

    for i, arg in enumerate(sys.argv):
        if i == 0:
            continue

        if arg == "1":
            test1(lsst.Object)
        elif arg == "2":
            test2(lsst.Source)
        elif arg == "3":
            test3(lsst.ForcedSource)
        elif arg == "4":
            test4(lsst.Object)
        elif arg == "5":
            test5(lsst.Object)
        elif arg == "6":
            #test6(lsst.Object)
            pass
        elif arg == "7":
            test7(lsst.Object)
        elif arg == "8":
            test8(lsst.Object)
        elif arg == "9":
            test9(lsst.Object)
        elif arg == "10":
            test10(lsst.Source)
        elif arg == "11":
            test11(lsst.Source)
        elif arg == "12":
            test12(lsst.Source)
        elif arg == "13":
            test13(lsst.ForcedSource)
        elif arg == "14":
            test14(lsst.ForcedSource)



"""
select count(*) 
from Object o1, Object o2 
where qserv_areaspec_box(90.299197, -66.468216, 98.762526, -56.412851) and scisql_angSep(o1.ra, o1.decl, o2.ra, o2.decl) < 0.015;   (11 min 16.02 sec)
"""
