#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os

if os.name == 'nt':
    HAS_SPARK = False
else:
    HAS_SPARK = True

if HAS_SPARK:
    HAS_FUTURES = False
    HAS_JOBLIB = False
else:
    HAS_FUTURES = False
    HAS_JOBLIB = False

SHOW_GRAPHICS = True
HAS_MONGODB = True

if HAS_MONGODB:
    import pymongo
import pickle

if HAS_MONGODB:
    MONGO_URL = r'mongodb://127.0.0.1:27017'

if HAS_FUTURES:
    import concurrent.futures

if HAS_JOBLIB:
    from joblib import Parallel, delayed
    import multiprocessing
    num_cores = multiprocessing.cpu_count()

def setup_db():
    if HAS_MONGODB:
        client = pymongo.MongoClient(MONGO_URL)
        lsst = client.lsst
        stars = lsst.stars
        return stars
    else:
        return None


if __name__ == '__main__':
    pass
