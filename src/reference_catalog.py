#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configuration as conf
import job
import stepper as step
import numpy as np

if job.HAS_MONGODB:
    import pymongo

if job.HAS_FUTURES:
    import concurrent.futures


"""
SkyObjects are generated randomly over the sky
"""

ObjectId = 0

class SkyObject(object):
    def __init__(self, ra=None, dec=None, intensity=None, color=None, redshift=None, odb=None):
        if odb is None:
            global ObjectId

            self.ra = ra
            self.dec = dec
            self.intensity = intensity
            self.color = color
            self.redshift = redshift
            self.luminosity = 1000.0 * self.redshift * np.log10(1.0 + conf.INTENSITY0/self.intensity)

            self.id = ObjectId
            ObjectId += 1
        else:
            self.ra = obj['ra']
            self.dec = obj['dec']
            self.intensity = obj['intensity']
            self.color = obj['color']
            self.redshift = obj['redshift']
            self.luminosity = obj['luminosity']
            self.id = obj['id']

    def to_db(self):
        obj = dict()

        obj['ra'] = self.ra
        obj['dec'] = self.dec
        obj['intensity'] = self.intensity
        obj['color'] = self.color
        obj['redshift'] = self.redshift
        obj['luminosity'] = self.luminosity
        obj['id'] = self.id

        return obj

    def __repr__(self):
        str = 'pos=[%f, %f] I=%f Color=%d Z=%f L=%f' % (self.ra,
                                                        self.dec,
                                                        self.intensity,
                                                        self.color,
                                                        self.redshift,
                                                        self.luminosity)
        return str

def simul_one_object(ra0_region, ra1_region, dec0_region, dec1_region):
    """
    simulate one celestial object inside a given region in the sky
    """
    ra = np.random.random() * (ra1_region - ra0_region) + ra0_region
    dec = np.random.random() * (dec1_region - dec0_region) + dec0_region
    intensity = conf.INTENSITY0 / 2 + np.random.random() * conf.INTENSITY0
    color = np.random.randint(1, 6)
    redshift = np.random.random() * 3.0
    o = SkyObject(ra, dec, intensity, color, redshift)
    return o



def create_reference_catalog():

    """
    We simulate a sky region extended by one image width
    besides the basic conf.IMAGES_IN_RA x conf.IMAGES_IN_DEC
    """

    stepper = step.Stepper()

    if job.HAS_JOBLIB:
        num_cores = multiprocessing.cpu_count()
        print('core number:', num_cores)

    if job.HAS_FUTURES:
        exe = concurrent.futures.ProcessPoolExecutor()

    region_ra0 = conf.RA0 - conf.IMAGE_RA_SIZE
    region_ra1 = region_ra0 + conf.IMAGE_RA_SIZE * (conf.IMAGES_IN_RA + 2)

    region_dec0 = conf.DEC0 - conf.IMAGE_DEC_SIZE
    region_dec1 = region_dec0 + conf.IMAGE_DEC_SIZE * (conf.IMAGES_IN_DEC + 2)

    print('simulation region [', region_ra0, ':', region_ra1, ',', region_dec0, ':', region_dec1, ']')

    submissions = []

    if job.HAS_JOBLIB:
        n_jobs = num_cores
        # n_jobs = 1
        submissions = Parallel(n_jobs=n_jobs)\
            (delayed(simul_one_object)(region_ra0, region_ra1, region_dec0, region_dec1) for n in range(conf.NOBJECTS))
    else:
        for n in range(conf.NOBJECTS):
            if job.HAS_FUTURES:
                o = exe.submit(simul_one_object, region_ra0, region_ra1, region_dec0, region_dec1)
            else:
                o = simul_one_object(region_ra0, region_ra1, region_dec0, region_dec1)

            submissions.append(o)

    #========================================================================================
    stepper.show_step('all submissions done')

    stars = None
    if job.HAS_MONGODB:
        client = pymongo.MongoClient(job.MONGO_URL)
        lsst = client.lsst

        recreate = True

        if recreate:
            try:
                stars = lsst.stars
                lsst.drop_collection('stars')
            except:
                pass

        stars = lsst.stars

    #========================================================================================
    stepper.show_step('Db initialized')

    objects = dict()
    for s in submissions:
        if job.HAS_FUTURES:
            o = s.result()
        else:
            o = s

        objects[o.id] = o

    # ========================================================================================
    stepper.show_step('all objects created {}'.format(conf.NOBJECTS))

    if job.HAS_MONGODB:
        for o_id in objects:
            o = objects[o_id]

            object = o.to_db()

            object['center'] = {'type': 'Point', 'coordinates': [o.ra, o.dec]}
            try:
                id = stars.insert_one(object)
                # print('object inserted', o.ra, o.dec)
            except Exception as e:
                print('oops')
                print(e.message)
                sys.exit()

        #========================================================================================
        stepper.show_step('all objects inserted')

        stars.create_index([('center', '2dsphere')])

        #========================================================================================
        stepper.show_step('2D index created')


def get_all_reference_objects():
    objects = dict()
    stars = None
    if job.HAS_MONGODB:
        client = pymongo.MongoClient(job.MONGO_URL)
        lsst = client.lsst
        stars = lsst.stars

        for oid, o in enumerate(
            stars.find()):
            objects[oid] = SkyObject(odb = o)

    return objects

if __name__ == '__main__':
    create_reference_catalog()

    objects = get_all_reference_objects()
    print(len(objects), 'objects found from Db')
