#!/usr/bin/env python
# -*- coding: utf-8 -*-

import stepper as step
import job
import reference_catalog as catalog
import configuration as conf
import numpy as np
import dataset

if job.HAS_FUTURES:
    import concurrent.futures

if job.HAS_JOBLIB:
    from joblib import Parallel, delayed
    import multiprocessing
    num_cores = multiprocessing.cpu_count()


def object_extension(height):
    """
    Compute the maximum extension (in pixels) of a simulated object with a maximum height value
    Since the simulation os based on a 2D gaussian, we limit the object extension to
    pixel values > 1.0
    """
    if height < 1.0:
        return 0.0

    size = 0

    try:
        size = int(conf.SIGMA * np.sqrt(- 2.0 * np.log(1.0 / height)))
    except:
        print('bad')

    return size


def build_simulation_pattern(height, size=None, sigma=conf.SIGMA):
    """
    a 2D grid of pixels as a 2D centered normalized gaussian
    """
    x = np.arange(0, size*2, 1, float)
    y = np.arange(0, size*2, 1, float)
    # transpose y
    y = y[:, np.newaxis]

    y0 = x0 = size

    # create a 2D gaussian distribution inside this grid.
    pattern = height * np.exp(-0.5 * ((x - x0) ** 2 + (y - y0) ** 2) / sigma**2)

    return pattern



"""
The Imager class is responsible of filling an image with the traces of SkyObjects
When filling the image we extend the raw image size with a margin able to
receive the complete trace of objects. The size of the margin is computed so as to
contain all object trace that can touch the base image frame.
The filling process adds also a background level.
"""
class Imager(object):
    def __init__(self, objects):
        self.objects = objects

    def fill(self, ra, dec):
        """
        Construct a pixel image from the shared database of simulated objects
        The image is first designed to cover a single patch
        but has to be extended to contain all complete object traces.
        - find all objects visible from this region of the sky
        - compute the max of all object extensions
        - extend the image to include all extended objects
        - then produce the object traces
        """

        # basic image characteristics = ra/dec position (top/left corner)
        image_ra = ra
        image_dec = dec

        # ra/dec convert to pixels
        image_x = int(image_ra * conf.PIXELS_PER_DEGREE)
        image_y = int(image_dec * conf.PIXELS_PER_DEGREE)

        image_x_size = int(conf.IMAGE_RA_SIZE * conf.PIXELS_PER_DEGREE)
        image_y_size = int(conf.IMAGE_DEC_SIZE * conf.PIXELS_PER_DEGREE)

        # print('image size: ', image_x_size, image_y_size)

        # store all objects that fit onto this image
        objects_in_image = []

        # compute the maximum extension needed to contain the complete object traces that might touch
        # the raw image frame
        extension = None

        # print(len(self.objects), 'objects')

        """
        scan all SkyObjects and select objects which trace might touch the image frame
        """
        for oid in self.objects:
            o = self.objects[oid]
            object_size = object_extension(o.luminosity)

            o_x = int(o.ra * conf.PIXELS_PER_DEGREE)
            o_y = int(o.dec * conf.PIXELS_PER_DEGREE)

            # Query if this object (with its extension) touches this image
            if object_size == 0.0:
                continue

            if (o_x + object_size) < image_x:
                continue
            if (o_x - object_size) > (image_x + image_x_size):
                continue
            if (o_y + object_size) < image_y:
                continue
            if (o_y - object_size) > (image_y + image_y_size):
                continue

            # print('object', object_size, o_x, o_y, 'image', image_x, image_x + image_x_size, image_y, image_y + image_y_size)

            # now this object fits within the image
            if extension is None or (object_size > extension):
                # compute the overall extension to the image
                extension = object_size

            # print(object_size, 'X:', o_x, image_x, ' Y:', o_y, image_y)

            # store this object
            objects_in_image.append(o)

        if extension is None:
            extension = 0
            print('not event one object in region')

        # we extend the image by a margin with a width able to accept the largest object
        margin = 2 * extension

        image_x -= margin
        image_y -= margin
        image_x_size += 2 * margin
        image_y_size += 2 * margin

        # create the extended image and fill it with a background level
        image = np.random.rand(image_x_size, image_y_size) * conf.BACKGROUND

        print('image [', ra, ':', ra + conf.IMAGE_RA_SIZE, ',',
              dec, ':', dec + conf.IMAGE_DEC_SIZE, ']',
              len(objects_in_image), 'objects in this image')

        # fill the image with all object traces
        for o in objects_in_image:
            # print(object_size, o.ra, o.dec, image_ra)
            object_size = object_extension(o.luminosity)

            # absolute coordinates
            o_x = int(o.ra * conf.PIXELS_PER_DEGREE)
            o_y = int(o.dec * conf.PIXELS_PER_DEGREE)

            # relative coordinates
            x = o_x - image_x
            y = o_y - image_y

            # print('ra:', o.ra, image_ra, ra, ' dec:', o.dec, image_dec, dec, 'x:', x, ' y:', y)

            # an object trace is a gaussian pattern with fixed width but height related width luminosity
            pattern = build_simulation_pattern(o.luminosity, size=object_size)

            try:
                image[x - object_size:x + object_size, y - object_size:y + object_size] += pattern
            except:
                print('bad shape')

        return image, margin


if __name__ == '__main__':
    stepper = step.Stepper()

    """
    First step all reference objects have been created in the reference_catalog application
    """

    objects = catalog.get_all_reference_objects()
    print('----', len(objects), 'objects found from Db')

    """
    Object simulation onto the complete half sky
    """
    if job.HAS_JOBLIB:
        num_cores = multiprocessing.cpu_count()
        print('core number:', num_cores)

    if job.HAS_FUTURES:
        exe = concurrent.futures.ProcessPoolExecutor()

    #========================================================================================
    stepper.show_step("starting simulation")

    """
    now we select a region of the sky
    we find all objects with an image will fall out of this region
    (ie. when no one simulated pixel will reach the region)
    """

    imager = Imager(objects)

    image_id = 0
    ra = conf.RA0
    for r in range(conf.IMAGES_IN_RA):
        dec = conf.DEC0
        for c in range(conf.IMAGES_IN_DEC):
            image, margin = imager.fill(ra, dec)

            data = dataset.Dataset(image_id, ra, dec, image, r, c)
            data.save(image_id)

            image_id += 1

            dec += conf.IMAGE_DEC_SIZE
        ra += conf.IMAGE_RA_SIZE

    #========================================================================================
    stepper.show_step('all images created')

