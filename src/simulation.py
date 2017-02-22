#!/usr/bin/env python
# -*- coding: utf-8 -*-


HAS_FUTURES = True
HAS_JOBLIB = False
SHOW_GRAPHICS = False

import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
import time

import os
import pymongo
import pickle

if os.name == 'nt':
    MONGO_URL = r'mongodb://127.0.0.1:27017'

if HAS_FUTURES:
    import concurrent.futures

if HAS_JOBLIB:
    from joblib import Parallel, delayed
    import multiprocessing
    num_cores = multiprocessing.cpu_count()

"""
We simulate objects in the sky
Each object has following characteristics

- position (ra, dec) : ra uniform on [-90.0, 90.0], dec uniform on [0.0, 90.0]
- intrinsic intensity : uniform on [1, 1000.0]
- intrinsic color : uniform on [1 .. 6]
- red shift : uniform on [0.0 .. 3.0]

- I0 = 500.0
- effective luminosity: f(intensity, redshift) : el = log(I/I0) * z

We can construct the image for an object

Gaussian pattern at position. Pixels are filled up to some threshold

Then we fill the image from simulated objects


- rasize = 180.0 / 100
- decsize = 90.0 / 100

We find all objects
"""

# Object number to simulate
NOBJECTS = 100000

# Basic intensity
INTENSITY0 = 100000.0

# Splitting the sky into patches at the simulation stage
RAPATCHES = 800
DECPATCHES = 400

# Dispersion for the simulator
SIGMA = 4.0

# Simulating the CCD
PIXELS_PER_DEGREE = 8000
# PIXELS_PER_DEGREE = 16000
# 4000 pixels = 0,2253744679276044 °

# Simulation region
RA0 = -20.0
DEC0 = 20
IMAGES_IN_RA = 3
IMAGES_IN_DEC = 2

# we consider that all images cover exactly one patch
IMAGE_RA_SIZE = 180.0 / RAPATCHES
IMAGE_DEC_SIZE = 90.0 / DECPATCHES

TOTALCCDS = 189
TOTALPIXELS = 3200000000

BACKGROUND = 200.0

SPHERE = 4*np.pi*(180.0/np.pi)**2.0 # = 41 253 °²

THRESHOLD_FACTOR = 2.0

class Stepper(object):
    previous_time = None

    def __init__(self):
        self.previous_time = time.time()

    def show_step(self, label='Initial time'):
        now = time.time()
        delta = now - self.previous_time

        print('--------------------------------', label, '{:.3f} seconds'.format(delta))

        self.previous_time = now


def object_extension(height):
    """
    Compute the maximum extension (in pixels) of a simulated object with a maximum height value
    Since the simulation os based on a 2D gaussian, we limit the object extension to
    pixel values > 1.0
    """
    if height < 1.0:
        return 0.0

    try:
        size = int(SIGMA * np.sqrt(- 2.0 * np.log(1.0 / height)))
    except:
        print('bad')

    return size


def build_simulation_pattern(height, size=None, sigma=SIGMA):
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


def gaussian_model(x, maxvalue, meanvalue, sigma):
    """
    Compute a gaussian function
    """
    return maxvalue * np.exp(-(x - meanvalue)**2 / (2 * sigma**2))


"""
SkyObjects are generated randomly over the sky
"""

ObjectId = 0

class SkyObject(object):
    def __init__(self, ra, dec, intensity, color, redshift):
        global ObjectId

        self.ra = ra
        self.dec = dec
        self.intensity = intensity
        self.color = color
        self.redshift = redshift
        self.luminosity = 1000.0 * self.redshift * np.log10(1.0 + INTENSITY0/self.intensity)

        self.id = ObjectId
        ObjectId += 1

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
    intensity = INTENSITY0 / 2 + np.random.random() * INTENSITY0
    color = np.random.randint(1, 6)
    redshift = np.random.random() * 3.0
    o = SkyObject(ra, dec, intensity, color, redshift)
    return o


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
        image_x = int(image_ra * PIXELS_PER_DEGREE)
        image_y = int(image_dec * PIXELS_PER_DEGREE)

        image_x_size = int(IMAGE_RA_SIZE * PIXELS_PER_DEGREE)
        image_y_size = int(IMAGE_DEC_SIZE * PIXELS_PER_DEGREE)

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

            o_x = int(o.ra * PIXELS_PER_DEGREE)
            o_y = int(o.dec * PIXELS_PER_DEGREE)

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
        image = np.random.rand(image_x_size, image_y_size) * BACKGROUND

        print('image [', ra, ':', ra + IMAGE_RA_SIZE, ',',
              dec, ':', dec + IMAGE_DEC_SIZE, ']',
              len(objects_in_image), 'objects in this image')

        # fill the image with all object traces
        for o in objects_in_image:
            # print(object_size, o.ra, o.dec, image_ra)
            object_size = object_extension(o.luminosity)

            # absolute coordinates
            o_x = int(o.ra * PIXELS_PER_DEGREE)
            o_y = int(o.dec * PIXELS_PER_DEGREE)

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


"""
Now the images has been generated following the simulation model.
We can now start the discovery process from those images
- get the background
- detect the object traces
- compute their luminosity
- find object references matching objects from the reference catalog
"""


def compute_background(pixels):
    """
    Recompute the bakground level is obtained by a fit against a gaussian distribution.
    """

    # Reshape the pixels array as a flat list
    flat = np.asarray(pixels).ravel()

    # sampling size to analyze the background
    sampling_size = 200

    # build the pixel distribution to extract the background
    y, x = np.histogram(flat, sampling_size)

    # normalize the distribution for the gaussian fit
    my = np.float(np.max(y))
    y = y/my
    mx = np.float(np.max(x))
    x = x[:-1]/mx

    # compute the gaussian fit for the background
    fit, _ = curve_fit(gaussian_model, x, y)

    background = fit[1] * mx
    dispersion = abs(fit[2]) * mx
    return background, dispersion, x*mx, y*my


class Cluster():
    """
    General description of a cluster:
    - its position, (row / column)
    - its integrated value
    """

    def __init__(self, ra0, dec0, row, column, top, integral):
        self.ra0 = ra0
        self.dec0 = dec0
        self.row = row
        self.column = column
        self.top = top
        self.integral = integral

    def ra(self):
        return self.ra0 + self.column/PIXELS_PER_DEGREE

    def dec(self):
        return self.dec0 + self.row/PIXELS_PER_DEGREE

    def __str__(self):
        return "{}/{} at ra={}, dec={}".format(self.top, self.integral, self.ra(), self.dec())


class Clustering():
    """
    General clustering algorithm
    """

    def __init__(self, ra0, dec0, pattern_size=9):
        self.pattern_size = pattern_size
        self.ra0 = ra0
        self.dec0 = dec0

    def build_convolution_pattern(self):

        """
        Create a 2D grid of pixels to form a PSF to be applied onto the
        image to detect objects. This pattern has a form of a 2D centered
        normalized gaussian. The size must be odd.
        """

        if self.pattern_size % 2 == 0:
            raise ValueError

        x = np.arange(0, self.pattern_size, 1, float)
        y = np.arange(0, self.pattern_size, 1, float)
        # transpose y
        y = y[:, np.newaxis]

        y0 = x0 = self.pattern_size // 2

        # create a 2D gaussian distribution inside this grid.
        sigma = self.pattern_size / 4.0
        pattern = np.exp(-1 * ((x - x0) ** 2 + (y - y0) ** 2) / sigma ** 2)
        pattern = pattern / pattern.sum()

        return pattern

    def has_peak(self, image, r, c):

        """
        Check if a peak exists at the (r, c) position
        To check if a peak exists:
           - we consider the value at the specified position
           - we verify all values immediately around the specified position are lower
        """

        zone = image[r - 1:r + 2, c - 1:c + 2]
        top = zone[1, 1]
        if top == 0.0:
            return False

        checker = zone < top
        checker[1, 1] = True

        return checker.all()

    def spread_peak(self, image, threshold, r, c):

        """
        Knowing that a peak exists at the specified position, we capture the cluster around it:
        - loop on the distance from center:
          - sum pixels at a given distance
          - increase the distance until the sum falls down below some threshold
        Returns integral, radius, geometric center
        """

        previous_integral = image[r, c]
        radius = 1

        while True:

            integral = np.sum(image[r - radius:r + radius + 1, c - radius:c + radius + 1])
            pixels = 8 * radius
            mean = (integral - previous_integral) / pixels
            if mean < threshold:
                return previous_integral, radius - 1
            elif (r - radius) == 0 or (r + radius + 1) == image.shape[0] or \
                            (c - radius) == 0 or (c + radius + 1) == image.shape[1]:
                return integral, radius
            radius += 1
            previous_integral = integral

    def convolution_image(self, image):

        """
        principle:
        - at every position of the input image:
            - we apply a fix pattern made of one 2D normalized gaussian distribution
                - width = 9
                - magnitude = 1.0
            - we extract one zone of the original image map with same shape as the pattern
            - this zone is normalized against the greatest magnitude of the image
            - this zone is convoluted with the pattern (convolution product - CP)
            - if the CP is greater than a threshold, the CP is stored at the row/column
                position in a convolution image (CI)
        """

        # we start by building a PSF with a given width
        pattern = self.build_convolution_pattern()
        half = self.pattern_size // 2

        # define a convolution image that stores the convolution products at each pixel position
        cp_image = np.zeros((image.shape[0] - 2 * half, image.shape[1] - 2 * half), np.float)

        # loop on all pixels except the border

        def convol(image, rnum, cnum):
            rmin = rnum
            rmax = rnum + half * 2 + 1
            cmin = cnum
            cmax = cnum + half * 2 + 1

            # convolution product
            return np.sum(image[rmin:rmax, cmin:cmax] * pattern)

        for rnum, row in enumerate(image[half:-half, half:-half]):
            for cnum, col in enumerate(row):
                cp_image[rnum, cnum] = convol(image, rnum, cnum)

        # result
        return cp_image

    def __call__(self, image, background, dispersion, factor=THRESHOLD_FACTOR):

        """
        principle:
        - we then start a scan of the convolution image (CI):
            - at every position we detect if there is a peak:
                - we extract a 3x3 region of the CI centered at the current position
                - a peak is detected when ALL pixels around the center of this little region are below the center.
            - when a peak is detected, we get the cluster (the group of pixels around a peak):
                - accumulate pixels circularly around the peak until the sum of pixels at a given distance
                    is lower than the threshold
                - we compute the integral of pixel values of the cluster
        - this list of clusters is returned.
        """

        def extend_image(image, margin):
            ext_shape = np.array(image.shape) + 2 * margin
            ext_image = np.zeros(ext_shape)
            ext_image[margin:-margin, margin:-margin] = image

            return ext_image

        # make a copy with a border of half
        half = self.pattern_size // 2
        ext_image = extend_image(image, half)

        # build the convolution product image
        cp_image = self.convolution_image(ext_image)

        # make a copy with a border of 1
        ext_cp_image = extend_image(cp_image, 1)

        stepper = Stepper()

        # ========================================================================================
        stepper.show_step('  image prepared for clustering')

        peaks = []
        # scan the convolution image to detect peaks and build clusters
        threshold = background + factor * dispersion
        for rnum, row in enumerate(image):
            for cnum, col in enumerate(row):
                if cp_image[rnum, cnum] <= threshold:
                    continue
                if not self.has_peak(ext_cp_image, rnum + 1, cnum + 1):
                    continue
                peaks.append((rnum, cnum))

        # ========================================================================================
        stepper.show_step('  peaks detected')

        clusters = []

        for peak in peaks:
            rnum = peak[0]
            cnum = peak[1]
            integral, radius = self.spread_peak(image, threshold, rnum, cnum)
            if radius == 0:
                continue
            clusters.append(Cluster(self.ra0, self.dec0, rnum, cnum, image[rnum, cnum], integral))

        # ========================================================================================
        stepper.show_step('  clusters built')

        # sort by integrals then by top
        if len(clusters) > 0:
            max_top = max(clusters, key=lambda cl: cl.top).top
            clusters.sort(key=lambda cl: cl.integral + cl.top / max_top, reverse=True)

        # ========================================================================================
        stepper.show_step('  clusters sorted')

        # results
        return clusters


def add_crosses(image, clusters):
    """
    Return a new image with crosses
    """

    x = 3
    peaks = np.copy(image)
    for cl in clusters:
        rnum, cnum = round(cl.row), round(cl.column)
        peaks[rnum - x:rnum + x + 1, cnum] = image[rnum, cnum]
        peaks[rnum, cnum - x:cnum + x + 1] = image[rnum, cnum]
    return peaks


def one_image(image_id):
    print('starting one_image', image_id)

    dataset1 = pickle.load(open("../data/image%d.p" % image_id, "rb"))

    stepper = Stepper()

    ra = dataset1['ra']
    dec = dataset1['dec']
    image = dataset1['image']
    r = dataset1['r']
    c = dataset1['c']
    background, dispersion, x, y = compute_background(image)

    dataset2 = {'ra': ra,
                'dec': dec,
                'image': image,
                'background': background,
                'dispersion': dispersion,
                'r': r,
                'c': c}

    max_y = np.max(y)
    print('max y', max_y, 'background', background, 'dispersion', dispersion)

    #========================================================================================
    stepper.show_step('background computed')

    ra = dataset2['ra']
    dec = dataset2['dec']
    image = dataset2['image']
    background = dataset2['background']
    dispersion = dataset2['dispersion']
    r = dataset2['r']
    c = dataset2['c']

    clustering = Clustering(ra, dec)
    clusters = clustering(image, background, dispersion)

    dataset3 = {'clusters':clusters, 'image':image, 'r':r, 'c':c}

    #========================================================================================
    stepper.show_step('== clusters computed')


    client = pymongo.MongoClient(MONGO_URL)
    lsst = client.lsst
    stars = lsst.stars

    image = dataset3['image']
    clusters = dataset3['clusters']
    r = dataset3['r']
    c = dataset3['c']

    radius = 0.0004
    cluster_found = 0
    all_matches = []
    for cid, cluster in enumerate(clusters):
        found = False
        matches = 0
        for oid, o in enumerate(stars.find({'center': {'$geoWithin': {'$centerSphere': [[cluster.ra(), cluster.dec()], radius]}}},
                            {'_id': 0, 'where': 1, 'center': 1})):
            if not found:
                cluster_found += 1
                found = True

            matches += 1

            # print('image [', r, c, '] star found (clusterid:', cid, ') object id:', oid, ') (object:', o)
        all_matches.append(matches)

    print('  -> ', cluster_found, 'found vs. ', len(clusters), 'clusters in image. Match efficiency', sum(all_matches)/len(clusters))

    """
    image = add_crosses(image, clusters)
    _ = main_ax[r, c].imshow(image, interpolation='none')
    """

    #========================================================================================
    stepper.show_step('full image')

    return image


if __name__ == '__main__':
    stepper = Stepper()

    """
    Object simulation onto the complete half sky
    """
    if HAS_JOBLIB:
        num_cores = multiprocessing.cpu_count()
        print('core number:', num_cores)

    if HAS_FUTURES:
        exe = concurrent.futures.ProcessPoolExecutor()

    #========================================================================================
    stepper.show_step("starting simulation")

    """
    We simulate a sky region extended by one image width
    besides the basic IMAGES_IN_RA x IMAGES_IN_DEC
    """

    region_ra0 = RA0 - IMAGE_RA_SIZE
    region_ra1 = region_ra0 + IMAGE_RA_SIZE * (IMAGES_IN_RA + 2)

    region_dec0 = DEC0 - IMAGE_DEC_SIZE
    region_dec1 = region_dec0 + IMAGE_DEC_SIZE * (IMAGES_IN_DEC + 2)

    print('simulation region [', region_ra0, ':', region_ra1, ',', region_dec0, ':', region_dec1, ']')

    submissions = []

    if HAS_JOBLIB:
        n_jobs = num_cores
        # n_jobs = 1
        submissions = Parallel(n_jobs=n_jobs)\
            (delayed(simul_one_object)(region_ra0, region_ra1, region_dec0, region_dec1) for n in range(NOBJECTS))
    else:
        for n in range(NOBJECTS):
            if HAS_FUTURES:
                o = exe.submit(simul_one_object, region_ra0, region_ra1, region_dec0, region_dec1)
            else:
                o = simul_one_object(region_ra0, region_ra1, region_dec0, region_dec1)

            submissions.append(o)

    #========================================================================================
    stepper.show_step('all submissions done')

    client = pymongo.MongoClient(MONGO_URL)
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
        if HAS_FUTURES:
            o = s.result()
        else:
            o = s

        objects[o.id] = o

    # ========================================================================================
    stepper.show_step('all objects created {}'.format(NOBJECTS))

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

    """
    now we select a region of the sky
    we find all objects with an image will fall out of this region
    (ie. when no one simulated pixel will reach the region)
    """

    imager = Imager(objects)

    image_id = 0
    ra = RA0
    for r in range(IMAGES_IN_RA):
        dec = DEC0
        for c in range(IMAGES_IN_DEC):
            image, margin = imager.fill(ra, dec)

            dataset = {'id':image_id, 'ra': ra, 'dec': dec, 'image': image, 'r': r, 'c': c}

            pickle.dump(dataset, open("../data/image%d.p" % image_id, "wb"))

            image_id += 1

            dec += IMAGE_DEC_SIZE
        ra += IMAGE_RA_SIZE

    #========================================================================================
    stepper.show_step('all images created')

    submissions = []

    if HAS_JOBLIB:
        n_jobs = num_cores
        # n_jobs = 1
        submissions = Parallel(n_jobs=n_jobs) \
            (delayed(one_image)(image_id) for image_id in range(4))
    else:
        for img_id in range(IMAGES_IN_RA*IMAGES_IN_DEC):
            if HAS_FUTURES:
                s = exe.submit(one_image, img_id)
            else:
                s = one_image(img_id)
            submissions.append(s)

    #========================================================================================
    stepper.show_step('All images computed.')

    s_id = 0

    if SHOW_GRAPHICS:
        _, axes = plt.subplots(IMAGES_IN_RA, IMAGES_IN_DEC)

    for r in range(IMAGES_IN_RA):
        for c in range(IMAGES_IN_DEC):
            s = submissions[s_id]
            if HAS_FUTURES:
                image = s.result()
            else:
                image = s

            if SHOW_GRAPHICS:
                axes[r, c].imshow(image)

            s_id += 1

    #========================================================================================
    stepper.show_step('Done')

    if SHOW_GRAPHICS:
        plt.show()

