#!/usr/bin/env python
# -*- coding: utf-8 -*-

import args
import job
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
import configuration as conf
import stepper as step
import dataset

if job.HAS_FUTURES:
    import concurrent.futures

if job.HAS_JOBLIB:
    from joblib import Parallel, delayed
    import multiprocessing
    num_cores = multiprocessing.cpu_count()

if job.HAS_SPARK:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import *
    from pyspark import SparkConf, SparkContext



def gaussian_model(x, maxvalue, meanvalue, sigma):
    """
    Compute a gaussian function
    """
    return maxvalue * np.exp(-(x - meanvalue)**2 / (2 * sigma**2))


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
        return self.ra0 + self.column/conf.PIXELS_PER_DEGREE

    def dec(self):
        return self.dec0 + self.row/conf.PIXELS_PER_DEGREE

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

    def __call__(self, image, background, dispersion, factor=conf.THRESHOLD_FACTOR):

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

        stepper = step.Stepper()

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

def one_image(image_id, dataframe=None):
    print('starting one_image', image_id)

    stepper = step.Stepper()

    if dataframe is None:
        data = dataset.Dataset(image_id)
        image_id = data.image_id
        ra = data.ra
        dec = data.dec
        image = data.image
        r = data.r
        c = data.c
    else:
        image_id = dataframe[0]
        ra = dataframe[1]
        dec = dataframe[2]
        r = dataframe[3]
        c = dataframe[4]
        image = np.array(dataframe[5])

    background, dispersion, x, y = compute_background(image)

    max_y = np.max(y)
    print('max y', max_y, 'background', background, 'dispersion', dispersion)

    #========================================================================================
    stepper.show_step('background computed')

    clustering = Clustering(ra, dec)
    clusters = clustering(image, background, dispersion)

    #========================================================================================
    stepper.show_step('== clusters computed')

    stars = job.setup_db()

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

    return image, cluster_found, len(clusters), sum(all_matches)


def analyze(x):
    image, cluster_found, clusters, all_matches = one_image(x[0], x)
    text = '  -> %d found vs. %d clusters in image. Match efficiency %f' % (cluster_found, clusters, all_matches/clusters)
    # return x[5]
    return 'image %d %s' % (x[0], text)

if __name__ == '__main__':

    a = args.get_args()
    print('rows=%d columns=%d pixels=%d graphic=%s' % (conf.IMAGES_IN_RA,
                                                       conf.IMAGES_IN_DEC,
                                                       conf.PIXELS_PER_DEGREE,
                                                       conf.HAS_GRAPHIC))

    stepper = step.Stepper()

    if job.HAS_JOBLIB:
        num_cores = multiprocessing.cpu_count()
        print('core number:', num_cores)

    if job.HAS_FUTURES:
        exe = concurrent.futures.ProcessPoolExecutor()

    submissions = []

    if job.HAS_SPARK:

        spark = SparkSession \
                .builder \
                .appName("LSSTSim") \
                .getOrCreate()

        sc = spark.sparkContext

        print("========================================= read back data")
        df = spark.read.format("com.databricks.spark.avro").load("./images")

        rdd = df.rdd.map(lambda x: (x.id, x.ra, x.dec, x.r, x.c, np.array(x.image)))\
            .map(lambda x: analyze(x))

        result = rdd.collect()

        for r in result:
            # submissions.append(image)
            print(r)
            pass

    else:

        if job.HAS_JOBLIB:
            n_jobs = num_cores
            # n_jobs = 1
            submissions = Parallel(n_jobs=n_jobs) \
                (delayed(one_image)(image_id) for image_id in range(4))
        else:
            for img_id in range(conf.IMAGES_IN_RA*conf.IMAGES_IN_DEC):
                if job.HAS_FUTURES:
                    s = exe.submit(one_image, img_id)
                else:
                    s = one_image(img_id)
                submissions.append(np.array(s))

    #========================================================================================
    stepper.show_step('All images computed.')

    s_id = 0

    if job.SHOW_GRAPHICS:
        _, axes = plt.subplots(conf.IMAGES_IN_RA, conf.IMAGES_IN_DEC)

    for r in range(conf.IMAGES_IN_RA):
        for c in range(conf.IMAGES_IN_DEC):
            if len(submissions) > 0: 
                s = submissions[s_id]
                if job.HAS_FUTURES:
                    image = s.result()
                else:
                    image = s

                if job.SHOW_GRAPHICS:
                    axes[r, c].imshow(image)

                s_id += 1

    #========================================================================================
    stepper.show_step('Done')

    if job.SHOW_GRAPHICS:
        plt.show()

