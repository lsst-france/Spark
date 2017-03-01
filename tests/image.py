#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pickle
import matplotlib.pyplot as plt

class Dataset:
    def __init__(self, image_id, ra, dec, image, r, c):
        self.image_id = image_id
        self.ra = ra
        self.dec = dec
        self.image = image
        self.r = r
        self.c = c

if __name__ == '__main__':
    images_in_ra = 0
    images_in_dec = 0

    datasets = dict()
    for id in range(6):
        try:
            dataset = pickle.load(open("../data/image%d.p" % id, "rb"))
            if (dataset.r + 1) > images_in_ra:
                images_in_ra = dataset.r + 1
            if (dataset.c + 1) > images_in_dec:
                images_in_dec = dataset.c + 1
            datasets[dataset.image_id] = dataset
        except:
            print('error', id)

    _, axes = plt.subplots(images_in_ra, images_in_dec)

    for id in datasets:
        dataset = datasets[id]
        axes[dataset.r, dataset.c].imshow(dataset.image)

    plt.show()
