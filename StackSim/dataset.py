#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pickle

class Dataset:
    def __init__(self, image_id, ra=None, dec=None, image=None, r=None, c=None):
        if ra is not None:
            self.image_id = image_id
            self.ra = ra
            self.dec = dec
            self.image = image
            self.r = r
            self.c = c
        else:
            obj = pickle.load(open("../data/image%d.p" % image_id, "rb"))
            self.image_id = obj.image_id
            self.ra = obj.ra
            self.dec = obj.dec
            self.image = obj.image
            self.r = obj.r
            self.c = obj.c

    def save(self, image_id):
        pickle.dump(self, open("../data/image%d.p" % image_id, "wb"))


if __name__ == '__main__':
    pass