#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configuration as conf
import argparse
import re

class Args(object):
    def __init__(self, rows=conf.IMAGES_IN_RA, columns=conf.IMAGES_IN_DEC, pixels=conf.PIXELS_PER_DEGREE, graphic=False):
        self.rows = rows
        self.columns = columns
        self.pixels = pixels
        self.graphic = graphic

    def __str__(self):
        return 'rows=%d columns=%d pixels=%d graphic=%s' % (self.rows, self.columns, self.pixels, self.graphic)

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--pixels', type=int, help="pixels", default=conf.PIXELS_PER_DEGREE)
    parser.add_argument('-i', '--images', help="images", default="%dx%d" % (conf.IMAGES_IN_RA, conf.IMAGES_IN_DEC))
    parser.add_argument('-g', '--graphic', help="graphic", action="store_true")
    args = parser.parse_args()

    m = re.match(r'(\d*)[x](\d*)', args.images)

    result = Args(int(m.group(1)), int(m.group(2)), args.pixels, args.graphic)

    conf.IMAGES_IN_RA = result.rows
    conf.IMAGES_IN_DEC = result.columns
    conf.PIXELS_PER_DEGREE = result.pixels
    conf.HAS_GRAPHIC = result.graphic

    return result

if __name__ == '__main__':
    a = get_args()
    print(a)

    pass

