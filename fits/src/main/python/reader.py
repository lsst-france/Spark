#!/usr/bin/env python
# -*- coding: utf-8 -*-


from astropy.io import fits

names = ["test.fits", "SDSS9.fits", "dss.NSV_193_40x40.fits", "NPAC01.fits"]
base = '/home/christian.arnault/LSSTSpark/fits/src/main/resources/sparkapps/'
for name in names:
    hdulist = fits.open(base + name)
    hdulist.info()

    try:
        scidata = hdulist[0].data
        print('0 has data', scidata)
    except:
        print('0 nodata')

    try:
        scidata = hdulist[1].data
        print('1 has data', scidata)
    except:
        print('1 nodata')

    hdulist.close()
