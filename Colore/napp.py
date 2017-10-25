
from pyspark.sql import SparkSession
from pyspark.sql import functions

from astropy.io import fits
import numpy as np
from pyspark.sql.types import *
import argparse
import random

hdu = fits.open('/mongo/log/colore/batch/gal10249.fits')

hdu.info()
hdu[0].header
hdu[1].data

header = hdu[1].header
rows = header['NAXIS2']

fields = dict()

for i in range(header['TFIELDS']):
    j = i + 1
    fields[header['TTYPE{}'.format(j)]] = (header['TFORM{}'.format(j)], header['TUNIT{}'.format(j)])

print("Fields = ", fields)

"""
for row in range(10):
    print(hdu[1].data[row])
"""

data = hdu[1].data

x = data[0:10]

y = np.array(x)

c = data.columns
cols = fits.ColDefs([c['RA'], c['DEC']])
data2 = fits.BinTableHDU.from_columns(cols)
a = np.array(data2.data.astype('<f4'))

xxx = a.data

points = 100000000
ra = data['RA'][:points]
dec = data['DEC'][:points]
z = data['Z_COSMO'][:points]
dz = data['DZ_RSD'][:points]

points = np.column_stack((ra, dec, z, dz))

print(points)

print("end")



