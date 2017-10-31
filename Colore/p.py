
import os
from astropy.io import fits
import numpy as np
import argparse
import random

import stepper as stp

cores = 1
tmp = '/'
where = '/'

if os.path.exists('/mongo'):
    where = '/mongo/log/colore/batch/'
    cores = 100
    tmp = '/mongo/log/tmp/'
elif os.path.exists('/home/ubuntu/'):
    where = '/home/ubuntu/'
    cores = 8
    tmp = '/home/ubuntu/'
else:
    print('where can I get fits files?')
    exit()


hdu = fits.open(where + 'gal10249.fits')

# for row in range(10):
#    print(hdu[1].data[row])

data = hdu[1].data

s1 = stp.Stepper()
s = stp.Stepper()

def build(data, subset, steps):

  points = []

  subset = int(1.0 / subset)

  if subset <= 0:
      return None

  part = subset / steps
  block = int((data.size / subset) / steps)

  print("steps = ", steps, " part = ", part, " block = ", block, " total data = ", (block * steps))


  for i in range(steps):
    start = int(i * block)
    ra = data['RA'][start:start+block]
    dec = data['DEC'][start:start+block]
    z = data['Z_COSMO'][start:start+block]
    dz = data['DZ_RSD'][start:start+block]

    points.append(np.column_stack((ra, dec, z, dz)))

    # print("i=", i, " points=", points_df.take(10))
    s.show_step("==> i={}".format(i))

  return np.concatenate(points)

allp = build(data, subset=1.0, steps=1)

# allp = data.transpose()

s1.show_step("==> total")




