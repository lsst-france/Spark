#!/usr/bin/env python
# -*- coding: utf-8 -*-



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


import job
import numpy as np


# Object number to simulate
NOBJECTS = 1000000

# Basic intensity
INTENSITY0 = 100000.0

# Splitting the sky into patches at the simulation stage
RAPATCHES = 800
DECPATCHES = 400

# Dispersion for the simulator
SIGMA = 4.0

# Simulating the CCD
PIXELS_PER_DEGREE = 16000
# 4000 pixels = 0,2253744679276044 °

# Simulation region
RA0 = -20.0
DEC0 = 20
IMAGES_IN_RA = 5
IMAGES_IN_DEC = 5

# we consider that all images cover exactly one patch
IMAGE_RA_SIZE = 180.0 / RAPATCHES
IMAGE_DEC_SIZE = 90.0 / DECPATCHES

TOTALCCDS = 189
TOTALPIXELS = 3200000000

BACKGROUND = 200.0

SPHERE = 4*np.pi*(180.0/np.pi)**2.0 # = 41 253 °²

THRESHOLD_FACTOR = 2.0


if __name__ == '__main__':
    pass
