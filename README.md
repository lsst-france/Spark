# Spark
RD in the context of the Spark technology

We simulate objects in the sky
Each object has following characteristics

- position (ra, dec) : ra uniform on [-90.0, 90.0], dec uniform on [0.0, 90.0]
- intrinsic intensity : uniform on [1, 1000.0]
- intrinsic color : uniform on [1 .. 6]
- red shift : uniform on [0.0 .. 3.0]

- I0 = 500.0
- effective luminosity: f(intensity, redshift) : el = log(I/I0) * z


Then we construct the image for objects:
- Gaussian pattern at position. Pixels are filled up to some threshold
- Then we fill the image from simulated objects

One image:
- rasize = 180.0 / 100
- decsize = 90.0 / 100

* object_extension(height):

    Compute the maximum extension (in pixels) of a simulated object with a maximum height value
    Since the simulation os based on a 2D gaussian, we limit the object extension to
    pixel values > 1.0

* build_simulation_pattern(height, size=None, sigma=SIGMA)

    a 2D grid of pixels as a 2D centered normalized gaussian

SkyObjects are generated randomly over the sky

* simul_one_object(ra0_region, ra1_region, dec0_region, dec1_region)

    simulate one celestial object inside a given region in the sky

The Imager class is responsible of filling an image with the traces of SkyObjects
When filling the image we extend the raw image size with a margin able to
receive the complete trace of objects. The size of the margin is computed so as to
contain all object trace that can touch the base image frame.
The filling process adds also a background level.

        Construct a pixel image from the shared database of simulated objects
        The image is first designed to cover a single patch
        but has to be extended to contain all complete object traces.
        - find all objects visible from this region of the sky
        - compute the max of all object extensions
        - extend the image to include all extended objects
        - then produce the object traces


Now the images has been generated following the simulation model.
We can now start the discovery process from those images
- get the background
- detect the object traces
- compute their luminosity
- find object references matching objects from the reference catalog


General description of a cluster:
- its position, (row / column)
- its integrated value

Create a 2D grid of pixels to form a PSF to be applied onto the
image to detect objects. This pattern has a form of a 2D centered
normalized gaussian. The size must be odd.

Check if a peak exists at the (r, c) position
To check if a peak exists:
- we consider the value at the specified position
- we verify all values immediately around the specified position are lower

Knowing that a peak exists at the specified position, we capture the cluster around it:
- loop on the distance from center:
  - sum pixels at a given distance
  - increase the distance until the sum falls down below some threshold

Returns integral, radius, geometric center


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


