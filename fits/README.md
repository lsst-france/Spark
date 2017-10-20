

* https://heasarc.gsfc.nasa.gov/docs/fcg/standard_dict.html
* https://archive.stsci.edu/fits/fits_standard/node39.html


    Nbits = |BITPIX| x GCOUNT x (PCOUNT + NAXIS x ... x NAXISm)


* NAXIS = 2 for TABLE or BINTABLE
* BITPIX
  * unsigned 8 bit integer (BITPIX=8) => Byte
  * 16 bit signed integer (BITPIX=16) => Short
  * 32 bit twos complement integer (BITPIX=32) => Int
  * 32 or 64 bit IEEE floating point format (BITPIX=-32 or -64) => Float or Double



FITS format code         Description                     8-bit bytes

* L                        logical (Boolean)               1
* X                        bit                             *
* B                        Unsigned byte                   1
* I                        16-bit integer                  2
* J                        32-bit integer                  4
* K                        64-bit integer                  4
* A                        character                       1
* E                        single precision floating point 4
* D                        double precision floating point 8
* C                        single precision complex        8
* M                        double precision complex        16
* P                        array descriptor                8
* Q                        array descriptor                16


