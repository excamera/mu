#!/usr/bin/python

import imghdr
from PIL import Image
import sys

def convert_rbgpng_to_rgbla(infile, outfile):
    """ Function to convert RGB PNG images to LA images (greyscale)

    Args:
        param1: Path and name of input rgb image with png header and extension
        param2: Path and name of output image with png extension

    """
    if imghdr.what(infile) is not 'png':
        sys.exit("Error: Input file does not have png header")
    # If file extension is not png does not imply the image header is not png
    if infile.lower().endswith('.png') is False:
        print "Warning: Input file does not have a png extension"
    if outfile.lower().endswith('.png') is False:
        sys.exit("Error: Output file requires a png extension")
    try:
        rgb_img = Image.open(infile)
    except IOError:
        sys.exit("Error: Unable to open input file")
    if rgb_img.mode is not 'RGB':
        sys.exit("Error: Input image color mode is [" + rgb_img.mode +
                "] ,RGB expected")
    grey_img = rgb_img.convert('LA')
    grey_img.save(outfile)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("Usage: " + sys.argv[0]+ " <infile> <outfile.png>")
    convert_rbgpng_to_rgbla(sys.argv[1], sys.argv[2])

