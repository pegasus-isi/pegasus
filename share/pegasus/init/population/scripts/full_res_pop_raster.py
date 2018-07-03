#!/usr/bin/python3

from typing import Dict
import optparse

import numpy as np
import rasterio
from rasterio import features


def main(county_pop_file, spatial_dist_file, fname_out, no_data_val=-9999):
    '''
    county_pop_file: County level population estimates
    spatial_dist_file: Spatial projection of population distribution
    '''

    # -------------------------------------
    # Open and read raster file with county
    # level population estimates
    # -------------------------------------
    with rasterio.open(county_pop_file) as rastf:
        county_pop = rastf.read()
        nodatacp = rastf.nodata

    # --------------------------------------------------------------
    # Open and read raster file with spatial population distribution
    # --------------------------------------------------------------
    with rasterio.open(spatial_dist_file) as rastf:
        pop_dist = rastf.read()
        nodatasp = rastf.nodata
        prf = rastf.profile

    county_pop = np.squeeze(county_pop)
    pop_dist = np.squeeze(pop_dist)

    pop_est = np.ones(pop_dist.shape)*no_data_val
    ind1 = np.where(county_pop.flatten() != nodatacp)[0]
    ind2 = np.where(pop_dist.flatten() != nodatasp)[0]

    ind = np.intersect1d(ind1, ind2)
    ind2d = np.unravel_index(ind, pop_dist.shape)

    pop_est[ind2d] = county_pop[ind2d] * pop_dist[ind2d]
    pop_est[ind2d] = np.round(pop_est[ind2d])

    # Update raster meta-data
    prf.update(nodata=no_data_val)

    # Write out spatially distributed population estimate to raster
    with open(fname_out, "wb") as fout:
        with rasterio.open(fout.name, 'w', **prf) as out_raster:
            out_raster.write(pop_est.astype(rasterio.float32), 1)


argparser = optparse.OptionParser()
argparser.add_option('--population-file', action='store', dest='pop_file',
                     help='County level population estimates')
argparser.add_option('--dist-file', action='store', dest='dist_file',
                     help='Spatial projection of population distribution')
argparser.add_option('--out-file', action='store', dest='out_file',
                     help='Filename of the output')
(options, args) = argparser.parse_args()

if not options.pop_file:
    print('Please specify a population file with --population-file')
    sys.exit(1)

if not options.dist_file:
    print('Please specify a distribution file with --dist-file')
    sys.exit(1)

if not options.out_file:
    print('Please specify the name of the output with --out-file')
    sys.exit(1)

main(options.pop_file, options.dist_file, options.out_file)

