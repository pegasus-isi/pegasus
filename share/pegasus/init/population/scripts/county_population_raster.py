#!/usr/bin/python3

import json
from collections import OrderedDict
import pandas as pd
from typing import Dict
import optparse

import geopandas as gpd
import numpy as np
import rasterio
from rasterio import features

import geospatial


def main(config_fname, geoshape_fname, fname_out, year, no_data_val=-9999, grwth_rate_fct=0.0):
    '''
    '''

    # Get raster reference project from configuration file (ref_proj)
    ref_proj = geospatial.get_raster_proj_config_file(config_fname)

    # Read the shapefile with total population data
    pop_data = gpd.read_file(geoshape_fname)

    # -------------------------------------
    # Calculate Population from 2008 Census
    # data and growth rate
    # Apply growth rate adjustment
    # -------------------------------------
    nstep = year - 2008
    pop_data['GROWTHRATE'] = pop_data['GROWTHRATE'] * (1 + grwth_rate_fct)
    pop_data['Population'] = pop_data['SS2008'] * np.exp(pop_data['GROWTHRATE'] * nstep)
    pop_data['Population'] = pop_data['Population'].apply(lambda x: np.round(x))
    pop_data['Population'] = pop_data['Population'].astype('int32')

    # -----------------------------------------------------------------------
    # Rasterize Total population along with age and gender ratios
    # Total population and age & gender ratios should go into one raster file
    # -----------------------------------------------------------------------
    (ncols, nrows) = ref_proj['ncolsrows']
    rast_meta = {
        'driver': 'GTiff',
        'height': nrows,
        'width': ncols,
        'count': 1,
        'dtype': np.int32,
        'crs': ref_proj['srs'],
        'transform': ref_proj['pixel'],
        'nodata': float(no_data_val),
    }

    # Open raster file for writing
    with open(fname_out, "wb") as fout:
        with rasterio.open(fout.name, 'w', **rast_meta) as out_raster:

            out_array = out_raster.read(1)

            # Rasterize geopandas geometries with population values
            shapes = (
                (geom, pop_values)
                for geom, pop_values in zip(pop_data['geometry'], pop_data['Population'])
            )
            burned_data = features.rasterize(
                shapes=shapes, fill=0, out=out_array, transform=out_raster.transform
            )

            out_raster.write_band(1, burned_data)

            # Tag raster bands
            band_tags = {'band_1': 'Total_Population'}
            out_raster.update_tags(**band_tags)



argparser = optparse.OptionParser()
argparser.add_option('--config', action='store', dest='config',
                     help='Config file')
argparser.add_option('--shapefile', action='store', dest='shapefile',
                     help='Shapefile')
argparser.add_option('--year', action='store', dest='year',
                     help='Year')
argparser.add_option('--outfile', action='store', dest='outfile',
                     help='Filename of the output')
(options, args) = argparser.parse_args()

if not options.config:
    print('Please specify a config file with --config')
    sys.exit(1)

if not options.shapefile:
    print('Please specify a shapefile with --shapefile')
    sys.exit(1)

if not options.year:
    print('Please specify a year with --year')
    sys.exit(1)

if not options.outfile:
    print('Please specify the name of the output with --outfile')
    sys.exit(1)

main(options.config, options.shapefile, options.outfile, int(options.year))


