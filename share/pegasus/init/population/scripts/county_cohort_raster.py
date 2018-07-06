import json
from collections import OrderedDict
import pandas as pd
from typing import Dict

import geopandas as gpd
import numpy as np
import rasterio
from rasterio import features


def main(no_data_val=-9999, fname_out='county_level_pop_out.tif', year=2017, grwth_rate_fct=0.0):
    '''
    '''
    # Get raster reference project from configuration file (ref_proj)
    with self.input()['ref_proj'].open('r') as fid:
        ref_proj = json.load(fid)

    # Read the shapefile with total population data
    shp_pop_obj = self.input()['geoshape_file']
    pop_data = gpd.read_file(shp_pop_obj.path)

    # -------------------------------------
    # Calculate Population from 2008 Census
    # data and growth rate
    # Apply growth rate adjustment
    # -------------------------------------
    nstep = self.year - 2008
    pop_data['GROWTHRATE'] = pop_data['GROWTHRATE'] * (1 + self.grwth_rate_fct)
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
        'nodata': -9999.0,
    }

    # Open raster file for writing
    with self.output().open("wb") as fout:
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


class CreateCountyLevelCohorts(luigi.Task):

    ''' '''
    no_data_val = luigi.IntParameter(default=-9999)
    fname_out = luigi.Parameter(default='county_level_cohort_out.tif')
    year = luigi.Parameter(default=2017)

    def requires(self):
        return NotImplementedError(
            'Method must specify \n'
            '{geoshape_file: Shapefile with aggregated population data,\n'
            'age_gender_file: csv file with age and gender cohort population,\n'
            'ref_proj: Reference project for raster}'
        )

    def output(self):
        '''
        Write County level population along with age and gender ratios to raster file:

        band 1 -- 'Population', band 2 -- '0-4_Total', band 3 -- '5-9_Total',
        band 4 -- '10-14_Total', band 5 -- '15-19_Total', band 6 -- '20-24_Total',
        band 7 -- '25-29_Total', band 8 -- '30-34_Total', band 9 -- '35-39_Total',
        band 10 -- '40-44_Total', band 11 -- '45-49_Total', band 12 -- '50-54_Total',
        band 13 -- '55-59_Total', band 14 -- '60-64_Total', band 15 -- '65-69_Total',
        band 16 -- '70-74_Total', band 17 -- '75-79_Total', band 18 -- '80-84_Total',
        band 19 -- '85-89_Total', band 20 -- '90-94_Total', band 21 -- '95-99_Total',
        band 22 -- '100+_Total', band 23 -- 'Total_Female', band 24 -- '0-4_Female',
        band 25 -- '5-9_Female', band 26 -- '10-14_Female', band 27 -- '15-19_Female',
        band 28 -- '20-24_Female', band 29 -- '25-29_Female', band 30 --'30-34_Female',
        band 31 -- '35-39_Female', band 32 -- '40-44_Female', band 33 -- '45-49_Female',
        band 34 -- '50-54_Female', band 35 -- '55-59_Female', band 36 -- '60-64_Female',
        band 37 -- '65-69_Female', band 38 -- '70-74_Female', band 39 -- '75-79_Female',
        band 40 -- '80-84_Female', band 41 -- '85-89_Female', band 42 -- '90-94_Female',
        band 43 -- '95-99_Female', band 44 -- '100+_Female', band 45 -- 'Total_Male',
        band 46 -- '0-4_Male', band 47 -- '5-9_Male', band 48 --'10-14_Male',
        band 49 -- '15-19_Male', band 50 -- '20-24_Male', band 51 --'25-29_Male',
        band 52 -- '30-34_Male', band 53 -- '35-39_Male', band 54 --'40-44_Male',
        band 55 -- '45-49_Male', band 56 -- '50-54_Male', band 57 -- '55-59_Male',
        band 58 -- '60-64_Male', band 59 -- '65-69_Male', band 60 -- '70-74_Male',
        band 61 -- '75-79_Male', band 62 -- '80-84_Male', band 63 -- '85-89_Male',
        band 64 -- '90-94_Male', band 65 -- '95-99_Male', band 66 -- '100+_Male'
        '''
        return LocalTarget(path=self.fname_out, format=luigi.format.Nop)

    def run(self):
        '''
        '''
        # Get raster reference project from configuration file (ref_proj)
        with self.input()['ref_proj'].open('r') as fid:
            ref_proj = json.load(fid)

        # Read the shapefile with total population data
        shp_pop_obj = self.input()['geoshape_file']
        pop_data = gpd.read_file(shp_pop_obj.path)

        # Read the csv file with age and gender cohort populations
        cohort_pop_obj = self.input()['age_gender_file']
        age_gender_data = pd.read_csv(cohort_pop_obj.path)
        age_gender_data.drop('Year', inplace=True, axis=1)

        # Normalize age and gender breakdown (Create fractions) according to total population
        total_pop = age_gender_data['Total'].iloc[0]
        age_gender_data['Total'] = age_gender_data['Total']/total_pop
        age_gender_data['Male'] = age_gender_data['Male']/total_pop
        age_gender_data['Female'] = age_gender_data['Female']/total_pop

        # Create age and gender maps with the same areas defined in total population
        for row in age_gender_data.itertuples():
            pop_data[row.Age+'_Total'] = np.ones(pop_data.shape[0]) * row.Total
            pop_data[row.Age+'_Male'] = np.ones(pop_data.shape[0]) * row.Male
            pop_data[row.Age+'_Female'] = np.ones(pop_data.shape[0]) * row.Female

        # Rasterize Total population along with age and gender ratios
        # Total population and age & gender ratios should go into one raster file
        (ncols, nrows) = ref_proj['ncolsrows']
        rast_meta = {
            'driver': 'GTiff',
            'height': nrows,
            'width': ncols,
            'count': 65,
            'dtype': np.float32,
            'crs': ref_proj['srs'],
            'transform': ref_proj['pixel'],
            'nodata': -9999.0,
        }
        # Tags for raster bands
        clmn_names = [
           '0-4_Total', '5-9_Total', '10-14_Total', '15-19_Total',
           '20-24_Total', '25-29_Total', '30-34_Total', '35-39_Total',
           '40-44_Total', '45-49_Total', '50-54_Total', '55-59_Total',
           '60-64_Total', '65-69_Total', '70-74_Total', '75-79_Total',
           '80-84_Total', '85-89_Total', '90-94_Total', '95-99_Total',
           '100+_Total', 'Total_Female', '0-4_Female', '5-9_Female', '10-14_Female',
           '15-19_Female', '20-24_Female', '25-29_Female', '30-34_Female',
           '35-39_Female', '40-44_Female', '45-49_Female', '50-54_Female',
           '55-59_Female', '60-64_Female', '65-69_Female', '70-74_Female',
           '75-79_Female', '80-84_Female', '85-89_Female', '90-94_Female',
           '95-99_Female', '100+_Female', 'Total_Male', '0-4_Male', '5-9_Male', '10-14_Male',
           '15-19_Male', '20-24_Male', '25-29_Male', '30-34_Male', '35-39_Male', '40-44_Male',
           '45-49_Male', '50-54_Male', '55-59_Male', '60-64_Male', '65-69_Male',
           '70-74_Male', '75-79_Male', '80-84_Male', '85-89_Male', '90-94_Male',
           '95-99_Male', '100+_Male'
        ]

        # Open raster file for writing
        with self.output().open("wb") as fout:
            with rasterio.open(fout.name, 'w', **rast_meta) as out_raster:

                out_array = out_raster.read(1)

                # Loop through population and age and gender ratios
                for i, clmn in enumerate(clmn_names):

                    # Rasterize geopandas geometries with population values
                    shapes = (
                        (geom, pop_values)
                        for geom, pop_values in zip(pop_data['geometry'], pop_data[clmn])
                    )
                    burned_data = features.rasterize(
                        shapes=shapes, fill=0, out=out_array, transform=out_raster.transform
                    )

                    out_raster.write_band(i+1, burned_data)

                # Tag raster bands
                band_tags = OrderedDict({'band_'+str(i+1): val for i, val in enumerate(clmn_names)})
                out_raster.update_tags(**band_tags)
