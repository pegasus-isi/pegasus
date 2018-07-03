#!/usr/bin/python3

import configparser
import logging
from typing import Dict

from osgeo import ogr, gdal, osr

logger = logging.getLogger('geospatial')

#def open_raster(raster_file):
#    """
#    Open raster file using GDAL
#    """
#    raster = gdal.Open(raster_file)
#    return raster
#
#
#def close_raster(raster):
#    """
#    Close raster file
#    """
#    raster = None
#
#
#class GetRasterProj(luigi.Task):
#    '''
#        Get projection and spatial system of reference raster file:
#        -- Spatial reference system
#        -- Origin X,Y
#        -- Pixel width, height
#        -- Number rows, cols
#
#        This data is used in creating the new raster file with the same spatial characteristics
#        as the reference raster file.
#    '''
#
#    def requires(self) -> luigi.Target:
#        return NotImplementedError('Subclasses must specify an ERSI Shapefile Target')
#
#    def output(self):
#        return ExpiringMemoryTarget(name='raster_proj', timeout=5)
#
#    def run(self):
#        projdata = {}
#
#        # ----------------------------------------------------------
#        # Opening the raster file can not be a separate Luigi task,
#        # because the return object of gdal read is of Swig type
#        # which cannot be pickled. Also, maybe it doesn't make sense
#        # passing a large raster file as a parameter
#        # ----------------------------------------------------------
#        file_obj = self.input()
#        with file_obj.open('r') as fobj:
#            fname = fobj.name
#            raster = open_raster(fname)
#
#        # ----------------------------
#        # Get spatial reference system
#        # ----------------------------
#        srs_wkt = raster.GetProjectionRef()
#
#        # ----------------------------
#        # Get grid (pixel) coordinates
#        # ----------------------------
#        geotransform = raster.GetGeoTransform()
#        originx = geotransform[0]
#        originy = geotransform[3]
#        pixelwidth = geotransform[1]
#        pixelheight = geotransform[5]
#
#        # ------------------------------
#        # Find number of rows and pixels
#        # ------------------------------
#        ncols = raster.RasterXSize
#        nrows = raster.RasterYSize
#        close_raster(raster)
#
#        projdata['srs'] = srs_wkt
#        projdata['pixel'] = (originx, pixelwidth, 0, originy, 0, pixelheight)
#        projdata['ncolsrows'] = (ncols, nrows)
#
#        self.output().put(projdata)


def get_raster_proj_config_file(fname):
    '''
    Get projection and spatial system from a configuration file:
    -- Spatial reference system
    -- Origin X,Y
    -- Pixel width, height
    -- Number rows, cols

    This data is used in creating the new raster file with the same spatial characteristics
    as the reference raster file.
    '''

    projdata = {}

    config = configparser.ConfigParser()
    config.read(fname)

    # ----------------------------
    # Get spatial reference system
    # ----------------------------
    srs_wkt = config['projection']['srs']
    wkp = config['projection']['wkp']
    originx = config.getfloat('projection', 'originx')
    originy = config.getfloat('projection', 'originy')
    pixel_width = config.getfloat('projection', 'pixel_width')
    pixel_height = config.getfloat('projection', 'pixel_height')
    ncols = config.getint('projection', 'ncols')
    nrows = config.getint('projection', 'nrows')

    projdata['srs'] = srs_wkt
    projdata['wkp'] = wkp
    projdata['pixel'] = (originx, pixel_width, 0, originy, 0, pixel_height)
    projdata['ncolsrows'] = (ncols, nrows)

    return projdata


#class Raster2Array(luigi.Task):
#    '''
#    Convert raster file to numpy array
#    '''
#    def requires(self) -> luigi.Target:
#        return NotImplementedError('Subclasses must specify an raster Target')
#
#    def output(self):
#        return ExpiringMemoryTarget(name='raster_array', timeout=10)
#
#    def run(self):
#
#        # ----------------------------------------------------------
#        # Opening the raster file can not be a separate Luigi task,
#        # because the return object of gdal read is of Swig type
#        # which cannot be pickled. Also, maybe it doesn't make sense
#        # passing a large raster file as a parameter
#        # ----------------------------------------------------------
#        file_obj = self.input()
#        with file_obj.open('r') as fobj:
#            fname = fobj.name
#            raster = open_raster(fname)
#
#        band = raster.GetRasterBand(1)
#        rast_array = band.ReadAsArray()
#        close_raster(raster)
#        self.output().put(rast_array)
#
#
#class Array2Raster(luigi.Task):
#    '''Converts a numpy array to a raster file (geo-tiff)'''
#
#    no_data_val = luigi.IntParameter(default=-9999)
#    proj_type = luigi.Parameter(default='wkp')
#    fname_out = luigi.Parameter(default='out.tif')
#
#    def requires(self) -> Dict[str, luigi.Target]:
#        return NotImplementedError('Subclasses must specify \n'
#                                   '{ref_proj: Reference projection for raster,\n'
#                                   'array: Array to be converted to raster}')
#
#    def output(self):
#
#        return LocalTarget(path=self.fname_out, format=luigi.format.Nop)
#
#    def run(self):
#
#        ref_proj = self.input()['ref_proj'].get()
#        array = self.input()['array'].get()
#        (ncols, nrows) = ref_proj['ncolsrows']
#
#        driver = gdal.GetDriverByName('GTiff')
#        with self.output().open("wb") as fout:
#            out_raster = driver.Create(fout.name, ncols, nrows, 1, gdal.GDT_Float32)
#            out_raster.SetGeoTransform(ref_proj['pixel'])
#            outband = out_raster.GetRasterBand(1)
#            outband.SetNoDataValue(self.no_data_val)
#            outband.WriteArray(array, 0, 0)
#
#            proj = osr.SpatialReference()
#
#            if self.proj_type.lower() == 'wkp':
#                proj.SetWellKnownGeogCS(ref_proj['wkp'])
#
#            elif self.proj_type.lower() == 'srs':
#                proj.ImportFromWkt(ref_proj['srs'])
#
#            else:
#                logger.error("Unrecongnized projection type for creating output raster file, "
#                             "must be wkp or srs")
#                raise Exception("Unrecongnized projection type for creating output raster file, "
#                                "must be wkp or srs")
#
#            out_raster.SetProjection(proj.ExportToWkt())
#            outband.FlushCache()
#
#            close_raster(driver)
#            close_raster(out_raster)
#            close_raster(outband)
#
#
#class RasterizeVectorFile(luigi.Task):
#    """
#    Base class for rasterizing a vector file.
#    -- Vector file should be an ERSI Shapefile
#    -- Raster file will be a geotiff
#
#    TO DO: Add support for other raster and vector file types
#    """
#
#    no_data_val = luigi.IntParameter(default=-9999)  # No data value for raster
#    fname_out = luigi.Parameter(default='out.tif')
#    proj_type = luigi.Parameter(default='wkp')
#    attr_field = luigi.Parameter()
#
#    def requires(self) -> Dict[str, luigi.Target]:
#        return NotImplementedError('Subclasses must specify \n'
#                                   '{ref_proj: Reference projection for raster,\n'
#                                   'vector_file: Vector file name (and path)}')
#
#    def output(self):
#
#        return LocalTarget(path=self.fname_out, format=luigi.format.Nop)
#
#    def run(self):
#
#        # prj_wkt = '+proj=longlat +datum=WGS84 +no_defs'
#        ref_proj = self.input()['ref_proj'].get()
#        f_vector = self.input()['vector_file']
#
#        (ncols, nrows) = ref_proj['ncolsrows']
#
#        driver = ogr.GetDriverByName("ESRI Shapefile")
#
#        with self.output().open("wb") as fout:
#            with f_vector.open('r') as fobj:
#                fname = fobj.name
#
#            data_source = driver.Open(fname, 0)
#            layer = data_source.GetLayer()
#
#            target_ds = gdal.GetDriverByName('GTiff').Create(
#                fout.name, ncols, nrows, 1, gdal.GDT_Float32)
#
#            target_ds.SetGeoTransform(ref_proj['pixel'])
#
#            proj = osr.SpatialReference()
#            if self.proj_type.lower() == 'wkp':
#                proj.SetWellKnownGeogCS(ref_proj['wkp'])
#
#            elif self.proj_type.lower() == 'srs':
#                proj.ImportFromWkt(ref_proj['srs'])
#
#            else:
#                logger.error("Unrecongnized projection type for creating output raster file, "
#                             "must be wkp or srs")
#                raise Exception("Unrecongnized projection type for creating output raster file, "
#                                "must be wkp or srs")
#
#            target_ds.SetProjection(proj.ExportToWkt())
#
#            band = target_ds.GetRasterBand(1)
#            band.SetNoDataValue(self.no_data_val)
#            band.FlushCache()
#
#            gdal.RasterizeLayer(target_ds, [1], layer,
#                                options=["ATTRIBUTE=%s" % self.attr_field])
#            close_raster(data_source)
#            close_raster(layer)
#            close_raster(target_ds)
#            close_raster(band)
