"""
Snow-pack.py
Author: W.Burt
Date: October, 2023

This script creates a polygon layer with the snowpack classifciations to support analysis 
of Government Actions Regulation 8-008 established for mule deer ungulate winter range

"""

import rasterio
import os
import numpy as np
import tempfile
from osgeo import gdal
import geopandas
from shapely.geometry import shape
from rasterio.features import shapes
import logging

logging.basicConfig(level=logging.INFO)

class Snowpack:
    def __init__(self,bec,rast_dem) -> None:
        self.bec = bec
        self.dem = rast_dem
    def classify(self) -> bool:
        pass
    def generate_aspect(self,rast_dem, output = None,class_dict = {'135 >= aspect =< 270':(135,271)}) -> None:
        """
        generate_aspect creates a polygon shapefile containing the aspect classes provided

        rast_dem: elevation raster format readable by gdal / rasterio
        output: optional shapefile
        class_dict: dictionary of aspect breaks with format {name:(low break point, high breakpoints)}

        returns output

        example usage sp.generate_aspect(rast_dem = "inputs/mydem.tif",output= "outputs/aspect.shp,class_dict ={'90 to 270':(90:270)})


        """
        output_shp = os.path.join(tempfile.gettempdir(),'aspect.shp')
        output_rast = os.path.join(tempfile.gettempdir(),'aspect.tif')
        options = gdal.DEMProcessingOptions(creationOptions= ['COMPRESS=LZW',])
        gdal.DEMProcessing(output_rast,rast_dem,processing='aspect',options=options)
        result = self.classify_elevation(rast_dem=output_rast,output=output_shp,
            elevation_classes={'135 >= aspect =< 270':(135,271)})
        os.remove(output_rast)
        return result
    def classify_elevation(self, rast_dem, output = None,
        elevation_classes={'Low': (0,1000),'High': (1000,np.inf)}) -> str:
        """
        Classify_elevation creates a polygon geometry shapefile containing the elevation classes provided.

        rast_dem: The input elevation raster
        output: (optional) output shapefile name, default is None which will provide a file in a temporary location
        elevation_classes: this is a dictionary of elevation class name:(low break point, high breakpoints)
            default is {'Low': (0,1000),'High': (1000,np.inf)} .
        
        returns output file path as str

        TODO: implement COG reading or WCS? https://automating-gis-processes.github.io/CSC/notebooks/L5/read-cogs.html
        """
        logging.info("Starting to classify elevation")
        if output is None:
            output = os.path.join(tempfile.gettempdir(),'classified_dem.shp')
        outputdem = os.path.join(tempfile.gettempdir(),'classified_dem.tif')
        assert os.path.exists(rast_dem)    
        with rasterio.open(rast_dem) as src:
            logging.debug(f"Reading raster: {rast_dem}")
            dem = src.read(1)
            classified_dem = np.zeros_like(dem, dtype=np.uint8)
            for label, (min_elevation, max_elevation) in elevation_classes.items():
                logging.debug("Classify {label} --> min: {min_elevation}, max:{max_elevation}")
                classified_dem[(dem >= min_elevation) & (dem < max_elevation)] = list(elevation_classes.keys()).index(label) + 1

        logging.info("Polygonizing raster")    
        polygons = list(shapes(classified_dem, transform=src.transform))
        polygon_features = [(shape(poly), value) for poly, value in polygons]
        gdf = geopandas.GeoDataFrame(polygon_features, columns=['geometry', 'class'])
        # chnage to human labels
        def get_class(x):
            v = int(x)
            if v > 0:
                return list(elevation_classes.keys())[v-1]
            else:
                return 'None'
        gdf['class_name'] = gdf['class'].apply(get_class)
        gdf.to_file(output)
        return output


if __name__ == '__main__':
    input_dem= r"/mnt/w/srm/nel/Workarea/acagle/Data/Raster/dem25_cb.tif"
    sp = Snowpack('testbec',input_dem)
    #result = sp.classify_elevation(input_dem)
    result = sp.generate_aspect(input_dem)
    print(result)

    