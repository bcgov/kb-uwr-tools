import os
import json
import tempfile
import logging
import requests
import geojson
import geopandas

from arcgis.gis import GIS
from arcgis.features import FeatureSet
from arcgis.geometry import Geometry
from arcgis.geometry.filters import intersects


logger = logging.getLogger(__name__)

class ArcGIS_downloader:
    '''
    Downloads data from ArcGIS Online
    '''
    def __init__(self,url="https://www.arcgis.com",username=None,password=None) -> None:
        try:
            self.mh = GIS(url,username,password)
            logging.debug(f'Connected to ArcGIS Online as user: {self.mh.user.username}')
        except:
            logging.debug('Failed to connect to ArcGIS Online')
        pass
    def download(self, item_id,query_str = "1=1", filter_geojson=None) -> str:
        output = os.path.join(tempfile.gettempdir(),'download.geojson')
        dl_item = self.mh.content.get(item_id)
        dl_layer = dl_item.layers[0]
        if filter_geojson:
            with open(r'W:\srm\nel\Local\Geomatics\Workarea\wburt\p2023\015_hlpo_tool_upgrade\boundary_tsa_4326.geojson') as f:
                filter_json = json.load(f)
                filter = FeatureSet.from_geojson(filter_json)
            filter_geom = filter.features[0].geometry
            filter_sr = filter.spatial_reference
            filter_fun = intersects(filter_geom,filter_sr)
        else:
            filter_fun = None   
        feat_set = dl_layer.query(where = query_str,geometry_filter=filter_fun)
        with open(output,'w',encoding="utf-8") as f:
            f.write(feat_set.to_geojson)
        return
    
 
class WFS_downloader:
    ''' Downloads data from WFS
    '''
    SERVICE_URL = "https://openmaps.gov.bc.ca/geo/pub/ows?"
    PAGESIZE = 10000
    # http://openmaps.gov.bc.ca/geo/pub/wfs?request=GetCapabilities
    # https://openmaps.gov.bc.ca/geo/pub/WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY/wfs?request=GetCapabilities


    def __init__(self) -> None:
        pass

    def get_data(self, dataset,query=None, fields=None,bbox=None):
        '''Returns dataset in json format
        params:
            query: CQL formated query
            fields: comma deliminated str
            bbox: comma delimited float values in EPSG:3005 metres
        
        example usage:
        wfs = WFS_downloader
        r = wfs.get_data('WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW')
        TODO: discover OBJECTID , discover GEOMETRY Column name (SHAPE,GEOMETRY,geom,the_geom)
        '''

        pagesize = self.PAGESIZE
        r = self.wfs_query(dataset=dataset,query=query,fields=fields,bbox=bbox)
        matched = int(r.get('numberMatched'))
        returned = int(r.get('numberReturned'))
        features = (r.get('features'))
        while returned < matched:
            logging.info("")
            start_index = returned
            r = self.wfs_query(dataset=dataset,query=query,fields=fields,bbox=bbox,start_index=start_index,count=pagesize)
            returned += int(r.get('numberReturned'))
            features = features + r.get('features')
        return features    

    def wfs_query(self,dataset, query=None, fields=None,bbox=None,start_index = None, count=None): 
        '''Returns dataset in json format'''
        if fields is None:
            fields = []
        url = self.SERVICE_URL
        params = {'service':'WFS',
            'version': '2.0.0',
            'request': 'GetFeature',
            'typeName': f'pub:{dataset}',
            'outputFormat':'json',
            "srsName": "EPSG:3005",
            'sortBy':'OBJECTID',
            }
        # build optional params
        if bbox is not None and query is not None:
            # append bbox to cql
            bbox = [str(b) for b in bbox]
            bbox_str = f'BBOX(GEOMETRY,{",".join(bbox)})'
            query = f'{bbox_str} AND {query}'
        elif bbox is not None:
            bbox = [str(b) for b in bbox]
            params['bbox'] = ",".join(bbox)
        elif query is not None:
            params['CQL_FILTER']=query
                    
        if query:
            params['CQL_FILTER']=query
        if len(fields)>0:
            params['propertyName']=','.join(fields).upper()
        
        # pagenation
        if start_index:
            params['startIndex'] = start_index
        if count:
            params['count'] = count

        r = requests.get(url, params)
        logging.debug(f"WFS URL request: {r.url}")
        return r.json()
    def to_geojson(self,features,output) ->str:
        ''' Writes WFS result to geojson file
        params:
            wfs_result: json wfs response from get_data
            output: output file path (str)
        usage:
            wfs.to_geojson(r,'T:/data/airports.geojson)
        '''
        collection = geojson.FeatureCollection(features=features)
        with open(output,'w') as f:
            geojson.dump(collection,f)
    def from_geojson(self,geojson_file):
        ''' Reads GeoJson file to object
        params:
            wfs_result: json wfs response from get_data
            output: output file path (str)
        usage:
            wfs.to_geojson(r,'T:/data/airports.geojson)
        '''
        with open(geojson_file,'r') as f:
            obj = geojson.load(f)
        return obj
    
    def to_df(self,features) -> geopandas.GeoDataFrame:
        ''' Creates Geopandas GeoDataFrame from 
            WFS.get_data() result
        params:
            features: self.get_data() result 
                list of GeoJson Features
        usage:
            wfs.to_df(r,response)
        '''
        logging.debug(f'Loading {len(features)} features to GeoDataFrame')
        fc = geojson.FeatureCollection(features=features)
        df = geopandas.GeoDataFrame.from_features(fc)
        logging.debug(f'Loading Complete')
        return df
    

