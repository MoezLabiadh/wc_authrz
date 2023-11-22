import geopandas as gpd
from requests import Request


def wfs_to_gdf (crs, bbox, layername):
    """Returns a geodataframe from a WFS GetFeature call """
    
    owsrootUrl= 'http://openmaps.gov.bc.ca/geo/ows?'
    
    params = dict(service='WFS', 
                  version="2.0.0", 
                  request='GetFeature',
                  typeName= f'pub:{layername}', 
                  SrsName=f'{crs}',
                  bbox=f'{",".join(str(value) for value in bbox)},{crs}',
                  outputFormat='json')
    
    request_url = Request('GET', 
                          owsrootUrl, 
                          params=params).prepare().url
    
    
    return gpd.read_file(request_url)


#test the function
crs= 'EPSG:4326'

xmin= -126.25
ymin= 49.11
xmax= -124.23
ymax= 50.61
bbox= (xmin,ymin,xmax,ymax)

layername= 'WHSE_WATER_MANAGEMENT.WLS_WATER_RIGHTS_LICENCES_SV'


gdf= wfs_to_gdf (crs, bbox, layername)


