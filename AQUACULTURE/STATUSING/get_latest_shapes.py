'''
Export a shapefile of the latest shapes of HarvestAreas

'''
import warnings
warnings.simplefilter(action='ignore')

import os
import geopandas as gpd
from datetime import datetime

def esri_to_gdf (aoi):
    """Returns a Geopandas file (gdf) based on 
       an ESRI format vector (shp or featureclass/gdb)"""
    
    if '.shp' in aoi: 
        gdf = gpd.read_file(aoi)
    
    elif '.gdb' in aoi:
        l = aoi.split ('.gdb')
        gdb = l[0] + '.gdb'
        fc = os.path.basename(aoi)
        gdf = gpd.read_file(filename= gdb, layer= fc)
        
    else:
        raise Exception ('Format not recognized. Please provide a shp or featureclass (gdb)!')
    
    return gdf


if __name__ == "__main__":
    gdb= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\DATASETS\aquaculture\aquaPlants_wildHarvest.gdb'
    fc= os.path.join(gdb, 'aquaPlants_wild_harvestAreas')
    
    gdf= esri_to_gdf (fc)
    
    hareas_list= ['5605', '5007', '5411', '5420', '5079', '5213']
    
    gdf=gdf[gdf['harvest_area'].isin(hareas_list)]
    
    gdf_latest = gdf.loc[gdf.groupby('harvest_area')['Year'].idxmax()]
    
    datetime= datetime.now().strftime("%Y%m%d_%H%M")
    outfolder= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20240919_wildPlants_harvest_statusing2025'
    outfile= f'{datetime}_status_shapes_2025.shp'
    
    gdf_latest.to_file(os.path.join(outfolder,outfile))
