'''
This script creates a HTML map containing 
all the Feature classes stored in the 
one_status_common_datasets geodatabase 
'''

import timeit
import os
import sys
#import arcpy
import geopandas as gpd
import numpy as np
import fiona
import folium
from folium.plugins import MeasureControl
from folium.plugins import MousePosition
from folium.features import LatLngPopup


def add_proj_lib ():
    """
    FIX: Geopandas not pointing to pyproj library.
    Checks if pyproj is in env path. if not, add it.
    """
    proj_lib = os.path.join(sys.executable[:-10], r'Library\share\proj')
    if proj_lib not in os.environ['path']:
        os.environ["proj_lib"] = proj_lib
    else:
        pass


def create_map_template():
    """Returns a folium map object"""
    
    map_obj = folium.Map()
    
    # Add the GeoBC basemap to the map
    wms_url = 'https://maps.gov.bc.ca/arcgis/rest/services/province/web_mercator_cache/MapServer/tile/{z}/{y}/{x}'
    wms_attribution = 'GeoBC, DataBC, TomTom, © OpenStreetMap Contributors'
    folium.TileLayer(
        tiles=wms_url,
        name='GeoBC Basemap',
        attr=wms_attribution,
        overlay=False,
        control=True,
        transparent=True).add_to(map_obj)
    
    # Add a satellite basemap to the map
    satellite_url = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
    satellite_attribution = 'Tiles &copy; Esri'
    folium.TileLayer(
        tiles=satellite_url,
        name='Imagery Basemap',
        attr=satellite_attribution,
        overlay=False,
        control=True).add_to(map_obj)
    
    # add measure controls to the map
    map_obj.add_child(MeasureControl())
    
    # add mouse psotion to the map
    MousePosition().add_to(map_obj)
    
    # add the LatLngPopup plugin to the map
    map_obj.add_child(LatLngPopup())
    
    return map_obj


    
def generate_html_maps(status_gdb,map_obj):
    """Creates a HTML map for each feature class in gdb"""


    # List all feature classes
    # Can replace with arcpy.ListFeatureClasses(). Fiona is faster!
    fc_list = fiona.listlayers(status_gdb)
    #arcpy.env.workspace = status_gdb
    #fc_list = arcpy.ListFeatureClasses()
    
    # Read the AOI feature class into a gdf 
    gdf_aoi = gpd.read_file(filename= status_gdb, layer= 'aoi')
    
    # Initiate a Map object and set extent based on the layer 
    
    xmin,ymin,xmax,ymax = gdf_aoi.to_crs(4326)['geometry'].total_bounds
    
    map_obj.fit_bounds([[ymin, xmin], [ymax, xmax]])
    
    
    # Add the AOI layer to the folium map
    folium.GeoJson(data=gdf_aoi, name='AOI',
                   style_function=lambda x:{'color': 'red',
                                            'weight': 3}).add_to(map_obj)
  
 
    # Remove the aoi and aoi buffers from the list of feature classes
    fc_list = [x for x in fc_list if 'aoi' not in x]
    
    # Loop through the feature classes and make maps
    counter = 1
    for fc in fc_list:
        # Read the feature class into a gdf 
        gdf_fc = gpd.read_file(filename= status_gdb, layer= fc)
        
        print ("Adding Layer {0} of {1}: {2}".format(counter,len(fc_list),fc))
        
        # Make sure the layer is not empty
        if gdf_fc.shape[0] > 0:
            # Set label column. Will be used for tooltip and legend.
            # Replace this with the label column from the tool inputs
            label_col = gdf_fc.columns[gdf_fc.columns.get_loc('label_field') + 1] 
        
            # Remove the Geometry column from the popup window. 
            # Popup columns can be set to the list of columns from the tool inputs (summarize columns)
            popup_cols = list(gdf_fc.columns)                     
            popup_cols.remove('geometry') 
            
            # Format the popup columns for better visulization
            for col in popup_cols:
                gdf_fc[col] = gdf_fc[col].astype(str)
                gdf_fc[col] = gdf_fc[col].str.wrap(width=20).str.replace('\n','<br>')
            

            
            # Assign random colors to the features (for legend)
            for x in gdf_fc.index:
                color = np.random.randint(16, 256, size=3)
                color = [str(hex(i))[2:] for i in color]
                color = '#'+''.join(color).upper()
                gdf_fc.at[x, 'color'] = color
                
            # Add the main layer to the folium map
            folium.GeoJson(data=gdf_fc,
                           name=fc,
                           style_function= lambda x: {'fillColor': x['properties']['color'],
                                                      'color': x['properties']['color'],
                                                      'weight': 2},
                           tooltip=folium.features.GeoJsonTooltip(fields=[label_col], 
                                                                  labels=True),
                           popup=folium.features.GeoJsonPopup(fields=popup_cols, 
                                                              sticky=False,
                                                              max_width=380)).add_to(map_obj)
            
        counter += 1 
     
    # add layer controls to the map
    folium.LayerControl().add_to(map_obj) 
    
    # Save the interavtive map to html file
    out_loc = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\TOOLS\SCRIPTS\STATUSING\fc_to_html\map_all'
    map_obj.save(os.path.join(out_loc, 'map_all.html'))
            


if __name__==__name__:
    # Execute the function and track processing time
    start_t = timeit.default_timer() #start time
    
    add_proj_lib ()

    # This is an example of a one_status_common_datasets geodatabase
    status_gdb = r'\\spatialfiles.bcgov\work\lwbc\visr\Workarea\FCBC_VISR\Lands_Statusing\1414630\one_status_common_datasets_aoi.gdb'
    
    map_obj = create_map_template()
    
    generate_html_maps(status_gdb,map_obj)
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print ('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))
