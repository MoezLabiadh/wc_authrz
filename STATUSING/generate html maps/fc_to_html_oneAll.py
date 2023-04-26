'''

This script generates interavtive HTML maps 
for the AST/UOT

Version: 2

'''

import timeit
import os
import sys
#import arcpy
import fiona
import numpy as np
import geopandas as gpd
import folium
from folium.plugins import MeasureControl, MousePosition



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



def create_map_template(map_title='Placeholder for title'):
    """Returns an empty folium map object"""
    # Create a map object
    map_obj = folium.Map()
    
    # Add GeoBC basemap to the map
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
    
    # create a title and button to refresh the map to the initial view
    title_refresh = """
    <div style="position: fixed; 
         top: 8px; left: 70px; width: 150px; height: 70px; 
         background-color:transparent; border:0px solid grey;z-index: 900;">
         <h5 style="font-weight:bold;color:#DE1610;white-space:nowrap;">{}</h5>
        <button style="font-weight:bold; color:#DE1610" 
                onclick="location.reload()">Refresh View</button>
    </div>
    """.format(map_title)
    
    map_obj.get_root().html.add_child(folium.Element(title_refresh))
    
    # Add measure controls to the map
    map_obj.add_child(MeasureControl(primary_length_unit='meters', 
                                     secondary_length_unit='kilometers',
                                     primary_area_unit='hectares', 
                                     secondary_area_unit='square kilometers'))
    
    # Add mouse psotion to the map
    MousePosition().add_to(map_obj)

    # Add Lat/Long Popup to the map
    map_obj.add_child(folium.features.LatLngPopup())
    
    return map_obj



def generate_html_maps(status_gdb):
    """Creates a HTML map for each feature class in gdb"""
    
    print ('Preparing Layers for mapping')
    # List all feature classes
    # Can replace with arcpy.ListFeatureClasses(). Fiona is faster!
    fc_list = fiona.listlayers(status_gdb)
    #arcpy.env.workspace = status_gdb
    #fc_list = arcpy.ListFeatureClasses()
    
    # Read the AOI feature class into a gdf 
    gdf_aoi = gpd.read_file(filename= status_gdb, layer= 'aoi')
    # Make a list of layers except aoi and aoi buffers
    ly_list = [x for x in fc_list if 'aoi' not in x]

    # Make a list of buffered areas
    #bf_list = [x for x in fc_list if 'aoi_' in x]
    bf_list = ['aoi_500','aoi_1000','aoi_5000']
    
    # Create a dict of buffered gdfs
    bf_gdfs = {bf: gpd.read_file(filename=status_gdb, layer=bf) for bf in bf_list}

    print ('Creating a map template')
    # Create an all-layers map
    map_all = create_map_template(map_title='Overview Map - All Overlaps')
    
    # Add the AOI layer to the all-layers map
    folium.GeoJson(data=gdf_aoi, name='AOI',
                   style_function=lambda x:{'color': 'red', 
                                            'fillColor': 'none',
                                            'weight': 3}).add_to(map_all)
    
    # Zoom the all-layers map to the AOI extent
    xmin,ymin,xmax,ymax = gdf_aoi.to_crs(4326)['geometry'].total_bounds
    map_all.fit_bounds([[ymin, xmin], [ymax, xmax]])
    

 
    # Add buffered areas to the all-layers maps
    for k,v in bf_gdfs.items():
            folium.GeoJson(data=v, name=k, show=False,
                           style_function=lambda x:{'color': 'orange',
                                                    'fillColor': 'none',
                                                    'weight': 3}).add_to(map_all)
    # Loop through the rest of layers and make maps
    counter = 1
    for fc in ly_list:
        # Read the feature class into a gdf 
        gdf_fc = gpd.read_file(filename= status_gdb, layer= fc)
        
        print ("Creating Map {0} of {1}: {2}".format(counter,len(ly_list),fc))
        
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
                
            # Create an individual map
            map_title = fc.replace('_', ' ')
            map_one = create_map_template(map_title=map_title)
            
            # Create a list of columns for the tooltip
            gdf_fc['map_title'] = map_title
            tooltip_cols = ['map_title',label_col]
            
            # Add the AOI layer to the individual map
            folium.GeoJson(data=gdf_aoi, name='AOI',
                           style_function=lambda x:{'color': 'red',
                                                    'fillColor': 'none',
                                                    'weight': 3}).add_to(map_one)
            
            # Zoom the map to the layer extent
            xmin,ymin,xmax,ymax = gdf_fc.to_crs(4326)['geometry'].total_bounds
            map_one.fit_bounds([[ymin, xmin], [ymax, xmax]])
                
            # Add the main layer to the folium maps
            for mp in [map_one, map_all]:
                folium.GeoJson(data=gdf_fc, name=map_title,
                               style_function= lambda x: {'fillColor': x['properties']['color'],
                                                          'color': x['properties']['color'],
                                                          'weight': 2},
                               tooltip=folium.features.GeoJsonTooltip(fields=tooltip_cols,
                                                                      aliases=['LAYER', label_col],
                                                                      labels=True),
                               popup=folium.features.GeoJsonPopup(fields=popup_cols, 
                                                                  sticky=False,
                                                                  max_width=380)).add_to(mp)

            # Add buffered areas to the individual maps
            for k,v in bf_gdfs.items():
                    folium.GeoJson(data=v, name=k, show=False,
                                   style_function=lambda x:{'color': 'orange',
                                                            'fillColor': 'none',
                                                            'weight': 3}).add_to(map_one)

            # Create a Legend
            #legend colors and names
            legend_labels = zip(gdf_fc['color'], gdf_fc[label_col])
            
            #start the div tag and set the legend size and position
            legend_html = '''
                        <div id="legend" style="position: fixed; 
                        bottom: 50px; right: 50px; z-index: 1000; 
                        background-color: #fff; padding: 10px; 
                        border-radius: 5px; border: 1px solid grey;">
                        '''
                        
            #add the AOI item to the legend
            legend_html += '''
                        <div style="display: inline-block; 
                        margin-right: 10px;
                        background-color: transparent;
                        border: 2px solid red;
                        width: 15px; height: 15px;"></div>AOI<br>
                        '''
                        
            #add the AOI buffer item to the legend
            legend_html += '''
                        <div style="display: inline-block; 
                        margin-right: 10px;background-color: transparent; 
                        border: 2px solid orange;
                        width: 15px; height: 15px;"></div>AOI buffers<br>
                        '''            
            
            #add a header to the legend            
            legend_html += '''
                        <div style="font-weight: bold; 
                        margin-bottom: 5px;">{}</div>
                        '''.format(label_col)
        
            #add items to the legend
            for color, name in legend_labels:
                legend_html += '''
                                <div style="display: inline-block; 
                                margin-right: 10px;background-color: {0}; 
                                width: 15px; height: 15px;"></div>{1}<br>
                                '''.format(color, name)
            #close the div tag
            legend_html += '</div>'
            
            #add the legend to the map
            map_one.get_root().html.add_child(folium.Element(legend_html))
           
            # Add layer controls to the individual map
            lyr_cont_one = folium.LayerControl()
            lyr_cont_one.add_to(map_one)
        
            # Save the indivdiual map to html file
            out_loc = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\TOOLS\SCRIPTS\STATUSING\fc_to_html\maps'
            map_one.save(os.path.join(out_loc, fc+'.html'))
            
        counter += 1
    
    # Add layer controls to the all-layers map
    lyr_cont_all = folium.LayerControl()
    lyr_cont_all.add_to(map_all)
    
    # Save the all-layers map to html file
    map_all.save(os.path.join(out_loc, '00_all_layers.html'))




if __name__==__name__:

    # Execute the function and track processing time
    start_t = timeit.default_timer() #start time
    
    add_proj_lib ()
    
    # This is an example of a one_status_common_datasets geodatabase
    #file_nbr='1414630'
    status_gdb = r'\\spatialfiles.bcgov\work\lwbc\visr\Workarea\FCBC_VISR\Lands_Statusing\1414630\one_status_common_datasets_aoi.gdb'
    
    generate_html_maps(status_gdb)
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print ('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))
