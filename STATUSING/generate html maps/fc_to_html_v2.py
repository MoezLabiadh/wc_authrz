'''

This script generates interavtive HTML maps 
for the AST/UOT.

Update: April 24, 2024.

'''
import warnings
warnings.simplefilter(action='ignore')

import timeit
import os
import sys
#import arcpy
import fiona
import base64
import numpy as np
import geopandas as gpd
import shapely.wkt as wkt
import folium
from folium.plugins import MeasureControl, MousePosition,FloatImage, MiniMap



class HTMLGenerator:
    def __init__(self, status_gdb, label_col, out_location):
        self.status_gdb = status_gdb
        self.label_col = label_col
        self.out_loc = out_location
        self.add_proj_lib()


    def add_proj_lib (self):
        """
        FIX: pyproj not pointing to proj.db database.
        Checks if proj repo is in env path. if not, add it.
        """
        proj_lib = os.path.join(sys.executable[:-10], r'Library\share\proj')
        if proj_lib not in os.environ['path']:
            os.environ["PROJ_LIB"] = proj_lib
        else:
            pass



    def create_map_template(self,map_title='Placeholder for title',Xcenter=0,Ycenter=0):
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
        map_var_nme = map_obj.get_name()
        
        title_refresh = """
        <div style="position: fixed; 
            top: 8px; left: 70px; width: 150px; height: 70px; 
            background-color:transparent; border:0px solid grey;z-index: 900;">
            <h5 style="font-weight:bold;color:#DE1610;white-space:nowrap;">{}</h5>
            <button style="font-weight:bold; color:#DE1610" 
                    onclick="{}.setView([{}, {}], 16)">Refresh View</button>
        </div>

        """.format(map_title,map_var_nme,Ycenter,Xcenter)
        
        map_obj.get_root().html.add_child(folium.Element(title_refresh))
        
        # Add measure controls to the map
        map_obj.add_child(MeasureControl(primary_length_unit='meters', 
                                        secondary_length_unit='kilometers',
                                        primary_area_unit='hectares'))
        
        # Add mouse psotion to the map
        MousePosition().add_to(map_obj)

        # Add Lat/Long Popup to the map
        map_obj.add_child(folium.features.LatLngPopup())

        
        # Add logo to the map
        logo_path = (r"\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\MAPS\Logos\BCID_V_key_pms_pos_small_150px.JPG")
        
        b64_content = base64.b64encode(open(logo_path, 'rb').read()).decode('utf-8')


        float_image = FloatImage('data:image/png;base64,{}'.format(b64_content), bottom=3, left=2)
        float_image.add_to(map_obj)
        
        #Add a Mini Map
        minimap = MiniMap(position="bottomright")
        map_obj.add_child(minimap)
        
        return map_obj



    def generate_html_maps(self):
        """Creates a HTML map for each feature class in gdb"""
        
        print ('Preparing Layers for mapping')
        # List all feature classes
        # Can replace with arcpy.ListFeatureClasses(). Fiona is faster!
        fc_list = fiona.listlayers(self.status_gdb)
        #arcpy.env.workspace = self.status_gdb
        #fc_list = arcpy.ListFeatureClasses()
        
        # Read the AOI feature class into a gdf 
        gdf_aoi = gpd.read_file(filename=self.status_gdb, layer= 'aoi')
        # Make a list of layers except aoi and aoi buffers
        ly_list = [x for x in fc_list if 'aoi' not in x]

        # Create a dict of buffered gdfs
        bf_gdfs= {'aoi_500': gpd.GeoDataFrame(geometry= gdf_aoi.buffer(500), crs= gdf_aoi.crs), 
                  'aoi_1000': gpd.GeoDataFrame(geometry= gdf_aoi.buffer(1000), crs= gdf_aoi.crs), 
                  'aoi_5000': gpd.GeoDataFrame(geometry= gdf_aoi.buffer(5000), crs= gdf_aoi.crs) 
                  }
        
        #bf_gdfs = {bf: gpd.read_file(filename=self.status_gdb, layer=bf) for bf in bf_list}

        print ('Creating a map template')
        # Create an all-layers map
        centroids = gdf_aoi.to_crs(4326).centroid
        Xcenter = centroids.x[0]
        Ycenter = centroids.y[0]

        map_all = self.create_map_template(map_title='Overview Map - All Overlaps',
                                    Xcenter=Xcenter,Ycenter=Ycenter)
             
        # Add the AOI layer to the all-layers map
        folium.GeoJson(data=gdf_aoi, name='AOI',
                    style_function=lambda x:{'color': 'red', 
                                                'fillColor': 'none',
                                                'weight': 3}).add_to(map_all)
        
        # Zoom the all-layers map to the AOI extent
        xmin,ymin,xmax,ymax = bf_gdfs.get('aoi_1000').to_crs(4326)['geometry'].total_bounds
        map_all.fit_bounds([[ymin, xmin], [ymax, xmax]])
        
    
        # Add buffered areas to the all-layers maps
        for k,v in bf_gdfs.items():
                folium.GeoJson(data=v, name=k, show=True,
                               style_function=lambda x:{'color': 'orange',
                                                        'fillColor': 'none',
                                                        'weight': 3}).add_to(map_all)
        # Loop through the rest of layers and make maps
        counter = 1
        for fc in ly_list:
        
            # Read the feature class into a gdf 
            gdf_fc = gpd.read_file(filename= self.status_gdb, layer= fc)
            
            print ("Creating Map {0} of {1}: {2}".format(counter,len(ly_list),fc))
            
            # Make sure the layer is not empty
            if gdf_fc.shape[0] > 0:
                
                #Convert Point geometries to Polys.
                # workaround issue with Folium/Geojon tooltips on Points only.
                #if gdf_fc.geometry.geom_type.iloc[0] == 'Point':
                 #   gdf_fc['geometry'] = gdf_fc.geometry.buffer(20)
                   
                #Flatten 3D geometries to 2D (Folium has issues with 3D)    
                if gdf_fc['geometry'].has_z.any():
                    gdf_fc['geometry'] = gdf_fc['geometry'].apply(
                                lambda geom: wkt.loads(
                                    wkt.dumps(geom, output_dimension=2)))
                    
                #convert all cols to str except geometry
                for col in gdf_fc.columns:
                    if col != 'geometry':
                        gdf_fc[col] = gdf_fc[col].astype(str)
                     
                # Set label column. Will be used for tooltip and legend.
                # Replace this with the label column from the tool inputs
                if (self.label_col== None) or (self.label_col== ''):
                    label_col = gdf_fc.columns[0] 
                else:
                    label_col= self.label_col
                    
            
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
                
                gdf_fc['color2']=color
                    
                # Create an individual map
                map_title = fc.replace('_', ' ')
                map_one = self.create_map_template(map_title=map_title,
                                            Xcenter=Xcenter,Ycenter=Ycenter)
                
                # Create a list of columns for the tooltip
                gdf_fc['map_title'] = map_title
                tooltip_cols = ['map_title',label_col]
                
                # Add the AOI layer to the individual map
                folium.GeoJson(data=gdf_aoi, name='AOI',
                            style_function=lambda x:{'color': 'red',
                                                        'fillColor': 'none',
                                                        'weight': 3}).add_to(map_one)
                  
                # Zoom the map to the layer extent
                gdf_fc = gdf_fc.to_crs(4326)
                xmin, ymin, xmax, ymax = gdf_fc['geometry'].total_bounds
                map_one.fit_bounds([[ymin, xmin], [ymax, xmax]])
                

                # Add the layer to the individual map  
                folium.GeoJson(data=gdf_fc, name=map_title,
                               marker=folium.Circle(radius=5),
                               style_function= lambda x: {'fillColor': x['properties']['color'],
                                                          'color': x['properties']['color'],
                                                          'weight': 2},
                               tooltip=folium.features.GeoJsonTooltip(fields=tooltip_cols,
                                                                    aliases=['LAYER', label_col],
                                                                    labels=True),
                               popup=folium.features.GeoJsonPopup(fields=popup_cols, 
                                                                sticky=False,
                                                                max_width=380)).add_to(map_one)

                
                # Add the layer to the all-Layers map
                folium.GeoJson(data=gdf_fc, name=map_title,
                               marker=folium.Circle(radius=5),
                               show= False,
                               style_function= lambda x: {'fillColor': x['properties']['color2'],
                                                          'color': x['properties']['color2'],
                                                          'weight': 2},
                               tooltip=folium.features.GeoJsonTooltip(fields=tooltip_cols,
                                                                    aliases=['LAYER', label_col],
                                                                    labels=True),
                               popup=folium.features.GeoJsonPopup(fields=popup_cols, 
                                                                sticky=False,
                                                                max_width=380)).add_to(map_all)

                
                # Add buffered areas to the individual maps
                for k,v in bf_gdfs.items():
                        folium.GeoJson(data=v, name=k, show=False,
                                    style_function=lambda x:{'color': 'orange',
                                                                'fillColor': 'none',
                                                                'weight': 3}).add_to(map_one)

                # Create a Legend for individual maps
                #legend colors and names
                legend_labels = zip(gdf_fc['color'], gdf_fc[label_col])
                
                #start the div tag and set the legend size and position
                legend_html = '''
                            <div id="legend" style="position: fixed; 
                            bottom: 200px; right: 30px; z-index: 1000; 
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
                

                #add the legend to the individual maps
                map_one.get_root().html.add_child(folium.Element(legend_html))

                # Add layer controls to the individual map
                lyr_cont_one = folium.LayerControl()
                lyr_cont_one.add_to(map_one)
            
                # Save the indivdiual map to html file
                map_one.save(os.path.join(self.out_loc, fc+'.html'))
                
            counter += 1

            # Create a Legend for all-layers map
            legend_html_all = '''
                        <div id="legend" style="position: fixed; 
                        bottom: 200px; right: 30px; z-index: 1000; 
                        background-color: #fff; padding: 10px; 
                        border-radius: 5px; border: 1px solid grey;">

                        <div style="display: inline-block; 
                        margin-right: 10px;
                        background-color: transparent;
                        border: 2px solid red;
                        width: 15px; height: 15px;"></div>AOI<br>
                        
                        <div style="display: inline-block; 
                        margin-right: 10px;background-color: transparent; 
                        border: 2px solid orange;
                        width: 15px; height: 15px;"></div>AOI buffers<br>
                        
                        </div>
                        '''    
                        
        #add the legend to the all-layers map
        map_all.get_root().html.add_child(folium.Element(legend_html_all))   
                
        # Add layer controls to the all-layers map
        lyr_cont_all = folium.LayerControl()
        lyr_cont_all.add_to(map_all)
        
        # Save the all-layers map to html file
        print('Creating the all-layers map')
        map_all.save(os.path.join(self.out_loc, '00_all_layers.html'))
        



# Execute the function and track processing time
start_t = timeit.default_timer() #start time

#add_proj_lib ()

# This is an example of a one_status_common_datasets geodatabase
#file_nbr='1414630'
work_gdb = r'\\spatialfiles.bcgov\work\lwbc\visr\Workarea\FCBC_VISR\Lands_Statusing\1414375\2024\one_status_common_datasets_aoi.gdb'
map_directory= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\TOOLS\SCRIPTS\STATUSING\fc_to_html\maps_AST_test\maps'

label_col=None
html = HTMLGenerator(work_gdb, label_col, map_directory)
html.generate_html_maps()

finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print ('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))