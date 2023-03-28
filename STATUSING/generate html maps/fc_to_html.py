import os
import numpy  as np
import geopandas as gpd
import fiona
import folium


geojson_file = 'test.geojson'
legend_column = 'BORDENNUMBER'

# This is the input gdb containing the status feature classes
status_gdb = r'\\spatialfiles.bcgov\work\lwbc\visr\Workarea\FCBC_VISR\Lands_Statusing\1414630\one_status_common_datasets_aoi.gdb'

# List all feature classes
fc_list = fiona.listlayers(status_gdb)

# Read the AOI feature class (fc) 
gdf_aoi = gpd.read_file(filename= status_gdb, layer= 'aoi')

#Remove the "aoi' buffers from the list 
fc_list = [x for x in fc_list if 'aoi' not in x]

gdf_fc = gpd.read_file(filename= status_gdb, layer= 'Archaeological_Sites_within_500_m_Radius')

    
# Datetime columns are causing errors when plotting in folium. Converting them to str
for col in gdf_fc.columns:
    if gdf_fc[col].dtype == 'datetime64[ns]':
        gdf_fc[col] = gdf_fc[col].astype(str)


    
# Remove the Geometry columns from the popup window. 
# This can be set to the list of columns pulled from the AST spreadsheets
popup_cols = list(gdf_fc.columns)                     
popup_cols.remove('geometry')  

# Initiate a Map object and set extent based on the feature class 
map_obj = folium.Map(tiles='openstreetmap')
xmin,ymin,xmax,ymax = gdf_fc.to_crs(4326)['geometry'].total_bounds
map_obj.fit_bounds([[ymin, xmin], [ymax, xmax]])
        

# Add the AOI layer to the folium map
folium.GeoJson(data=gdf_aoi, name='AOI',
               style_function=lambda x:{'color': 'red',
            'weight': 3}).add_to(map_obj)

# Add random colors to the features
for x in gdf_fc.index:
    color = np.random.randint(16, 256, size=3)
    color = [str(hex(i))[2:] for i in color]
    color = '#'+''.join(color).upper()
    gdf_fc.at[x, 'color'] = color
    
# Create a style function for the main layer 
def style(feature):
    return {'fillColor': feature['properties']['color'],
            'color': feature['properties']['color'],
            'weight': 2}

# Add the main layer to the folium map
folium.GeoJson(data=gdf_fc,
               name='Archaeological_Sites_within_500_m_Radius',
               style_function= style,
               tooltip=folium.features.GeoJsonTooltip(fields=[legend_column], labels=True),
               popup=folium.features.GeoJsonPopup(fields=popup_cols, sticky=False)).add_to(map_obj)



# add layer controls to the map
folium.LayerControl().add_to(map_obj)


# Create a Legend
#legend colors and names
legend_labels = zip(gdf_fc['color'], gdf_fc[legend_column])

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
                    margin-right: 10px;background-color: red; 
                    width: 15px; height: 15px;"></div>AOI<br>
                    '''

#add a title to the legend            
legend_html += '''
            <div style="font-weight: bold; 
            margin-bottom: 5px;">{}</div>
            '''.format(legend_column)
                    
#add the main layer items to the legend
for color, name in legend_labels:
    legend_html += '''
                    <div style="display: inline-block; 
                    margin-right: 10px;background-color: {0}; 
                    width: 15px; height: 15px;"></div>{1}<br>
                    '''.format(color, name)
#close the div tag
legend_html += '</div>'

#add the legend to the map
map_obj.get_root().html.add_child(folium.Element(legend_html))



# Save the interavtive map to html file
map_obj.save('test_html.html')
