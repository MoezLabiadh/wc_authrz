#-------------------------------------------------------------------------------
# Name:        Komoks Water Applications
#
# Purpose:     This script generates a reporting materials for 
#              Komoks Water Pilot Project
#
# Input(s):    (1) Workspace (folder) where outputs will be generated.
#              (2) New Water Applications ledger(xlsx)
#              (3) Existing Use Applications ledger(xlsx)
#              (4) BCGW connection parameters
#              (5) GDB containing input datasets
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     16-11-2023
# Updated:     
#-------------------------------------------------------------------------------


import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkb
import folium
from folium.plugins import HeatMap
from folium.plugins import Search
from datetime import datetime
import timeit


def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection


def create_dir (path, dir):
    """ Creates new folder and returns path"""
    try:
      os.makedirs(os.path.join(path,dir))

    except OSError:
        print('...Folder {} already exists!'.format(dir))
        pass

    return os.path.join(path,dir)


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


def flatten_to_2d(gdf):
    """Flattens 3D geometries to 2D"""
    for i, row in gdf.iterrows():
        geom = row.geometry
        if geom.has_z:
            geom_2d = wkb.loads(wkb.dumps(geom, output_dimension=2))
            gdf.at[i, 'geometry'] = geom_2d
    
    return gdf


def reproject_to_bcalbers(gdf):
    """ Reprojects a gdf to bc albers"""
    if gdf.crs != 'epsg:4326':
        gdf = gdf.to_crs('epsg:4326')
    
    return gdf


def prepare_geo_data(aoi):
    """ Runs data preparation functions"""
    gdf = esri_to_gdf(aoi)
    gdf = flatten_to_2d(gdf)
    gdf = reproject_to_bcalbers(gdf)

    return gdf
            
    
def process_ledgers(f_eug,f_new):
    df_eug = pd.read_excel(f_eug, 'Existing Use Applications', usecols="A:AG")
    df_new = pd.read_excel(f_new, 'Active Applications',converters={'File Number':str})
    
    df_new.dropna(subset=['Application Type'],inplace=True)
    types = [x for x in df_new['Application Type'].unique() if 'Cancellation' not in x]
    
    df_new = df_new.loc[df_new['Application Type'].isin(types)]
    
    df_new.columns = map(str.upper, df_new.columns)
    
    df_eug.rename(columns={'FILE_NO': 'FILE NUMBER', 
                           'ATS_PROJECT': 'ATS NUMBER',
                           'APP_VOLUME': 'VOLUME',
                           'AQUIFER': 'SOURCE_AQUIFER'}, inplace=True)
    
    df_eug ['APPLICATION TYPE'] = 'Existing Use - Groundwater'
    df_new ['ATS NUMBER'] = ''
    df_eug ['HOUSING'] = ''
    
    cols = ['APPLICATION TYPE','FILE NUMBER','ATS NUMBER', 
            'APPLICANT','PURPOSE','STATUS','LATITUDE', 'LONGITUDE', 'HOUSING']

    df_eug = df_eug[cols+['SOURCE_AQUIFER','VOLUME']]
    df_new = df_new[cols]
    
    df = pd.concat([df_new,df_eug])
    df.reset_index(drop=True, inplace=True)
    
    df['VOLUME_UNIT'] = 'm3/year'
 
    df ['DECISION TIMEFRAME'] = ''
    
    df.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
    
    df.columns = df.columns.str.replace(' ', '_')
    
    df['UNIQUE_ID'] = df['FILE_NUMBER']
    df['UNIQUE_ID'].fillna(df['ATS_NUMBER'], inplace=True)
    df = df[['UNIQUE_ID'] + [ col for col in df.columns if col != 'UNIQUE_ID' ]]
    
    # Add suffixes to duplicate IDs
    df['UNIQUE_ID'] = df['UNIQUE_ID'].astype(str)
    
    duplicates = df.duplicated(subset=['UNIQUE_ID'], keep=False)
    id_counts = {}
    
    def modify_duplicates(row):
        """ Add count prefixes to duplicated Unique IDs """
        if duplicates[row.name]:
            current_id = row['UNIQUE_ID']
            count = id_counts.get(current_id, 1)
            new_id = f"{current_id}-{count}"
            id_counts[current_id] = count + 1
            
            return new_id
        
        return row['UNIQUE_ID']

    df['UNIQUE_ID'] = df.apply(modify_duplicates, axis=1)

    return df


def modify_applic_types(gdf_wapp):
    """Add prefixes to Applications types - for mapping purposes"""
    dict = {'Water Licence - Surface': '1-Water Licence - Surface', 
            'Water Licence - Ground' : '2-Water Licence - Ground',
            'Amendment - Surface'    : '3-Amendment - Surface',
            'Amendment - Ground'     : '4-Amendment - Ground',
            'Amendment - Ground / Surface': '5-Amendment - Ground / Surface',
            'Abandon - Surface': '6-Abandon - Surface',
            'Abandon - Ground': '7-Abandon - Ground',
            'Existing Use - Groundwater': '8-Existing Use - Groundwater',
            }

    df['APPLICATION_TYPE'] = df['APPLICATION_TYPE'].replace(dict)
    

    return gdf_wapp


def wapp_to_gdf(df):
    """Converts the water applications df into a gdf """
    gdf= gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['LONGITUDE'], df['LATITUDE']))
    gdf.crs = 'EPSG:4326'
    
    return gdf


def filter_kfn(df, gdf_wapp, gdf_kfn_pip):
    """Filters water applications within KFN territory"""
    intr = gpd.overlay(gdf_wapp, gdf_kfn_pip, how='intersection')
    
    df= df.loc[df['UNIQUE_ID'].isin(intr['UNIQUE_ID'].to_list())]
    
    df['WITHIN_KFN']= 'YES'
    
    df= df.drop('geometry', axis=1)
    
    df.reset_index(drop=True, inplace= True)
        
    return df


def add_aquifer_info(df,connection):
    """Add aquifer overlap info """
    sql= """
           SELECT AQUIFER_ID
           FROM WHSE_WATER_MANAGEMENT.GW_AQUIFERS_CLASSIFICATION_SVW aqf
           WHERE  SDO_RELATE (aqf.GEOMETRY, SDO_GEOMETRY('POINT({long} {lat})', 4326),
                                      'mask=ANYINTERACT') = 'TRUE'
           """
      
    for index, row in df.iterrows():
        print(f'...working on row {index+1} of {len(df)}')

        
        long = row['LONGITUDE']
        lat = row['LATITUDE']
            
        query = sql.format(lat=lat,long=long)
        df_q = pd.read_sql(query,connection)
            
        if df_q.shape[0] > 0:
            aq = ", ".join(str(x) for x in df_q['AQUIFER_ID'].to_list())
            df.at[index,'AQUIFER_OVERLAP'] = aq
            
        else:
            pass

    cols = list(df.columns)
    cols.insert(11, cols.pop(cols.index('AQUIFER_OVERLAP')))
    df = df.reindex(columns=cols)
        
    return df


def add_southKFN_info (df, gdf_wapp, gdf_skfn):
    """ Overlay with south KFN """
    gdf_int= gpd.overlay(gdf_wapp, gdf_skfn, how='intersection')
    
    skfn_l = gdf_int['UNIQUE_ID'].to_list()
    
    df['WITHIN_SOUTH_KFN'] = 'NO'
    df.loc[ df['UNIQUE_ID'].isin(skfn_l), 'WITHIN_SOUTH_KFN'] = "YES"
    
    return df


def add_drght_wshd_info (df, gdf_wapp, gdf_drgh):
    """ Overlay with drought watersheds """
    
    df['WITHIN_DROUGHT_WSHD']= 'NO'
    
    gdf_intr= gpd.overlay(gdf_wapp, gdf_drgh, how='intersection')
    
    drgh_wshd_l = gdf_intr['UNIQUE_ID'].to_list()
    df.loc[df['UNIQUE_ID'].isin(drgh_wshd_l), 'WITHIN_DROUGHT_WSHD'] = "YES"

    df_intr= gdf_intr.groupby('UNIQUE_ID')['DROUGHT_WSHD_NAME']\
              .agg(lambda x: ', '.join(x)).reset_index()
    
    df= pd.merge(df, df_intr, how='left', on='UNIQUE_ID')
    
    
    return df


def add_cnrn_area_info (df, gdf_wapp, gdf_crna):
    """ Overlay with concern areas """
    
    df['WITHIN_CONCERN_AREA']= 'NO'
    
    gdf_intr= gpd.overlay(gdf_wapp, gdf_crna, how='intersection')
    
    drgh_wshd_l = gdf_intr['UNIQUE_ID'].to_list()
    df.loc[df['UNIQUE_ID'].isin(drgh_wshd_l), 'WITHIN_CONCERN_AREA'] = "YES"
        
    df_intr= gdf_intr.groupby('UNIQUE_ID')['CONCERN_AREA_NAME']\
              .agg(lambda x: ', '.join(x)).reset_index()
    
    df= pd.merge(df, df_intr, how='left', on='UNIQUE_ID')
    
    
    return df


def add_mntrd_wshd_info (df, gdf_wapp, gdf_mwsh):
    """ Overlay with concern areas """
    
    df['WITHIN_MNTRD_WSHD']= 'NO'
    
    gdf_intr= gpd.overlay(gdf_wapp, gdf_mwsh, how='intersection')
    
    drgh_wshd_l = gdf_intr['UNIQUE_ID'].to_list()
    df.loc[df['UNIQUE_ID'].isin(drgh_wshd_l), 'WITHIN_MNTRD_WSHD'] = "YES"

    df_intr= gdf_intr.groupby('UNIQUE_ID')['NameNom']\
              .agg(lambda x: ', '.join(x)).reset_index()
    
    df= pd.merge(df, df_intr, how='left', on='UNIQUE_ID')
    
    df.rename(columns={'NameNom': 'HYDRO_STATION_NAME'}, inplace= True)
    
    
    return df


def add_mntrd_aqfr_info (df, gdf_wapp, gdf_mnaq):
    """ Overlay with concern areas """
    
    df['WITHIN_MNTRD_AQFR']= 'NO'
    
    gdf_intr= gpd.overlay(gdf_wapp, gdf_mnaq, how='intersection')
    
    drgh_wshd_l = gdf_intr['UNIQUE_ID'].to_list()
    df.loc[df['UNIQUE_ID'].isin(drgh_wshd_l), 'WITHIN_MNTRD_AQFR'] = "YES"
    
    gdf_intr['AQUIFER_ID'] = gdf_intr['AQUIFER_ID'].astype(str)
    
    df_intr= gdf_intr.groupby('UNIQUE_ID')['AQUIFER_ID']\
              .agg(lambda x: ', '.join(x)).reset_index()
    
    df= pd.merge(df, df_intr, how='left', on='UNIQUE_ID')
    

    return df


def export_shp (gdf, out_dir, shp_name):
    """Exports a shapefile based on a geodataframe"""
    shp_f = os.path.join(out_dir, shp_name+'.shp')
    gdf.to_file(shp_f, driver="ESRI Shapefile")
    

def create_html_map(gdf_skfn, gdf_kfn_pip, gdf_wapp, gdf_hydr, gdf_obsw):
    """Creates a HTML map"""
    # Create a map object
    m = folium.Map()
    
    xmin,ymin,xmax,ymax = gdf_kfn_pip['geometry'].total_bounds
    m.fit_bounds([[ymin, xmin], [ymax, xmax]])
    
    # Add water applications
    '''
    gdf_wapp.explore(
        m=m, 
        name="Water Applications",
        column= 'APPLICATION_TYPE',
        cmap= 'Paired',
        tooltip= True,
        popup= True,
        legend= True,
        style_kwds=dict(
            fill=True,
            weight=2)
        )
    
    '''
    cols = list(gdf_wapp.columns.drop('geometry'))
    
    cmap= {
        '1-Water Licence - Surface': '#2874ed',
        '2-Water Licence - Ground': '#3db31d',
        '3-Amendment - Surface': '#eb67ca',
        '4-Amendment - Ground': '#d93b23',
        '5-Amendment - Ground / Surface': '#eb9e3b',
        '6-Abandon - Surface': '#be68e3',
        '7-Abandon - Grounde': '#e0de53',
        '8-Existing Use - Groundwater': '#8a6c49'}

    gdf_wapp['color']= gdf_wapp['APPLICATION_TYPE'].map(cmap)
    
    wapp_lyr = folium.GeoJson(
        data=gdf_wapp,
        name='Water Applications',
        marker=folium.Circle(radius=5),
        style_function= lambda x: {'fillColor': x['properties']['color'],
                                   'color': x['properties']['color'],
                                   'weight': 5},
        tooltip= folium.features.GeoJsonTooltip(fields=cols, labels=True),
        popup= folium.features.GeoJsonPopup(fields=cols, sticky=False, max_width=380)
    ).add_to(m)

     
    
    #Add a heatmap
    heat_data = [[point.xy[1][0], point.xy[0][0]] for point in gdf_wapp.geometry]
    HeatMap(heat_data, 
            min_opacity= 0.4,
            blur= 20).add_to(folium.FeatureGroup(name='Heatmap of Water applics').add_to(m))
    
    
    # Add KFN pip layer
    gdf_kfn_pip.explore(
        m=m, 
        name="KFN Consultation Area",
        #column= 'NAME',
        tooltip= False,
        #legend= True,
        style_kwds=dict(fill= False, 
                        color="black", 
                        weight=3)
         )
    
    
    # Add KFN south layer
    gdf_skfn.explore(
        m=m, 
        name="Southern KFN Area",
        tooltip= False,
        #legend= True,
        style_kwds=dict(fill= False, 
                        color="#707070", 
                        weight=3)
         )
    

    # Add a satellite basemap to the map
    satellite_url = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
    satellite_attribution = 'Tiles &copy; Esri'
    folium.TileLayer(
        tiles=satellite_url,
        name='Imagery Basemap',
        attr=satellite_attribution,
        overlay=False,
        control=True).add_to(m)


    #Add Aquifers layer to the map
    aq_group = folium.FeatureGroup(name='Aquifer Classification', show=False)
    aq_url = 'https://openmaps.gov.bc.ca/geo/pub/WHSE_WATER_MANAGEMENT.GW_AQUIFERS_CLASSIFICATION_SVW/ows?service=WMS'
    aq_layer = folium.raster_layers.WmsTileLayer(
        url=aq_url,
        fmt='image/png',
        layers='WHSE_WATER_MANAGEMENT.GW_AQUIFERS_CLASSIFICATION_SVW',
        transparent=True,
        overlay=False)
    aq_layer.add_to(aq_group)
    aq_group.add_to(m)


    #Add Watersheds layer to the map
    ws_group = folium.FeatureGroup(name='Water Licensing Watersheds', show=False)
    ws_url = 'https://openmaps.gov.bc.ca/geo/pub/WHSE_WATER_MANAGEMENT.WLS_WATER_LIC_WATERSHEDS_SP/ows?service=WMS'
    ws_layer = folium.raster_layers.WmsTileLayer(
        url=ws_url,
        fmt='image/png',
        layers='WHSE_WATER_MANAGEMENT.WLS_WATER_LIC_WATERSHEDS_SP',
        transparent=True,
        overlay=False)
    ws_layer.add_to(ws_group)
    ws_group.add_to(m)  
    
    
    # Add hydrometric stations layer
    gdf_hydr.explore(
        m=m, 
        name="Active Hydrometric Gauges",
        tooltip= True,
        popup= True,
        show=False,
        style_kwds=dict(fill= True, 
                        color="black", 
                        weight=4))

    # Add hydrometric stations layer
    gdf_obsw.explore(
        m=m, 
        name="Active GW Observation Wells",
        tooltip= True,
        popup= True,
        show=False,
        style_kwds=dict(fill= True, 
                        color="purple", 
                        weight=4))
    
    
    #Add PMBC layer to the map
    pm_group = folium.FeatureGroup(name='Cadastre Parcels', show=False)
    pm_url = 'https://openmaps.gov.bc.ca/geo/pub/WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_SVW/ows?service=WMS'
    pm_layer = folium.raster_layers.WmsTileLayer(
        url=pm_url,
        fmt='image/png',
        layers='WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_SVW',
        transparent=True,
        overlay=False)
    pm_layer.add_to(pm_group)
    pm_group.add_to(m)  
    
    # create a title
    title_txt1= 'KFN Water Pilot Project'
    title_txt2= 'Water Applications within KFN territory'
    title_obj = """
        
            <div style="position: fixed; 
                 top: 740px; left: 30px; 
                 background-color:#f0f0eb; border:0px solid grey;z-index: 900; padding:0.5%;">
                 <h2 style="font-weight:bold;color:#992c25;white-space:nowrap;">{}</h2>
                 <h4 style="font-weight:bold;color:#992c25;white-space:nowrap;">{}</h4>
            </div>
        
        """.format(title_txt1, title_txt2)   
    m.get_root().html.add_child(folium.Element(title_obj))
    

    folium.plugins.Fullscreen(
        position="topright",
        title="Expand me",
        title_cancel="Exit me",
        force_separate_button=True).add_to(m)
    
    Search(
        layer=wapp_lyr,
        geom_type="Point",
        placeholder="Search Water Applications by Unique ID",
        search_label="UNIQUE_ID",
        weight=3,
    ).add_to(m)
    
    
    #Create a Legend
    #start the div tag and set the legend size and position
    legend_html = '''
                <div id="legend" style="position: fixed; 
                bottom: 30px; right: 30px; z-index: 1000; 
                background-color: #fff; padding: 10px; 
                border-radius: 5px; border: 1px solid grey;">
                '''
    #add a header to the legend            
    legend_html += '''
                <div style="font-weight: bold; 
                margin-bottom: 5px;">APPLICATION TYPE</div>
                '''
    #add items to the legend
    for name, color in cmap.items():
        legend_html += '''
                        <div style="display: inline-block; 
                        margin-right: 10px;background-color: {0}; 
                        width: 15px; height: 15px;"></div>{1}<br>
                        '''.format(color, name)
    #close the div tag
    legend_html += '</div>'
    

    #add the legend to the individual maps
    m.get_root().html.add_child(folium.Element(legend_html))
           
    lyr_cont= folium.LayerControl(collapsed=False)
    lyr_cont.add_to(m)
    
    
    return m

    
def generate_report (workspace, df_list, sheet_list,filename):
    """ Exports dataframes to multi-tab excel spreasheet"""
    file_name = os.path.join(workspace, filename+'.xlsx')

    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'count'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    
    
if __name__ == '__main__':
    start_t = timeit.default_timer() #start time
        
    print ('\nProcessing input water ledgers')
    out_wks= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20231110_komoks_waterPilot_proj_workflow'
    in_gdb= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\DATASETS\WaterAuth\KFN_waterPilot_proj.gdb'
    #in_ldgrs= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20231110_komoks_waterPilot_proj_workflow\ledgers'
    
    in_wap_ldgr= r'\\sfp.idir.bcgov\S140\S40133\WaterStewardship_Share\WSD\Allocation\Application Database'
    in_eug_ldgr= r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Logs'
    
    f_eug = os.path.join(in_eug_ldgr,'Existing_Use_Groundwater.xlsx')
    f_new = os.path.join(in_wap_ldgr,'Water Application Ledger.xlsx')
    df = process_ledgers(f_eug,f_new)
    
    
    print ('\nConnecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    
    print ('\nFiltering applications within KFN territory')
    gdf_wapp= wapp_to_gdf(df)
    
    gdf_kfn_pip= prepare_geo_data(os.path.join(in_gdb, 'kfn_consultation_area'))
    
    df= filter_kfn(df, gdf_wapp, gdf_kfn_pip)

    print ('\nAdding Aquifer info *********REMINDER TO ADD THIS SECTION*********')
    df = add_aquifer_info(df,connection)
    
    print ("\nOverlaying with South KFN boundary")
    gdf_skfn= prepare_geo_data(os.path.join(in_gdb, 'kfn_southern_core'))
    df= add_southKFN_info (df, gdf_wapp, gdf_skfn)
    
    print ("\nOverlaying with Drought Watershed")
    gdf_drgh= prepare_geo_data(os.path.join(in_gdb, 'drought_watershed'))
    df= add_drght_wshd_info (df, gdf_wapp, gdf_drgh)
    
    print ("\nOverlaying with KFN Areas of Concern")
    gdf_crna= prepare_geo_data(os.path.join(in_gdb, 'kfn_concern_area'))
    df= add_cnrn_area_info (df, gdf_wapp, gdf_crna)
    
    print ("\nOverlaying with Monitored Watersheds")
    gdf_mwsh= prepare_geo_data(os.path.join(in_gdb, 'monitored_watersheds'))
    df= add_mntrd_wshd_info (df, gdf_wapp, gdf_mwsh)
    
    print ("\nOverlaying with Monitored Aquifers")
    gdf_mnaq= prepare_geo_data(os.path.join(in_gdb, 'aquifers_obs_well'))
    df= add_mntrd_aqfr_info (df, gdf_wapp, gdf_mnaq)
    
    print ('\nExporting results')
    out_path = create_dir (out_wks, 'OUTPUTS')
    #spatial_path = create_dir (out_path, 'SPATAL')
    #excel_path = create_dir (out_path, 'SPREADSHEET')
        
    today = datetime.today().strftime('%Y%m%d')
    
    xls_name= f'{today}_KFN_waterPilot_report' 
    map_name= f'{today}_KFN_waterPilot_map' 
    generate_report (out_path, [df], ['Water Applics - KFN territory'], xls_name)
    
    gdf_wapp= wapp_to_gdf(df)
    #export_shp (gdf_wapp, spatial_path, filename)

    # Create the html map
    gdf_wapp= wapp_to_gdf(df)
    gdf_wapp= modify_applic_types(gdf_wapp)
    
    gdf_hydr= prepare_geo_data(os.path.join(in_gdb, 'active_hydrometric_gauges'))
    
    gdf_obsw= prepare_geo_data(os.path.join(in_gdb, 'active_gw_obseration_wells'))
    gdf_obsw=gdf_obsw[['WELL_TAG', 'AQUIFER_ID', 'COMPANY', 'FNSH_DEPTH', 'geometry']]
    
    m= create_html_map(gdf_skfn, gdf_kfn_pip, gdf_wapp, gdf_hydr, gdf_obsw)
    m.save(os.path.join(out_path, map_name + '.html'))
    
    
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print ('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs)) 