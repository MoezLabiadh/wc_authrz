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
# Created:     10-11-2023
# Updated:     
#-------------------------------------------------------------------------------


import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkb
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
        print('Folder {} already exists!'.format(dir))
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
    
    return df


def wapp_to_gdf(df):
    """Converts the water applications df into a gdf """
    gdf= gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['LONGITUDE'], df['LATITUDE']))
    gdf.crs = 'EPSG:4326'
    
    return gdf
    
    
def load_sql ():
    sql = {}
    sql['aqf'] = """
                SELECT AQUIFER_ID
                
                FROM WHSE_WATER_MANAGEMENT.GW_AQUIFERS_CLASSIFICATION_SVW aqf
                
                WHERE  SDO_RELATE (aqf.GEOMETRY, SDO_GEOMETRY('POINT({long} {lat})', 4326),
                                      'mask=ANYINTERACT') = 'TRUE'
                """
    
    return sql


def filter_kfn(df, gdf_wapp, gdf_kfn_pip):
    """Filters water applications within KFN territory"""
    intr = gpd.overlay(gdf_wapp, gdf_kfn_pip, how='intersection')
    
    df= df.loc[df['UNIQUE_ID'].isin(intr['UNIQUE_ID'].to_list())]
    
    df['WITHIN_KFN']= 'YES'
    
    df.reset_index(drop=True, inplace= True)
        
    return df


def add_aquifer_info(df,connection,sql):
    for index, row in df.iterrows():
        print('...working on row {}'.format(index))
        #app_typ = row['APPLICATION_TYPE']
        
        #if app_typ in ('Existing Use - Groundwater', 'Water Licence - Ground', 
                      # 'Abandon - Ground', 'Amendment - Ground'):
        long = row['LONGITUDE']
        lat = row['LATITUDE']
            
        query = sql['aqf'].format(lat=lat,long=long)
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



def add_southKFN_info (df, gdf_skfn, gdf_wapp):
    """ Overlay with south KFN """
    gdf_skfn = gdf_skfn.to_crs({'init': 'epsg:4326'})
    
    gdf_int= gpd.overlay(gdf_wapp, gdf_skfn, how='intersection')
    
    skfn_l = gdf_int['UNIQUE_ID'].to_list()
    
    df['WITHIN_SOUTH_KFN'] = 'NO'
    df.loc[ df['UNIQUE_ID'].isin(skfn_l), 'WITHIN_SOUTH_KFN'] = "YES"
    
    
    return df


def export_shp (gdf, out_dir, shp_name):
    """Exports a shapefile based on a geodataframe"""
    shp_f = os.path.join(out_dir, shp_name+'.shp')
    gdf.to_file(shp_f, driver="ESRI Shapefile")
    
    
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
    
    
#def main():  
start_t = timeit.default_timer() #start time
    
print ('\nProcessing input water ledgers')
in_gdb= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\DATASETS\WaterAuth\KFN_waterPilot_proj.gdb'
workspace= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20231110_komoks_waterPilot_proj_workflow\ledgers'

f_eug = os.path.join(workspace,'Existing_Use_Groundwater.xlsx')
f_new = os.path.join(workspace,'Water Application Ledger.xlsx')
df = process_ledgers(f_eug,f_new)


print ('\nConnecting to BCGW.')
hostname = 'bcgw.bcgov/idwprod1.bcgov'
bcgw_user = os.getenv('bcgw_user')
bcgw_pwd = os.getenv('bcgw_pwd')
connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)


print ('\nAdding KFN info')
gdf_wapp= wapp_to_gdf(df)

gdf_kfn_pip= prepare_geo_data(os.path.join(in_gdb, 'kfn_consultation_area'))


df= filter_kfn(df, gdf_wapp, gdf_kfn_pip)




'''
print ('\nFiltering Applications within KFN')
df = df.loc[df['WITHIN_KFN']== 'YES']
df.reset_index(drop=True, inplace= True)

print ('\nAdding Aquifer info')
df = add_aquifer_info(df,connection,sql)



print ("\nOverlaying with South KFN boundary")
skfn_fc = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\DATASETS\WaterAuth\local_datasets.gdb\komoks_southern_core'

gdf_skfn= esri_to_gdf (skfn_fc)

gdf_wapp = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['LONGITUDE'], df['LATITUDE']))
gdf_wapp.set_crs('epsg:4326', inplace=True)

add_southKFN_info (df, gdf_skfn, gdf_wapp)

print ('\nExporting results')
out_path = create_dir (workspace, 'OUTPUTS')
spatial_path = create_dir (out_path, 'SPATAL')
excel_path = create_dir (out_path, 'SPREADSHEET')
    
df = df.drop('geometry', axis=1)

today = datetime.today().strftime('%Y%m%d')

filename= f'{today}_waterApplics_KFN' 
generate_report (excel_path, [df], ['Water Applics - KFN territory'], filename)

export_shp (gdf_wapp, spatial_path, filename)

finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print ('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs)) 

'''
#main()

