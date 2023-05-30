import os
import cx_Oracle
import pandas as pd
from shapely import wkt
import geopandas as gpd
from postalcodes_ca import postal_codes


def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection


def df_2_gdf (df, crs):
    """ Return a geopandas gdf based on a df with Geometry column"""
    df['SHAPE'] = df['SHAPE'].astype(str)
    
    def wkt_loads(x):
        try:
            return wkt.loads(x)
        except Exception:
            return None
        
    df['geometry'] = df['SHAPE'].apply(wkt_loads)   
    
    for col in df.columns:
        if 'DATE' in col:
            df[col]= pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        
    gdf = gpd.GeoDataFrame(df, geometry = df['geometry'])
    
    gdf.crs = "EPSG:" + str(crs)
    del df['SHAPE']
    
    return gdf


def load_wsh_ids():
    """Returns a dict of Assesement Watershed IDs"""
    wsh_dict = {}
    wsh_dict['Koksilah'] = '2567,2572,2573'
    wsh_dict['Chemainus'] = '2596,2602,2601'
    wsh_dict['Tsolum'] = '2450,2456,2455'
    wsh_dict['Black'] = '2457'
    wsh_dict['Fulford'] = '19639'
    wsh_dict['Millstone'] = '12919'
    
    return wsh_dict



def load_sql ():
    sql = {}

    sql ['wlc'] = """
                    SELECT wl.POD_NUMBER,
                       wl.LICENCE_NUMBER,
                       pl.PID, 
                       wl.PURPOSE_USE, 
                       wl.LICENCE_STATUS_DATE AS LICENCE_DATE,
                       wl.PRIMARY_LICENSEE_NAME,
                       wl.ADDRESS_LINE_1,
                       wl.ADDRESS_LINE_2,
                       wl.ADDRESS_LINE_3,
                       null AS CITY, 
                       SUBSTR(wl.ADDRESS_LINE_4,1,3) AS PROVINCE,
                       wl.COUNTRY,
                       wl.POSTAL_CODE,
                       wl.PRIVATE_CONTACT_INFO_IND
                
                FROM WHSE_WATER_MANAGEMENT.WLS_WATER_RIGHTS_LICENCES_SP wl
                  LEFT JOIN WHSE_WATER_MANAGEMENT.WLS_LICENCE_WITH_PARCELS_ISP pl
                         ON wl.LICENCE_NUMBER = pl.LICENCE_NO
                  INNER JOIN WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY aw
                         ON SDO_RELATE (wl.SHAPE, aw.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                         AND aw.WATERSHED_FEATURE_ID IN ({})
                
                WHERE wl.LICENCE_STATUS = 'Current'
                ORDER BY LICENCE_STATUS_DATE DESC
                  """
   
    sql ['asw'] = """
                SELECT aw.WATERSHED_FEATURE_ID,
                       aw.GNIS_NAME_1,
                       SDO_UTIL.TO_WKTGEOMETRY(aw.GEOMETRY) SHAPE
                       
                FROM WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY aw
                
                WHERE WATERSHED_FEATURE_ID IN (2567,2572,2573,2596,2602,2601,
                                               2450,2456,2455,2457,19639,12919)
                  """

    sql ['fnc'] = """
                SELECT fn.CNSLTN_AREA_NAME, 
                       fn.CONTACT_ORGANIZATION_NAME,
                       fn.CONTACT_NAME,
                       fn.CONTACT_TITLE,
                       fn.CONTACT_ADDRESS,
                       fn.CONTACT_CITY,
                       fn.CONTACT_PROVINCE,
                       fn.CONTACT_POSTAL_CODE,
                       fn.CONTACT_FAX_NUMBER,
                       fn.CONTACT_PHONE_NUMBER,
                       fn.CONTACT_EMAIL_ADDRESS
                
                FROM WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP fn
                  INNER JOIN WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY aw 
                    ON SDO_RELATE (fn.SHAPE, aw.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                       AND aw.WATERSHED_FEATURE_ID IN ({})
                       
                ORDER BY fn.CNSLTN_AREA_NAME
                  """
                  
    return sql

def create_wsh_gdf (connection,sql):
    """Returns a gdf containing the Assesement Watersheds"""
    df = pd.read_sql(sql['asw'], connection)
    gdf = df_2_gdf (df, 3005)
    
    return gdf
    
def create_eug_gdf (eug_xlsx,eug_cols):
    """Returns a gdf containing the Existing Use Groundwater layer"""
    df = pd.read_excel(eug_xlsx)
    df.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
    
    df=df[eug_cols]
    
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.LONGITUDE, df.LATITUDE), 
                           crs="EPSG:4326")
    
    gdf.to_crs(3005, inplace=True)
    
    return gdf
    


print ('Connecting to BCGW')
hostname = 'bcgw.bcgov/idwprod1.bcgov'
bcgw_user = os.getenv('bcgw_user')
bcgw_pwd = os.getenv('bcgw_pwd')
connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)

print ('Load the SQL queries')
sql = load_sql()

print('Create an Assesement Watershed Layer')  
gdf_wsh= create_wsh_gdf (connection,sql)  
    
print ('Create an Existing Use Groundwater Layer')
eug_xlsx= 'Existing_Use_Groundwater_clean.xlsx'
eug_cols= ['ATS_PROJECT', 'FILE_NO','PURPOSE','APPLICANT',
           'LAND_PARCEL_PID','LATITUDE','LONGITUDE']
gdf_eug= create_eug_gdf (eug_xlsx,eug_cols)


print ('Run Queries')
wsh_dict= load_wsh_ids()

for k, v in wsh_dict.items():
    if k == 'Koksilah':
        print ('...working on watershed: {}'.format(k))
        gdf_wsh_ex = gdf_wsh.loc[gdf_wsh['GNIS_NAME_1'].str.contains(k)]
        gdf_intr = gpd.overlay(gdf_eug, gdf_wsh_ex, how='intersection')
        df_eug= gdf_intr[eug_cols[:-2]]
        
        df_fnc = pd.read_sql(sql['fnc'].format(v), connection)
        df_fnc.drop_duplicates(subset=['CNSLTN_AREA_NAME','CONTACT_ORGANIZATION_NAME'],
                              inplace= True)
        
        df_wlc = pd.read_sql(sql['wlc'].format(v), connection)
        
        df_wlc['LICENCE_DATE'] = pd.to_datetime(df_wlc['LICENCE_DATE'],
                                        infer_datetime_format=True,
                                        errors = 'coerce').dt.date
        
        for col in df_wlc.columns:
            if col != 'LICENCE_DATE':
                df_wlc[col] = df_wlc[col].str.lstrip()
                df_wlc[col].fillna('', inplace=True)
            

        for index, row in df_wlc.iterrows():
            if " BC" in row['ADDRESS_LINE_3']:
                df_wlc.at[index, 'CITY'] = row['ADDRESS_LINE_3'].replace(' BC', '')
                df_wlc.at[index, 'ADDRESS_LINE_3'] = ''
            
            if "BC V" in row['ADDRESS_LINE_3']:
                df_wlc.at[index, 'ADDRESS_LINE_3'] = ''
            
            if (" BC" in row['ADDRESS_LINE_2']) or (" B C" in row['ADDRESS_LINE_2']):
                df_wlc.at[index, 'CITY'] = row['ADDRESS_LINE_2'].replace(' BC', '').replace(' B C', '')
                df_wlc.at[index, 'ADDRESS_LINE_2'] = ''
            
            if row['ADDRESS_LINE_2'].strip() == row['ADDRESS_LINE_1'].strip():
                df_wlc.at[index, 'ADDRESS_LINE_2'] = ''
            
            if (len(row['ADDRESS_LINE_3']) > 0):
                df_wlc.at[index, 'ADDRESS_LINE_2'] = row['ADDRESS_LINE_2'] +' '+row['ADDRESS_LINE_3']
                df_wlc.at[index, 'ADDRESS_LINE_3'] = '' 

            if (row['ADDRESS_LINE_1']=='') and (len(row['ADDRESS_LINE_2']) > 0):
                df_wlc.at[index, 'ADDRESS_LINE_1'] = row['ADDRESS_LINE_2']
                df_wlc.at[index, 'ADDRESS_LINE_1'] = row['ADDRESS_LINE_1'].strip()
                df_wlc.at[index, 'ADDRESS_LINE_2'] = ''
                
            if (len(row['POSTAL_CODE']) > 0) and (row['CITY']==''):
                if (len(row['POSTAL_CODE'])== 7):
                    rslt = postal_codes.get(row['POSTAL_CODE'])
                    if (rslt is not None) :
                        df_wlc.at[index, 'CITY'] = rslt.name.upper()
                        df_wlc.at[index, 'PROVINCE'] = rslt.province
                        if row['PROVINCE'] == 'British Columbia':
                            df_wlc.at[index, 'PROVINCE'] = 'BC'
                    
                    
        df_wlc.drop(columns=['ADDRESS_LINE_3'], inplace=True)
