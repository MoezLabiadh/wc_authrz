#-------------------------------------------------------------------------------
# Name:        Data Quality - Find Multiple Parcels
#
# Purpose:     This script finds Tenure Files with multiple Parcel IDs
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     02-02-2023
# Updated:
#-------------------------------------------------------------------------------


import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
#from shapely import wkt

#Hide pandas warning
pd.set_option('mode.chained_assignment', None)


def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection



def read_query(connection,query):
    "Returns a df containing SQL Query results"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        names = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=names)
    
    finally:
        if cursor is not None:
            cursor.close()



def df_2_gdf (df, crs):
    """ Return a geopandas gdf based on a df with Geometry column"""
    df['SHAPE'] = df['SHAPE'].astype(str)
    df['geometry'] = gpd.GeoSeries.from_wkt(df['SHAPE'])
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    #df['geometry'] = df['SHAPE'].apply(wkt.loads)
    #gdf = gpd.GeoDataFrame(df, geometry = df['geometry'])
    gdf.crs = "EPSG:" + str(crs)
    del gdf['SHAPE']
    
    return gdf


def load_queries ():
    """ Return the SQL queries that will be executed"""
    sql = {}
    
    sql['>2'] = """
    SELECT a.CROWN_LANDS_FILE, a.DISPOSITION_TRANSACTION_SID, a.INTRID_SID as PARCEL_ID,
           COUNT(a.INTRID_SID) OVER (PARTITION BY a.CROWN_LANDS_FILE,a.DISPOSITION_TRANSACTION_SID) as PARCEL_COUNT,
           a.TENURE_STATUS, a.TENURE_STAGE, a.TENURE_TYPE, a.TENURE_SUBTYPE, a.TENURE_PURPOSE, a.TENURE_SUBPURPOSE, 
           a.TENURE_LOCATION, a.TENURE_LEGAL_DESCRIPTION,
           ROUND(SDO_GEOM.SDO_AREA(a.SHAPE, 0.005, 'unit=HECTARE'), 5) PARCEL_HECTARE, 
           SDO_UTIL.TO_WKTGEOMETRY(a.SHAPE) SHAPE
           
    FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a
      INNER JOIN (SELECT CROWN_LANDS_FILE, DISPOSITION_TRANSACTION_SID
                    FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW
                      GROUP BY CROWN_LANDS_FILE, DISPOSITION_TRANSACTION_SID
                        HAVING COUNT(INTRID_SID)>2) b             
          ON a.CROWN_LANDS_FILE = b.CROWN_LANDS_FILE 
            AND a.DISPOSITION_TRANSACTION_SID = b.DISPOSITION_TRANSACTION_SID
      
    WHERE a.RESPONSIBLE_BUSINESS_UNIT= 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
    
    ORDER BY a.CROWN_LANDS_FILE
                 """
    
    sql['=2'] = """
    SELECT a.CROWN_LANDS_FILE, a.DISPOSITION_TRANSACTION_SID, a.INTRID_SID as PARCEL_ID,
           COUNT(a.INTRID_SID) OVER (PARTITION BY a.CROWN_LANDS_FILE,a.DISPOSITION_TRANSACTION_SID) as PARCEL_COUNT,
           a.TENURE_STATUS, a.TENURE_STAGE, a.TENURE_TYPE, a.TENURE_SUBTYPE, a.TENURE_PURPOSE, a.TENURE_SUBPURPOSE, 
           a.TENURE_LOCATION, a.TENURE_LEGAL_DESCRIPTION,
           ROUND(SDO_GEOM.SDO_AREA(a.SHAPE, 0.005, 'unit=HECTARE'), 5) PARCEL_HECTARE, 
           SDO_UTIL.TO_WKTGEOMETRY(a.SHAPE) SHAPE
           
    FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a
      INNER JOIN (SELECT CROWN_LANDS_FILE, DISPOSITION_TRANSACTION_SID
                    FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW
                      GROUP BY CROWN_LANDS_FILE, DISPOSITION_TRANSACTION_SID
                        HAVING COUNT(INTRID_SID)=2) b             
          ON a.CROWN_LANDS_FILE = b.CROWN_LANDS_FILE 
            AND a.DISPOSITION_TRANSACTION_SID = b.DISPOSITION_TRANSACTION_SID
      
    WHERE a.RESPONSIBLE_BUSINESS_UNIT= 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
    
    ORDER BY a.CROWN_LANDS_FILE

               """
              
    return sql
         

def filter_TITAN (titan_report):
    """Returns a df of filtered TITAN report"""
    #Read TITAN report into df
    df = pd.read_excel(titan_report, 'TITAN_RPT012',
                       converters={'FILE #':str})
    df.loc[df['PURPOSE'] == 'AQUACULTURE', 'DISTRICT OFFICE'] = 'AQUACULTURE'
    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
    
    df.drop_duplicates(subset="FILE #", keep='first', inplace=True)
    df = df[['DISTRICT OFFICE', 'FILE #']]
    
    return df


           
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


def main():
    #"""Runs the program"""
        
    workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20230202_files_multipleParcels_shawn'
    
    print ('Connecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ('Running SQL Queries...')
    sql= load_queries ()
    df_a = read_query(connection,sql['=2']) 
    df_b = read_query(connection,sql['>2'])   
    
    df_a_em = df_a.loc[df_a['SHAPE'].isnull()]
    df_b_em = df_b.loc[df_b['SHAPE'].isnull()]
    
    df_a = df_a.loc[~df_a['DISPOSITION_TRANSACTION_SID'].isin(df_a_em['DISPOSITION_TRANSACTION_SID'].tolist())]
    df_b = df_b.loc[~df_b['DISPOSITION_TRANSACTION_SID'].isin(df_b_em['DISPOSITION_TRANSACTION_SID'].tolist())]
    
    print ('Computing Overlap Areas - 2 Parcels')   
    
    df_a['OVERLAP_AREA'] = 0
    l_files = sorted(df_a['DISPOSITION_TRANSACTION_SID'].unique())
    
    counter = 1
    for f in l_files:
        print ('...working on Disposition {}: {} of {}'.format(f,counter, len(l_files)))
        df_f = df_a.loc[df_a['DISPOSITION_TRANSACTION_SID'] == f]
        gdf = df_2_gdf (df_f, 3005)
    
        gdf_intr = gdf.overlay(gdf, how='intersection')
        gdf_intr = gdf_intr.loc[gdf_intr['PARCEL_ID_1'] != gdf_intr['PARCEL_ID_2']]
    
        if gdf_intr.shape[0] > 0:
    
            gdf_intr['PARCEL_ID_1'] = gdf_intr['PARCEL_ID_1'].astype('str')
            gdf_intr['PARCEL_ID_2'] = gdf_intr['PARCEL_ID_2'].astype('str')
            gdf_intr['PAIRS'] = gdf_intr.apply(lambda x: '-'.join(sorted(x[["PARCEL_ID_1", "PARCEL_ID_2"]])), axis=1)
            gdf_intr = gdf_intr.drop_duplicates(subset="PAIRS")
            
            gdf_intr['OVERLAP_AREA'] = round(gdf_intr['geometry'].area/ 10**4,3)
            ov_ar = gdf_intr['OVERLAP_AREA'].iloc[0]
            df_a.loc[ df_a['DISPOSITION_TRANSACTION_SID'] == f, 'OVERLAP_AREA'] = ov_ar
        else:
            pass
    
        
        counter += 1
    
    df_a['OVERLAP_PERCENT'] = round((df_a['OVERLAP_AREA'] /df_a['PARCEL_HECTARE'] * 100),0)
    
    df_a.drop('SHAPE', axis=1, inplace=True)
    
    
    print ('Computing Overlap Areas - >2 Parcels')   
    
    df_b['OVERLAP_AREA'] = 0
    l_files = sorted(df_b['DISPOSITION_TRANSACTION_SID'].unique())
    
    counter = 1
    for f in l_files:
        print ('...working on Disposition {}: {} of {}'.format(f,counter, len(l_files)))
        df_f = df_b.loc[df_b['DISPOSITION_TRANSACTION_SID'] == f]
        gdf = df_2_gdf (df_f, 3005)
    
        gdf_intr = gdf.overlay(gdf, how='intersection')
        gdf_intr = gdf_intr.loc[gdf_intr['PARCEL_ID_1'] != gdf_intr['PARCEL_ID_2']]
    
        if gdf_intr.shape[0] > 0:
    
            gdf_intr['PARCEL_ID_1'] = gdf_intr['PARCEL_ID_1'].astype('str')
            gdf_intr['PARCEL_ID_2'] = gdf_intr['PARCEL_ID_2'].astype('str')
            gdf_intr['PAIRS'] = gdf_intr.apply(lambda x: '-'.join(sorted(x[["PARCEL_ID_1", "PARCEL_ID_2"]])), axis=1)
            gdf_intr = gdf_intr.drop_duplicates(subset="PAIRS")
            
            gdf_intr['OVERLAP_AREA'] = round(gdf_intr['geometry'].area/ 10**4,3)
            
            sum_ov = gdf_intr['OVERLAP_AREA'].sum()
            sum_pc = gdf['PARCEL_HECTARE'].sum() 
            pct_ov = round((sum_ov/sum_pc)*100,2)
            
            df_b.loc[ df_b['DISPOSITION_TRANSACTION_SID'] == f, 'OVERLAP_AREA'] = sum_ov
            df_b.loc[ df_b['DISPOSITION_TRANSACTION_SID'] == f, 'OVERLAP_PERCENT'] = pct_ov
        else:
            pass
    
        
        counter += 1
    
    df_b['OVERLAP_PERCENT'] = df_b['OVERLAP_PERCENT'].fillna(0)
    
    df_b.drop('SHAPE', axis=1, inplace=True)
    
    df_c = pd.concat([df_a_em,df_b_em])
    df_c['PARCEL_HECTARE'] = 0
    df_c['SHAPE'] = 'None'
    
    
    print ('Exporting report')
    titan_report = os.path.join(workspace, 'TITAN_RPT012.xlsx')
    df_o = filter_TITAN (titan_report)
    
    
    
    df_c = pd.merge(df_o,df_c, how='right',right_on='CROWN_LANDS_FILE', left_on='FILE #')
    df_c.drop('FILE #', axis=1, inplace=True)
    
    df_a = pd.merge(df_o,df_a, how='right',right_on='CROWN_LANDS_FILE', left_on='FILE #')
    df_a.drop('FILE #', axis=1, inplace=True)
    
    df_b = pd.merge(df_o,df_b, how='right',right_on='CROWN_LANDS_FILE', left_on='FILE #')
    df_b.drop('FILE #', axis=1, inplace=True)
    
    
    df_list = [df_a,df_b,df_c]
    sheet_list = ['Files with 2 Parcels', 'Files  with >2 Parcels', 'Files with Empty Parcels']
    filename = '20230209_queryCleanup_landfilesParcels'
    generate_report (workspace, df_list, sheet_list,filename)

main()
