#-------------------------------------------------------------------------------
# Name:        Maanluth Annual Reporting
#
# Purpose:     This script generates information required
#              for Maanluth Annual Reporting
#
# Input(s):    (1) Workspace (folder) where outputs will be generated.
#              (2) Titan report (excel file) - TITAN_RPT009
#              (3) Report Year (e.g 2022)
#              (4) BCGW connection parameters
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     23-01-2023
# Updated:     15-08-2023
#-------------------------------------------------------------------------------


import warnings
warnings.simplefilter(action='ignore')

import os
import datetime
import cx_Oracle
import pandas as pd
import geopandas as gpd
#from shapely import wkt
import fiona
import numpy as np


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


def get_titan_report_date (titan_report):
    """ Returns the date of the input TITAN report"""
    df = pd.read_excel(titan_report,'Info')
    titan_date = df.columns[1]

    return titan_date


def filter_TITAN (titan_report, year):
    """Returns a df of filtered TITAN report"""
    #Read TITAN report into df
    df = pd.read_excel(titan_report, 'TITAN_RPT009',
                       converters={'FILE NUMBER':str,
                                   'OFFER ACCEPTED DATE':str})

    # Convert expiry date column to datetime format
    df['OFFERED DATE'] =  pd.to_datetime(df['OFFERED DATE'],
                                    infer_datetime_format=True,
                                    errors = 'coerce').dt.date

    df['EXPIRY DATE'] =  pd.to_datetime(df['EXPIRY DATE'],
                                    infer_datetime_format=True,
                                    errors = 'coerce').dt.date


    # Filter the needed data: tenures expiring during fiscal year
    df = df.loc [(df['OFFERED DATE'] >= datetime.date(year-1,9,1)) &
                 (df['OFFERED DATE'] <= datetime.date(year,8,31)) &
                 (~df['STATUS'].isin(['CANCELLED', 'EXPIRED']))]

    #Calculate Tenure Length
    df ['diff'] = ((df['EXPIRY DATE'] - df['OFFERED DATE'] )\
                                  / np.timedelta64(1,'Y'))
    df['TENURE LENGTH YEARS'] = df['diff'].fillna(0).round().astype(int)

    #Remove spaces from column names, remove special characters
    df.sort_values(by = ['OFFERED DATE'], inplace = True)
    #df['OFFERED DATE'] = df['OFFERED DATE'].astype(str)
    #df['EXPIRY DATE'] = df['EXPIRY DATE'].astype(str)
    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
    #df.columns = df.columns.str.replace(' ', '_')

    return df


def load_queries ():
    """ Return the SQL queries that will be executed"""
    sql = {}
    sql['maan'] = """
                SELECT
               ipr.INTRID_SID, ipr.CROWN_LANDS_FILE,
               ROUND (ipr.TENURE_AREA_IN_HECTARES, 2) TENURE_HECTARE, 
               ROUND(SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(pip.SHAPE,ipr.SHAPE, 0.005), 0.005, 'unit=HECTARE'), 2) OVERLAP_HECTARE,
               SDO_UTIL.TO_WKTGEOMETRY(ipr.SHAPE) SHAPE
            
            FROM
                WHSE_TANTALIS.TA_CROWN_TENURES_SVW ipr,
                WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                
            
             WHERE pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
               AND (ipr.TENURE_STAGE = 'TENURE' 
                    OR (ipr.TENURE_STAGE = 'APPLICATION' AND ipr.TENURE_STATUS in ('OFFERED', 'OFFER ACCEPTED')))
               AND ipr.CROWN_LANDS_FILE in ({t})
               AND SDO_RELATE (pip.SHAPE, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                 """
    
    sql['iha'] = """
                SELECT
               ipr.INTRID_SID, ipr.CROWN_LANDS_FILE, iha.AREA_TYPE, iha.TREATY_SIDE_AGREEMENT_ID,
               iha.TREATY_SIDE_AGREEMENT_AREA_ID,iha.STATUS IHA_STATUS, iha.START_DATE_TEXT, iha.END_DATE_TEXT,
               ROUND (ipr.TENURE_AREA_IN_HECTARES, 2) TENURE_HECTARE, 
               ROUND(SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(iha.GEOMETRY,ipr.SHAPE, 0.005), 0.005, 'unit=HECTARE'), 2) OVERLAP_HECTARE
            
            FROM
                WHSE_TANTALIS.TA_CROWN_TENURES_SVW ipr,
                WHSE_LEGAL_ADMIN_BOUNDARIES.FNT_TREATY_SIDE_AGREEMENTS_SP iha
            
             WHERE ipr.CROWN_LANDS_FILE in ({tm})
               --AND ipr.TENURE_STAGE = 'TENURE' 
               AND iha.AREA_TYPE = 'Important Harvest Area'
               AND iha.STATUS = 'ACTIVE'
               AND SDO_RELATE (iha.GEOMETRY, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                 """    

    sql['lu'] = """
                SELECT
                    ipr.CROWN_LANDS_FILE,
                    ldm.LANDSCAPE_UNIT_NAME,
                    ROUND(SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(ldm.GEOMETRY, ipr.SHAPE, 0.005), 0.005, 'unit=HECTARE'), 2) AS OVERLAP_HECTARE
                FROM
                    WHSE_TANTALIS.TA_CROWN_TENURES_SVW ipr
                JOIN
                    (
                        SELECT
                            ldu.LANDSCAPE_UNIT_NAME,
                            ldu.GEOMETRY
                        FROM
                            WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW ldu
                        JOIN
                            WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                        ON
                            SDO_RELATE(pip.SHAPE, ldu.GEOMETRY, 'mask=ANYINTERACT') = 'TRUE'
                        AND
                            pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                    ) ldm
                ON
                    SDO_RELATE(ldm.GEOMETRY, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                WHERE
                    ipr.CROWN_LANDS_FILE IN ({tm})
                 """           
    return sql
         

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


def df_2_gdf (df, crs):
    """ Return a geopandas gdf based on a df with Geometry column"""
    df['SHAPE'] = df['SHAPE'].astype(str)
    df['geometry'] = gpd.GeoSeries.from_wkt(df['SHAPE'])
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    #df['geometry'] = df['SHAPE'].apply(wkt.loads)
    #gdf = gpd.GeoDataFrame(df, geometry = df['geometry'])
    gdf.crs = "EPSG:" + str(crs)
    del df['SHAPE']
    del gdf['SHAPE']
    
    return gdf


def get_maan_tenures (df_off,connection,sql):
    """Returns a df containing Tenures offered within Maanulth Territory"""
        
    s_off = ",".join("'" + str(x) + "'" for x in df_off['FILE NUMBER'].tolist())
    query = sql['maan'].format(t= s_off)
    df_maan = read_query(connection,query)
    
    gdf_maan = df_2_gdf (df_maan, 3005)
    gdf_maan.drop(['OVERLAP_HECTARE'], axis=1, inplace=True)
    
    df_maan = df_maan.groupby('CROWN_LANDS_FILE')[['TENURE_HECTARE', 'OVERLAP_HECTARE']].apply(sum).reset_index()
    
    df_maan = pd.merge(df_maan, df_off, how= 'left', left_on='CROWN_LANDS_FILE', right_on='FILE NUMBER')
    
    df_maan['PURPOSE FULL'] = df_maan['PURPOSE'] + ' - ' + df_maan['SUBPURPOSE']
    
    df_maan = df_maan[['FILE NUMBER', 'DISTRICT OFFICE', 'STATUS', 'TASK DESCRIPTION', 
                       'OFFERED DATE', 'OFFER ACCEPTED DATE','EXPIRY DATE', 'TENURE LENGTH YEARS',
                       'TYPE','SUBTYPE','PURPOSE','SUBPURPOSE','PURPOSE FULL','TENURE_HECTARE','OVERLAP_HECTARE']]
    
    for col in df_maan.columns:
        if 'DATE' in col:
            df_maan[col] = pd.to_datetime(df_maan[col]).dt.date
            
    df_maan.sort_values(by = ['FILE NUMBER'], inplace = True)
    
    return df_maan, gdf_maan


def get_iha_overlaps (df_maan,connection, sql):
    """Return a df containing Tenures overlapping with IHAs"""
    s_maan= ",".join("'" + str(x) + "'" for x in df_maan['FILE NUMBER'].tolist())
    query = sql['iha'].format(tm= s_maan)
    df_iha = read_query(connection,query)
    df_iha.sort_values(by = ['CROWN_LANDS_FILE'], inplace = True)
    
    return df_iha
 
    
def get_lu_overlaps (df_maan,connection, sql):
    """"Return a df containing overlaps of Tenures and Land Use Units"""
    s_maan= ",".join("'" + str(x) + "'" for x in df_maan['FILE NUMBER'].tolist())
    query = sql['lu'].format(tm= s_maan)
    df_lu = read_query(connection,query)
    
    df_lu = df_lu.groupby(['CROWN_LANDS_FILE', 'LANDSCAPE_UNIT_NAME'])\
                            ['OVERLAP_HECTARE'].sum().reset_index()
    
    df_lu.sort_values(by = ['CROWN_LANDS_FILE'], inplace = True)
    
    df_lu_sum = df_lu.groupby('LANDSCAPE_UNIT_NAME')[['OVERLAP_HECTARE']].apply(sum).reset_index()
    
    return df_lu, df_lu_sum


def generate_spatial_files(gdf, workspace, year):
    """Generate a Shapefile of Tenures"""

    shp_name = os.path.join(workspace, 'maan_report_{}_shapes.shp'.format(str(year)))
    kml_name = os.path.join(workspace, 'maan_report_{}_shapes.kml'.format(str(year)))
    #if not os.path.isfile(shp_name):
    gdf.to_file(shp_name, driver="ESRI Shapefile")
    
    gdf = gdf.to_crs(4326)    
    
    if os.path.isfile(kml_name):
        os.remove(kml_name)
        
    fiona.supported_drivers['KML'] = 'rw'
    gdf.to_file(kml_name, driver='KML')

           
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
    """Runs the program"""
    
    workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20230815_maanulth_reporting_2023\aug16'
    titan_report = os.path.join(workspace, 'TITAN_RPT009.xlsx')
    
    print ('Connecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
        
    titan_date = get_titan_report_date (titan_report)
    print ('Titan report date/time is: {}'.format (titan_date))
    
    print ("TITAN filtering: Getting Offered Tenures...")
    year = 2023
    df_off = filter_TITAN (titan_report,year)
    
    print ("Loading SQL queries...")
    sql = load_queries ()
    
    print ("SQL-1: Getting Tenures within Maanulth Territory...")
    df_maan, gdf_maan = get_maan_tenures (df_off,connection, sql)
    
    print ("SQL-2: Getting overlaps with Important Harvest Areas...")
    df_iha = get_iha_overlaps (df_maan,connection, sql)
    
    print ("SQL-3: Getting overlaps with Landscape Units...")
    df_lu, df_lu_sum = get_lu_overlaps (df_maan,connection, sql) 
    
    print ("Getting overlaps with Individual Nations")
    indNat_fc = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\DATASETS\Maa-nulth.gdb\PreTreatyFirstNationAreas'
    gdf_nat = esri_to_gdf (indNat_fc)
    gdf_intr = gpd.overlay(gdf_maan, gdf_nat, how='intersection')
    gdf_intr['OVERLAP_AREA'] = round(gdf_intr.area/10**4,2)
    df_nat = pd.DataFrame(gdf_intr)
    df_nat=df_nat[['INTRID_SID', 'CROWN_LANDS_FILE', 'FN_area', 'TENURE_HECTARE','OVERLAP_AREA']]
    
    print ('Generating Outputs...')
    df_list = [df_maan,df_iha, df_nat, df_lu, df_lu_sum]
    sheet_list = ['Offered Tenures in Maanulth', 'Overlay - IHA', 'Overlay - Nations','Overlay - LU', 'LU Area Summary']
    filename = 'Maanulth_annualReporting_{}_tables'.format(str(year))
    generate_report (workspace, df_list, sheet_list,filename)
    
    generate_spatial_files(gdf_maan, workspace, year)
    
main ()
