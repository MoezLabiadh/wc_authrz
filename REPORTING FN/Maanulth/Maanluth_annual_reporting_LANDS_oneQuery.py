#-------------------------------------------------------------------------------
# Name:        Maanluth Annual Reporting
#
# Purpose:     This script generates information required
#              for Maanluth Annual Reporting
#
# Input(s):    (1) BCGW connection parameters
#              (2) Reporting Year (e.g 2023)
#              (3) Workspace (folder) where outputs will be generated.
#
# Author:      Moez Labiadh - GeoBC
#
# Created:     2023-09-26
# Updated:     2024-09-03
#-------------------------------------------------------------------------------


import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import numpy as np
import pandas as pd
import geopandas as gpd
import fiona
import timeit


def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("...Successffuly connected to the database")
    except:
        raise Exception('...Connection failed! Please verifiy your login parameters')

    return connection


def load_queries():
    """ Return the SQL queries that will be executed"""
    sql= {}
    
    sql['lus']= """
                SELECT ldw.LANDSCAPE_UNIT_NAME
                FROM WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW ldw
                  JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                    ON SDO_RELATE(ldw.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT')= 'TRUE' 
                      AND  pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                """
                
    sql['main']= """
                SELECT TN.INTRID_SID, 
                       TN.FILE_NBR,
                       ldu.LANDSCAPE_UNIT_NAME AS LANDSCAPE_UNIT,
                       CASE
                        WHEN iha.TREATY_SIDE_AGREEMENT_AREA_ID IS NOT NULL
                          THEN 'Yes'
                            ELSE 'No'
                              END AS OVERLAP_IHA,
                       iha.TREATY_SIDE_AGREEMENT_AREA_ID AS IHA_ID,  
                       TN.STAGE,
                       TN.STATUS,
                       TN.APPLICATION_TYPE,
                       TN.EFFECTIVE_DATE,
                       TN.TENURE_TYPE,
                       TN.TENURE_SUBTYPE,
                       TN.TENURE_TYPE || ' ' || '-' || ' ' || TN.TENURE_SUBTYPE AS FULL_TYPE,
                       TN.TENURE_PURPOSE,
                       TN.TENURE_SUBPURPOSE,
                       TN.TENURE_PURPOSE || ' ' || '-' || ' ' || TN.TENURE_SUBPURPOSE AS FULL_PURPOSE,
                       TF.OFFERED_DATE, 
                       TN.EXPIRY_DATE,
                       (EXTRACT(YEAR FROM TN.EXPIRY_DATE) - EXTRACT(YEAR FROM TF.OFFERED_DATE)) AS TENURE_LENGTH_YRS,
                       ROUND(TN.AREA_HA,2) AS AREA_HA, 
                       SDO_UTIL.TO_WKTGEOMETRY(TN.SHAPE) SHAPE
       
                FROM(
                SELECT
                      CAST(IP.INTRID_SID AS NUMBER) INTRID_SID,
                      CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_SID,
                      DS.FILE_CHR AS FILE_NBR,
                      SG.STAGE_NME AS STAGE,
                      TT.STATUS_NME AS STATUS,
                      DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
                      TS.EFFECTIVE_DAT AS EFFECTIVE_DATE,
                      TY.TYPE_NME AS TENURE_TYPE,
                      ST.SUBTYPE_NME AS TENURE_SUBTYPE,
                      PU.PURPOSE_NME AS TENURE_PURPOSE,
                      SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
                      DT.COMMENCEMENT_DAT AS COMMENCEMENT_DATE,
                      DT.EXPIRY_DAT AS EXPIRY_DATE,
                      IP.AREA_CALC_CDE,
                      IP.AREA_HA_NUM AS AREA_HA,
                      DT.LOCATION_DSC,
                      OU.UNIT_NAME,
                      --IP.LEGAL_DSC,
                      CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY,
                      SP.SHAPE
                      
                FROM WHSE_TANTALIS.TA_DISPOSITION_TRANSACTIONS DT 
                  JOIN WHSE_TANTALIS.TA_INTEREST_PARCELS IP 
                    ON DT.DISPOSITION_TRANSACTION_SID = IP.DISPOSITION_TRANSACTION_SID
                      AND IP.EXPIRY_DAT IS NULL
                  JOIN WHSE_TANTALIS.TA_DISP_TRANS_STATUSES TS
                    ON DT.DISPOSITION_TRANSACTION_SID = TS.DISPOSITION_TRANSACTION_SID 
                      AND TS.EXPIRY_DAT IS NULL
                  JOIN WHSE_TANTALIS.TA_DISPOSITIONS DS
                    ON DS.DISPOSITION_SID = DT.DISPOSITION_SID
                  JOIN WHSE_TANTALIS.TA_STAGES SG 
                    ON SG.CODE_CHR = TS.CODE_CHR_STAGE
                  JOIN WHSE_TANTALIS.TA_STATUS TT 
                    ON TT.CODE_CHR = TS.CODE_CHR_STATUS
                  JOIN WHSE_TANTALIS.TA_AVAILABLE_TYPES TY 
                    ON TY.TYPE_SID = DT.TYPE_SID    
                  JOIN WHSE_TANTALIS.TA_AVAILABLE_SUBTYPES ST 
                    ON ST.SUBTYPE_SID = DT.SUBTYPE_SID 
                      AND ST.TYPE_SID = DT.TYPE_SID 
                  JOIN WHSE_TANTALIS.TA_AVAILABLE_PURPOSES PU 
                    ON PU.PURPOSE_SID = DT.PURPOSE_SID    
                  JOIN WHSE_TANTALIS.TA_AVAILABLE_SUBPURPOSES SP 
                    ON SP.SUBPURPOSE_SID = DT.SUBPURPOSE_SID 
                      AND SP.PURPOSE_SID = DT.PURPOSE_SID 
                  JOIN WHSE_TANTALIS.TA_ORGANIZATION_UNITS OU 
                    ON OU.ORG_UNIT_SID = DT.ORG_UNIT_SID 
                  JOIN WHSE_TANTALIS.TA_TENANTS TE 
                    ON TE.DISPOSITION_TRANSACTION_SID = DT.DISPOSITION_TRANSACTION_SID
                      AND TE.SEPARATION_DAT IS NULL
                      AND TE.PRIMARY_CONTACT_YRN = 'Y'
                  JOIN WHSE_TANTALIS.TA_INTERESTED_PARTIES PR
                    ON PR.INTERESTED_PARTY_SID = TE.INTERESTED_PARTY_SID
                  JOIN WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES SP
                    ON SP.INTRID_SID = IP.INTRID_SID
                    
                  JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                    ON SDO_RELATE (SP.SHAPE, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                      AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                       
                WHERE TT.STATUS_NME IN ('DISPOSITION IN GOOD STANDING', 'OFFERED', 'OFFER ACCEPTED')
                
                ORDER BY TS.EFFECTIVE_DAT DESC) TN
                
                --Add Offered Date  
                JOIN (SELECT DISPOSITION_TRANSACTION_SID, EFFECTIVE_DAT AS OFFERED_DATE 
                      FROM WHSE_TANTALIS.TA_DISP_TRANS_STATUSES
                      WHERE CODE_CHR_STATUS = 'OF'
                      AND EFFECTIVE_DAT BETWEEN TO_DATE('01/09/{prvyr}', 'DD/MM/YYYY') AND TO_DATE('31/08/{yr}', 'DD/MM/YYYY')) TF  
                  ON TF.DISPOSITION_TRANSACTION_SID = TN.DISPOSITION_TRANSACTION_SID
                      
                 -- Add Landscape Units    
                JOIN WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW ldu
                  ON SDO_RELATE (ldu.GEOMETRY, TN.SHAPE, 'mask=ANYINTERACT')= 'TRUE'
                  AND ldu.LANDSCAPE_UNIT_NAME IN ({lus})
                          
                -- Add IHAs                                   
                LEFT JOIN WHSE_LEGAL_ADMIN_BOUNDARIES.FNT_TREATY_SIDE_AGREEMENTS_SP iha
                    ON SDO_RELATE (iha.GEOMETRY, TN.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                       AND iha.AREA_TYPE = 'Important Harvest Area'
                       AND iha.STATUS = 'ACTIVE'      
    """
    return sql


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
    #del df['SHAPE']
    del gdf['SHAPE']
    
    return gdf


def get_FN_overlaps (gdf_maan, indNat_fc):
    """"Return a df containing overlaps with individual Maanulth FNs"""
    gdf_nat = esri_to_gdf (indNat_fc)
    gdf_intr = gpd.overlay(gdf_maan, gdf_nat, how='intersection')
    gdf_intr['OVERLAP_HECTARE'] = round(gdf_intr.area/10**4,2)
    df_nat= gdf_intr.groupby(['FILE_NBR','FN_area_r'])[['OVERLAP_HECTARE']].apply(sum).reset_index()
    
    df_nat=df_nat[['FILE_NBR','FN_area_r']]
    
    return df_nat


def calculate_stats (df):
    df_st= df.drop_duplicates(subset=['FILE_NBR'])
    df_st= df_st[['FILE_NBR','FULL_TYPE','FULL_PURPOSE']]
    df_st_pr= df_st.groupby(['FULL_PURPOSE'])['FULL_PURPOSE'].count()
    df_st_pr = df_st.groupby('FULL_PURPOSE').size().reset_index(name='PURPOSE_COUNT')
    df_st_lu= df.groupby('LANDSCAPE_UNIT').size().reset_index(name='LU_COUNT')
    
    return df_st, df_st_pr, df_st_lu


def generate_spatial_files(gdf, workspace, year):
    """Generate a Shapefile of Tenures"""

    shp_name = os.path.join(workspace, f'maan_report_{str(year)}_shapes.shp')
    kml_name = os.path.join(workspace, f'maan_report_{str(year)}_shapes.kml')
    #if not os.path.isfile(shp_name):
        
    for col in gdf.columns:
        if 'DATE' in col:
            gdf[col] = gdf[col].astype(str)
             
    gdf.to_file(shp_name, driver="ESRI Shapefile")
    
    #generate seperte shapefiles for each tenure nbr
    files= gdf['FILE_NBR'].to_list()
    for file in files:
        gdf_f= gdf[gdf['FILE_NBR']== file]
        
        
        shps_folder = os.path.join(workspace, 'shapefiles')
        os.makedirs(shps_folder, exist_ok=True)
        
        shp_f_name= os.path.join(shps_folder, f'maan_report_{str(year)}_file{file}.shp'.format(str(year)))
        gdf_f.to_file(shp_f_name, driver="ESRI Shapefile")
    
    
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

    writer.close()
    

if __name__ == "__main__": 
    start_t = timeit.default_timer() #start time

    ############################ CHANGE WORKSPACE AND REPORTING YEAR ########################################
    workspace= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20250805_maanulth_reporting_2025'
    fiscal= 2025  
    ############################ CHANGE WORKSPACE AND REPORTING YEAR ########################################
        
    print ('\nConnecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')     #################### REPLACE WITH BCGW CREDENTIALS ###################          
    bcgw_pwd = os.getenv('bcgw_pwd')       #################### REPLACE WITH BCGW CREDENTIALS ################### 
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ("\nRunning SQL queries...")
    sql = load_queries ()
    
    df_lsu= pd.read_sql(sql['lus'], connection)
    lsu_list= ",".join("'"+ str(x) +"'" for x in df_lsu['LANDSCAPE_UNIT_NAME'].to_list())
    
    query= sql['main'].format(lus= lsu_list,
                              yr= fiscal,
                              prvyr= fiscal-1)
    
    df_geo= pd.read_sql(query, connection)
    
    # Create a df without Geometry information
    df= df_geo.drop(columns=['SHAPE'])
    
    print ('\nGenerating overlaps with individual FNs...')
    gdf_tn = df_2_gdf (df_geo, 3005)
    gdf_tn.drop_duplicates(subset=['FILE_NBR'], inplace= True)
    gdf_tn.drop(columns=['LANDSCAPE_UNIT','OVERLAP_IHA', 'IHA_ID'], inplace= True)
    gdf_tn.rename(columns={'INTRID_SID': 'PARCEL_ID'}, inplace=True)
    
    indNat_fc= r'W:\lwbc\visr\Workarea\moez_labiadh\DATASETS\Maa-nulth.gdb\PreTreatyFirstNationAreas'   #### PATH TO MAANLUTH INDIVIDUAL NATIONS BOUNDARIES ####
    df_nat= get_FN_overlaps(gdf_tn, indNat_fc)
    
    print ("\nCleaning up results...")
    #caluclating total areas (files with multiple parcels)
    df_areas = df.drop_duplicates(subset=['FILE_NBR', 'INTRID_SID'])
    df_areas = df_areas.groupby('FILE_NBR')['AREA_HA'].sum().reset_index()
    df_areas.rename(columns={'AREA_HA': 'TOTAL_AREA_HA'}, inplace=True)
    df= pd.merge(df, df_areas,how= 'left', on= 'FILE_NBR')
    df.drop(columns=['INTRID_SID','AREA_HA'], inplace= True)
    
    #grouping IHA IDs into single row
    df_iha= df[['FILE_NBR','IHA_ID']]
    df_iha['IHA_ID'] = pd.to_numeric(df['IHA_ID'], errors='coerce').fillna(0).astype(int)
    
    df_iha = df_iha.groupby('FILE_NBR')['IHA_ID'].agg(lambda x: ', '.join(map(str, set(x)))).reset_index()
    
    df.drop(columns=['IHA_ID'], inplace= True)
    df= pd.merge(df, df_iha, how= 'left', on= 'FILE_NBR')
    
    df.drop_duplicates(subset=['FILE_NBR', 'LANDSCAPE_UNIT'], inplace=True)
    df['IHA_ID'] = df['IHA_ID'].replace({'0': None})
    
    #add FN info to the main df
    df_nat.rename(columns={'FN_area_r': 'FN'}, inplace=True)
    df_nat = df_nat.groupby('FILE_NBR')['FN'].agg(lambda x: ' & '.join(map(str, set(x)))).reset_index()
    
    df= pd.merge(df, df_nat, how= 'left', on= 'FILE_NBR')
    
    #cleanup columns
    for col in df.columns:
        if 'DATE' in col:
            df[col] =  pd.to_datetime(df[col], infer_datetime_format=True, errors = 'coerce').dt.date
    
    #add extra columns
    df['AGENCY']= 'FLNR'
    df['LEGISLATION']= 'Land Act'
    df['SPATIAL']= 'Yes'
    df['LAT_LONG']= None
    df['ENAGAGE']= None
    df['ENAGAGE_DET']= None
    df['AMEND_DATE']= None
    
    #replace values as per Annual reporting template
    df['STAGE'] = np.where(df['STAGE'] == 'TENURE', 'T', 'A')
    df['APPLICATION_TYPE'] = np.where(df['APPLICATION_TYPE'] == 'NEW', 'New', 'Renewal')
    
    df['TENURE_LENGTH_YRS'] = np.where(df['TENURE_LENGTH_YRS'] == 0, 1, df['TENURE_LENGTH_YRS'])
    df['TENURE_LENGTH_YRS']= df['TENURE_LENGTH_YRS'].fillna(9999)
    df['TENURE_LENGTH_YRS']= df['TENURE_LENGTH_YRS'].astype(int).astype(str)
    df['TENURE_LENGTH_YRS'] = df['TENURE_LENGTH_YRS'].replace({'9999': 'N/A'})
    
    df.sort_values(by='OFFERED_DATE', inplace= True)
    
    cols= ['LANDSCAPE_UNIT','FILE_NBR','AGENCY','LEGISLATION','FULL_TYPE','FULL_PURPOSE','STAGE',
           'APPLICATION_TYPE','OFFERED_DATE','TENURE_LENGTH_YRS','TOTAL_AREA_HA','SPATIAL','LAT_LONG',
           'OVERLAP_IHA','IHA_ID','ENAGAGE','ENAGAGE_DET','FN','AMEND_DATE']
    df= df[cols]
    
    #calculate stats
    df_st, df_st_pr, df_st_lu= calculate_stats(df)
    
    print ("\nExporting  outputs...")
    dfs= [df, df_st, df_st_pr, df_st_lu]
    sheets= ['master_report', 'stats_all', 'stats_auth', 'stats_lu']
    filename = 'Maanulth_annualReporting_{}_tables'.format(str(fiscal))
    generate_report (workspace, dfs, sheets, filename)
    
    generate_spatial_files(gdf_tn, workspace, fiscal)
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print ('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs)) 
