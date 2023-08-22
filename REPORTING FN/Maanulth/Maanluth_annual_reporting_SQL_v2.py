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
# Updated:     22-08-2023
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
import fiona



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



def load_queries ():
    """ Return the SQL queries that will be executed"""
    sql = {}
    sql['maan'] = """
                SELECT --TN.INTRID_SID, 
                       --TN.DISPOSITION_TRANSACTION_SID,
                       TN.FILE_NBR,
                       TN.STAGE,
                       TN.STATUS,
                       TN.APPLICATION_TYPE,
                       --TN.EFFECTIVE_DATE,
                       TN.TENURE_TYPE,
                       TN.TENURE_SUBTYPE,
                       TN.TENURE_PURPOSE,
                       TN.TENURE_SUBPURPOSE,
                       TN.TENURE_PURPOSE || ' ' || '-' || ' ' || TN.TENURE_SUBPURPOSE AS FULL_PURPOSE,
                       TF.OFFERED_DATE, 
                       TN.EXPIRY_DATE,
                       (EXTRACT(YEAR FROM TN.EXPIRY_DATE) - EXTRACT(YEAR FROM TF.OFFERED_DATE)) AS TENURE_LENGTH_YRS,
                       ROUND(TN.AREA_HA,2) AS AREA_HA,
                       SDO_UTIL.TO_WKTGEOMETRY(TN.SHAPE) SHAPE
                       --TN.LOCATION_DSC,
                       --TN.CLIENT_NAME_PRIMARY
                      
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
                
                JOIN (SELECT DISPOSITION_TRANSACTION_SID, EFFECTIVE_DAT AS OFFERED_DATE 
                      FROM WHSE_TANTALIS.TA_DISP_TRANS_STATUSES
                      WHERE CODE_CHR_STATUS = 'OF'
                      AND EFFECTIVE_DAT BETWEEN TO_DATE('01/09/{py}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY')) TF
                      
                     ON TF.DISPOSITION_TRANSACTION_SID = TN.DISPOSITION_TRANSACTION_SID
                 """
    
    sql['iha'] = """
                SELECT
               ipr.CROWN_LANDS_FILE, iha.AREA_TYPE, iha.TREATY_SIDE_AGREEMENT_ID,
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
         


def get_maan_tenures (year, connection, sql):
    """Returns a df containing Tenures offered within Maanulth Territory"""
        
    query = sql['maan'].format(y= year, py=year-1)
    df_maan_geo = read_query(connection,query)
    
    df_maan =  df_maan_geo.drop(['SHAPE'], axis=1)
    
   
    df_maan_ar = df_maan.groupby('FILE_NBR')[['AREA_HA']].apply(sum).reset_index()
    
    df_maan =  df_maan.drop(['AREA_HA'], axis=1)
    
    df_maan = pd.merge(df_maan_ar, df_maan, on='FILE_NBR')
    
    df_maan.drop_duplicates('FILE_NBR', inplace= True)
    
    for col in df_maan.columns:
        if 'DATE' in col:
            df_maan[col] = pd.to_datetime(df_maan[col]).dt.date
            
    df_maan.sort_values(by = ['OFFERED_DATE'], inplace = True)
    df_maan.reset_index(drop=True, inplace= True)

    
    return df_maan,df_maan_geo


def get_iha_overlaps (df_maan,connection, sql):
    """Return a df containing Tenures overlapping with IHAs"""
    s_maan= ",".join("'" + str(x) + "'" for x in df_maan['FILE_NBR'].tolist())
    query = sql['iha'].format(tm= s_maan)
    df_iha = read_query(connection,query)
    
    df_iha = df_iha.groupby(['CROWN_LANDS_FILE','TREATY_SIDE_AGREEMENT_AREA_ID'])\
                            [['OVERLAP_HECTARE']].apply(sum).reset_index()
    
    df_iha.sort_values(by = ['CROWN_LANDS_FILE'], inplace = True)
    
    return df_iha
 
    
 
def get_lu_overlaps (df_maan,connection, sql):
    """"Return a df containing overlaps of Tenures and Land Use Units"""
    s_maan= ",".join("'" + str(x) + "'" for x in df_maan['FILE_NBR'].tolist())
    query = sql['lu'].format(tm= s_maan)
    df_lu = read_query(connection,query)
    
    df_lu = df_lu.groupby(['CROWN_LANDS_FILE', 'LANDSCAPE_UNIT_NAME'])\
                            ['OVERLAP_HECTARE'].sum().reset_index()
    
    df_lu.sort_values(by = ['CROWN_LANDS_FILE'], inplace = True)
    
    df_lu_sum = df_lu.groupby('LANDSCAPE_UNIT_NAME')[['OVERLAP_HECTARE']].apply(sum).reset_index()
    
    return df_lu, df_lu_sum


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



def get_FN_overlaps (gdf_maan, indNat_fc):
    """"Return a df containing overlaps with individual Maanulth FNs"""
    gdf_nat = esri_to_gdf (indNat_fc)
    gdf_intr = gpd.overlay(gdf_maan, gdf_nat, how='intersection')
    gdf_intr['OVERLAP_HECTARE'] = round(gdf_intr.area/10**4,2)
    #df_nat = pd.DataFrame(gdf_intr)
    df_nat= gdf_intr.groupby(['FILE_NBR','FN_area'])[['OVERLAP_HECTARE']].apply(sum).reset_index()
    
    
    return df_nat


    
def generate_spatial_files(gdf, workspace, year):
    """Generate a Shapefile of Tenures"""

    shp_name = os.path.join(workspace, 'maan_report_{}_shapes.shp'.format(str(year)))
    kml_name = os.path.join(workspace, 'maan_report_{}_shapes.kml'.format(str(year)))
    #if not os.path.isfile(shp_name):
        
    for col in gdf.columns:
        if 'DATE' in col:
            gdf[col] = gdf[col].astype(str)
             
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
  
  workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20230815_maanulth_reporting_2023\aug22'
  
  print ('Connecting to BCGW...')
  hostname = 'bcgw.bcgov/idwprod1.bcgov'
  bcgw_user = os.getenv('bcgw_user')
  bcgw_pwd = os.getenv('bcgw_pwd')
  connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
      
  year = 2023
  
  print ("Loading SQL queries...")
  sql = load_queries ()
  
  print ("SQL-1: Getting Tenures within Maanulth Territory...")
  df_maan, df_maan_geo= get_maan_tenures (year, connection, sql)
  
  print ("SQL-2: Getting overlaps with Important Harvest Areas...")
  df_iha = get_iha_overlaps (df_maan,connection, sql)
  
  print ("SQL-3: Getting overlaps with Landscape Units...")
  df_lu, df_lu_sum = get_lu_overlaps (df_maan,connection, sql) 
  
  print ('Creating Spatial file')
  gdf_maan = df_2_gdf (df_maan_geo, 3005)
  
  
  print ("Getting overlaps with Individual Nations")
  indNat_fc= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\DATASETS\Maa-nulth.gdb\PreTreatyFirstNationAreas'
  df_nat= get_FN_overlaps(gdf_maan, indNat_fc)
  
  print ('Generating Outputs...')
  df_list = [df_maan,df_iha, df_nat, df_lu, df_lu_sum]
  sheet_list = ['Offered Tenures in Maanulth', 'Overlay - IHA', 'Overlay - Nations','Overlay - LU', 'LU Area Summary']
  filename = 'Maanulth_annualReporting_{}_tables'.format(str(year))
  generate_report (workspace, df_list, sheet_list,filename)
  
  generate_spatial_files(gdf_maan, workspace, year)
 
main ()
