import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkt

xl= '20240618_hmnCore_PM_permissions_leases_licences.xlsx'
#leases
df_ls= pd.read_excel(xl, sheet_name='lease')
list_ls= ",".join("'"+str(x)+"'" for x in list(df_ls['FILE_NBR'].unique()))

#permissions
df_pr= pd.read_excel(xl, sheet_name='prmss')
list_pr= ",".join("'"+str(x)+"'" for x in list(df_pr['FILE_NBR'].unique()))

#licence
df_lc= pd.read_excel(xl, sheet_name='lcnce')
list_lc= ",".join("'"+str(x)+"'" for x in list(df_lc['FILE_NBR'].unique()))



#gen_perm
df_gn= pd.read_excel('2024 Lance - List of General Permissions in HMN Territory.xlsx')




#create a general permissions shape
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
    del gdf['SHAPE']
    
    return gdf

list_gn= ",".join(str(x) for x in df_gn['DTID'].unique())

sql=f"""
        SELECT* FROM(
        SELECT
              CAST(IP.INTRID_SID AS NUMBER) INTEREST_PARCEL_ID,
              CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_ID,
              DS.FILE_CHR AS FILE_NBR,
              SG.STAGE_NME AS STAGE,
              TT.STATUS_NME AS STATUS,
              DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
              TS.EFFECTIVE_DAT AS EFFECTIVE_DATE,
              TY.TYPE_NME AS TENURE_TYPE,
              ST.SUBTYPE_NME AS TENURE_SUBTYPE,
              PU.PURPOSE_NME AS TENURE_PURPOSE,
              SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
              DT.RECEIVED_DAT AS RECEIVED_DATE,
              DT.COMMENCEMENT_DAT AS COMMENCEMENT_DATE,
              DT.EXPIRY_DAT AS EXPIRY_DATE,
              DT.LOCATION_DSC,
              CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY,
              SDO_UTIL.TO_WKTGEOMETRY(SHAPE) AS SHAPE 
              
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
        	
          JOIN WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES SH
            ON SH.INTRID_SID = IP.INTRID_SID
            
          JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP FN
            ON SDO_RELATE (SH.SHAPE, FN.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
              AND FN.CNSLTN_AREA_NAME = q'[Hul'qumi'num Nations - Core Territory]'
              AND FN.CONTACT_NAME = 'Halalt First Nation') TN
        
        WHERE 
            TN.DISPOSITION_TRANSACTION_ID IN ({list_gn})
         
        ORDER BY TN.EFFECTIVE_DATE DESC
"""
hostname = 'bcgw.bcgov/idwprod1.bcgov'
bcgw_user = os.getenv('bcgw_user')
bcgw_pwd = os.getenv('bcgw_pwd')
connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
df = pd.read_sql(sql, connection) 
gdf = df_2_gdf (df, 3005)  
gdf.to_file('PM_general_permissions.shp')





