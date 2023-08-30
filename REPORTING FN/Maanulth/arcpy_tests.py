import os
import pyodbc
import pandas as pd

import arcpy

def connect_to_DB (driver,server,port,dbq, username,password):
    """ Returns a connection to Oracle database"""
    try:
        connectString ="""
                    DRIVER={driver};
                    SERVER={server}:{port};
                    DBQ={dbq};
                    Uid={uid};
                    Pwd={pwd}
                       """.format(driver=driver,server=server, port=port,
                                  dbq=dbq,uid=username,pwd=password)

        connection = pyodbc.connect(connectString)
        print  ("...Successffuly connected to the database")
    except:
        raise Exception('...Connection failed! Please check your connection parameters')

    return connection


def read_query(connection,query):
    "Returns a df containing SQL Query results"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        cols = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame.from_records(rows, columns=cols)
    
    finally:
        if cursor is not None:
            cursor.close()
            connection.close()


def load_queries ():
    """ Return the SQL queries that will be executed"""
    sql = {}
    sql['maan'] = """
                SELECT --TN.INTRID_SID, 
                       TN.FILE_NBR,
                       TN.STAGE,
                       TN.STATUS,
                       TN.APPLICATION_TYPE,
                       TN.TENURE_TYPE,
                       TN.TENURE_SUBTYPE,
                       TN.TENURE_PURPOSE,
                       TN.TENURE_SUBPURPOSE,
                       TN.TENURE_PURPOSE || ' ' || '-' || ' ' || TN.TENURE_SUBPURPOSE AS FULL_PURPOSE,
                       TF.OFFERED_DATE, 
                       TN.EXPIRY_DATE,
                       (EXTRACT(YEAR FROM TN.EXPIRY_DATE) - EXTRACT(YEAR FROM TF.OFFERED_DATE)) AS TENURE_LENGTH_YRS,
                       ROUND(TN.AREA_HA,2) AS AREA_HA,
                       --TN.SHAPE
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
                      AND EFFECTIVE_DAT BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY')) TF
                      
                     ON TF.DISPOSITION_TRANSACTION_SID = TN.DISPOSITION_TRANSACTION_SID
                 """
         
    return sql


def get_maan_tenures (year, connection, sql):
    """Returns a df containing Tenures offered within Maanulth Territory"""
        
    query = sql['maan'].format(y= year, prvy=year-1)
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




def create_feature_class(output_workspace, output_name, field_names):
    """Create a feature class in the specified workspace with the given fields."""
    geometry_type = "POLYGON"
    spatial_reference = arcpy.SpatialReference(3005) 

    arcpy.CreateFeatureclass_management(output_workspace, output_name, geometry_type, spatial_reference=spatial_reference)

    # Add fields to the feature class
    for field_name in field_names:
        arcpy.AddField_management(os.path.join(output_workspace, output_name), field_name, "TEXT")



def insert_features(feature_class, data_frame,field_names):
    """Insert features from DataFrame into the feature class."""
    fields = field_names
    
    with arcpy.da.InsertCursor(feature_class, ['SHAPE@WKT'] + fields) as cursor:
        for index, row in data_frame.iterrows():
            feature = row['SHAPE']
            values = [row[field] for field in fields]
            rowdata= [feature] + values
            cursor.insertRow(rowdata)
    
    print(f"...Inserted {len(data_frame)} features into '{feature_class}'")




print ('Connecting to BCGW.')
driver = 'Oracle in OraClient12Home1'
server = 'bcgw.bcgov'
port= '1521'
dbq= 'idwprod1'
hostname = 'bcgw.bcgov/idwprod1.bcgov'

connection= connect_to_DB (driver,server,port,dbq,'MLABIADH','MoezLab8823')

year = 2023

print ("\nLoading SQL queries...")
sql = load_queries ()

print ("\nSQL-1: Getting Tenures within Maanulth Territory...")
df_maan, df_maan_geo= get_maan_tenures (year, connection, sql)


print ("\nCreating a FeatureClass")

output_workspace = r"\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20230815_maanulth_reporting_2023\arcpy_tests\maanulth_proj.gdb"  # Update this path
output_feature_class = "Maanulth_Tenures"

arcpy.env.overwriteOutput = True

# Get the column names of df_maan_geo_without_shape as fields
field_names = df_maan.columns.tolist()

# Create the feature class with fields from df_maan_geo
create_feature_class(output_workspace, output_feature_class, field_names)

# Insert features from df_maan_geo into the feature class
feature_class_path = os.path.join(output_workspace, output_feature_class)
insert_features(feature_class_path, df_maan_geo, field_names)
