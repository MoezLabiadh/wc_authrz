'''
This script performs the following workflow:
1) Connect to BCGW
2) Execute the SQL queries
3) Export the query results to Feature Classes
'''


import warnings
warnings.simplefilter(action='ignore')

import os
import arcpy
import pyodbc
import pandas as pd


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



def load_queries ():
    """ Return the SQL queries that will be executed in the main script"""
    sql = {}
    sql['lands_auth'] = """
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
                  
                  ON TF.DISPOSITION_TRANSACTION_SID = TN.DISPOSITION_TRANSACTION_SID;
                 """  

    sql['forest_auth'] = """
          SELECT 
            frr.FOREST_FILE_ID,
            frr.CUTTING_PERMIT_ID,
            frr.MAP_LABEL,
            frr.FILE_TYPE_DESCRIPTION,
            frr.FILE_STATUS_CODE,
            frr.LIFE_CYCLE_STATUS_CODE,
            frr.HARVEST_AUTH_STATUS_CODE,
            frr.ISSUE_DATE,
            amdd.AMEND_STATUS_DATE,
            
            CASE 
              WHEN amdd.AMEND_STATUS_DATE > frr.ISSUE_DATE+ 5 
                THEN 'AMENDEMENT' 
                  ELSE 'NEW' 
                    END AS NEW_AMEND,
                    
            frr.EXPIRY_DATE,
            EXTRACT(YEAR FROM frr.EXPIRY_DATE) - EXTRACT(YEAR FROM frr.ISSUE_DATE) AS TENURE_LENGTH_YRS,
            ROUND(SDO_GEOM.SDO_AREA(frr.GEOMETRY, 0.005, 'unit=HECTARE'), 2) AREA_HA,
            
            CASE 
              WHEN frr.ADMIN_DISTRICT_CODE = 'DSI' 
                THEN 'SOUTH' 
                  ELSE 'NORTH' 
                    END AS REGION,
                    
            SDO_UTIL.TO_WKTGEOMETRY(frr.GEOMETRY) SHAPE 

          FROM WHSE_FOREST_TENURE.FTEN_HARVEST_AUTH_POLY_SVW frr

            JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
              ON SDO_RELATE (frr.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                
            LEFT JOIN (
                      WITH CTE AS (
                          SELECT
                              amd.FOREST_FILE_ID || ' ' || amd.CUTTING_PERMIT_ID AS MAP_LABEL,
                              amd.AMEND_STATUS_DATE,
                              ROW_NUMBER() OVER (PARTITION BY amd.FOREST_FILE_ID, amd.CUTTING_PERMIT_ID ORDER BY amd.AMEND_STATUS_DATE) AS rn
                          FROM WHSE_FOREST_TENURE.FTEN_HARVEST_AMEND amd
                          WHERE amd.AMEND_STATUS_DATE BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY')
                      )
                      SELECT MAP_LABEL, AMEND_STATUS_DATE
                      FROM CTE
                      WHERE rn = 1
            ) amdd
              ON amdd.MAP_LABEL = frr.MAP_LABEL
                  
          WHERE frr.LIFE_CYCLE_STATUS_CODE <> 'PENDING'
            AND (amdd.AMEND_STATUS_DATE BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY') 
                  OR 
                (frr.ISSUE_DATE BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY')AND amdd.AMEND_STATUS_DATE is NULL)) 
                  
          ORDER BY frr.MAP_LABEL;
                 """ 
       
    sql['forest_road'] = """
            SELECT ftr.MAP_LABEL,
                  ftr.ROAD_SECTION_LENGTH AS ROAD_SECTION_LENGTH_KM,
                  ftr.FILE_TYPE_DESCRIPTION,
                  ftr.FILE_STATUS_CODE,
                  ftr.LIFE_CYCLE_STATUS_CODE,
                  rd.ENTRY_TIMESTAMP,
                  rd.UPDATE_TIMESTAMP,
                  rd.CHANGE_TIMESTAMP4,
                  ftr.AWARD_DATE,
                  ftr.EXPIRY_DATE,
                  EXTRACT(YEAR FROM ftr.EXPIRY_DATE) - EXTRACT(YEAR FROM ftr.AWARD_DATE) AS TENURE_LENGTH_YRS,
                  CASE 
                      WHEN ftr.AWARD_DATE > rd.CHANGE_TIMESTAMP4+ 5 
                        THEN 'NEW' 
                          ELSE 'AMENDEMENT' 
                            END AS NEW_AMEND,
                  CASE 
                    WHEN ftr.GEOGRAPHIC_DISTRICT_CODE = 'DSI' 
                      THEN 'SOUTH' 
                        ELSE 'NORTH' 
                          END AS REGION,
                          
                  SDO_UTIL.TO_WKTGEOMETRY(rd.GEOMETRY) SHAPE 
                    
            FROM (
                SELECT rdd.ENTRY_TIMESTAMP,
                      rdd.UPDATE_TIMESTAMP,
                      rdd.REVISION_COUNT,
                      rdd.RETIREMENT_DATE,
                      rdd.CHANGE_TIMESTAMP4,
                      rdd.UPDATE_USERID,
                      rdd.FOREST_FILE_ID || ' ' || rdd.ROAD_SECTION_ID AS MAP_LABEL,
                      rdd.GEOMETRY
                FROM WHSE_FOREST_TENURE.FTEN_ROAD_LINES rdd
                JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                ON SDO_RELATE(rdd.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                  AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                ) rd
              JOIN WHSE_FOREST_TENURE.FTEN_ROAD_SECTION_LINES_SVW ftr
                ON ftr.MAP_LABEL = rd.MAP_LABEL

            WHERE ftr.LIFE_CYCLE_STATUS_CODE <> 'PENDING'
              AND rd.RETIREMENT_DATE IS NULL
              AND (UPDATE_USERID NOT LIKE '%DATAFIX%' AND UPDATE_USERID NOT LIKE '%datafix%')
              AND (rd.CHANGE_TIMESTAMP4 BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY') 
                    OR 
                  ftr.AWARD_DATE BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY'))

            ORDER BY ftr.MAP_LABEL;
                 """ 
    
    sql['recr_poly'] = """
            SELECT rcp.FOREST_FILE_ID,
                  ROUND(SDO_GEOM.SDO_AREA(rcpv.GEOMETRY, 0.005, 'unit=HECTARE'), 2) AREA_HA,
                  rcpv.FILE_STATUS_CODE,
                  rcpv.PROJECT_TYPE,
                  rcpv.LIFE_CYCLE_STATUS_CODE,
                  rcpv.PROJECT_ESTABLISHED_DATE,
                  CASE 
                      WHEN rcpv.PROJECT_ESTABLISHED_DATE >= rcp.CHANGE_TIMESTAMP3
                        THEN 'NEW' 
                          ELSE 'AMENDEMENT' 
                            END AS NEW_AMEND,
                  CASE 
                    WHEN rcpv.GEOGRAPHIC_DISTRICT_CODE = 'DSI' 
                      THEN 'SOUTH' 
                        ELSE 'NORTH' 
                          END AS REGION,
                  rcp.ENTRY_TIMESTAMP,
                  rcp.UPDATE_TIMESTAMP,
                  rcp.CHANGE_TIMESTAMP3,
                  SDO_UTIL.TO_WKTGEOMETRY(rcpv.GEOMETRY) SHAPE 

            FROM (
                  SELECT  rcpp.FOREST_FILE_ID,
                          rcpp.RETIREMENT_DATE,
                          rcpp.ENTRY_USERID,  
                          rcpp.UPDATE_USERID,
                          rcpp.ENTRY_TIMESTAMP,
                          rcpp.UPDATE_TIMESTAMP,
                          rcpp.CHANGE_TIMESTAMP3
                  
                  FROM WHSE_FOREST_TENURE.FTEN_RECREATION_POLY rcpp
                  JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                    ON SDO_RELATE (rcpp.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                      AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                  )rcp
                  
              JOIN WHSE_FOREST_TENURE.FTEN_RECREATION_POLY_SVW rcpv
                ON rcp.FOREST_FILE_ID = rcpv.FOREST_FILE_ID
              
            WHERE rcpv.LIFE_CYCLE_STATUS_CODE <> 'PENDING'
              AND rcp.RETIREMENT_DATE IS NULL
              AND (rcp.UPDATE_USERID NOT LIKE '%DATAFIX%' AND rcp.UPDATE_USERID NOT LIKE '%datafix%')
              AND (rcp.CHANGE_TIMESTAMP3 BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY') 
                    OR 
                  rcpv.PROJECT_ESTABLISHED_DATE BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY'))

            ORDER BY rcp.FOREST_FILE_ID;
                 """ 
    

    sql['recr_line'] = """
            SELECT rcp.MAP_LABEL,
                rcpv.FEATURE_LENGTH AS LENGTH_KM,
                rcpv.FILE_STATUS_CODE,
                rcpv.PROJECT_TYPE,
                rcpv.LIFE_CYCLE_STATUS_CODE,
                rcpv.PROJECT_ESTABLISHED_DATE,
                CASE 
                    WHEN rcpv.PROJECT_ESTABLISHED_DATE >= rcp.CHANGE_TIMESTAMP3
                      THEN 'NEW' 
                        ELSE 'AMENDEMENT' 
                          END AS NEW_AMEND,
                CASE 
                  WHEN rcpv.DISTRICT_CODE = 'DSI' 
                    THEN 'SOUTH' 
                      ELSE 'NORTH' 
                        END AS REGION,
                rcp.ENTRY_TIMESTAMP,
                rcp.UPDATE_TIMESTAMP,
                rcp.CHANGE_TIMESTAMP3,
                SDO_UTIL.TO_WKTGEOMETRY(rcpv.GEOMETRY) SHAPE 

          FROM (
                SELECT  rcpp.FOREST_FILE_ID || ' ' || rcpp.SECTION_ID AS MAP_LABEL,
                        rcpp.RETIREMENT_DATE,
                        rcpp.ENTRY_USERID,  
                        rcpp.UPDATE_USERID,
                        rcpp.ENTRY_TIMESTAMP,
                        rcpp.UPDATE_TIMESTAMP,
                        rcpp.CHANGE_TIMESTAMP3
                
                FROM WHSE_FOREST_TENURE.FTEN_RECREATION_LINE rcpp
                JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                  ON SDO_RELATE (rcpp.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                    AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
                )rcp
                
            JOIN WHSE_FOREST_TENURE.FTEN_RECREATION_LINES_SVW rcpv
              ON rcp.MAP_LABEL = rcpv.MAP_LABEL
            
          WHERE rcpv.LIFE_CYCLE_STATUS_CODE <> 'PENDING'
            AND rcp.RETIREMENT_DATE IS NULL
            AND (rcp.UPDATE_USERID NOT LIKE '%DATAFIX%' AND rcp.UPDATE_USERID NOT LIKE '%datafix%')
            AND (rcp.CHANGE_TIMESTAMP3 BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY') 
                  OR 
                rcpv.PROJECT_ESTABLISHED_DATE BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY'))

          ORDER BY rcp.MAP_LABEL;
                 """ 
    
    sql['spec_use'] = """
          SELECT supv.MAP_LABEL,
                ROUND(SDO_GEOM.SDO_AREA(supv.GEOMETRY, 0.005, 'unit=HECTARE'), 2) AREA_HA,
                supv.SPECIAL_USE_DESCRIPTION,
                supv.FILE_STATUS_CODE,
                supv.AMENDMENT_ID,
                CASE 
                    WHEN supv.AMENDMENT_ID = 0
                      THEN 'NEW' 
                        ELSE 'AMENDEMENT' 
                          END AS NEW_AMEND,
                supv.LIFE_CYCLE_STATUS_CODE,
                sup.ENTRY_TIMESTAMP,
                sup.UPDATE_TIMESTAMP,

                CASE 
                  WHEN supv.ADMIN_DISTRICT_CODE = 'DSI' 
                    THEN 'SOUTH' 
                      ELSE 'NORTH' 
                        END AS REGION,
                        
                  SDO_UTIL.TO_WKTGEOMETRY(supv.GEOMETRY) SHAPE 
                  
          FROM WHSE_FOREST_TENURE.FTEN_SPEC_USE_PERMIT_POLY_SVW supv
            JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
              ON SDO_RELATE (supv.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
              AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
              
            JOIN WHSE_FOREST_TENURE.FTEN_SPEC_USE_PERMIT sup
              ON sup.FOREST_FILE_ID = supv.MAP_LABEL

          WHERE supv.LIFE_CYCLE_STATUS_CODE <> 'PENDING'
            AND supv.RETIREMENT_DATE IS NULL
            AND (sup.UPDATE_USERID NOT LIKE '%DATAFIX%' AND sup.UPDATE_USERID NOT LIKE '%datafix%')
            AND sup.ENTRY_TIMESTAMP BETWEEN TO_DATE('01/09/{prvy}', 'DD/MM/YYYY') AND TO_DATE('31/08/{y}', 'DD/MM/YYYY')

          ORDER BY supv.MAP_LABEL;
                 """
    return sql



def df_to_featureclass(output_gdb, geometry_type, feature_class_name, data_frame):
    """Create a feature class based on pandas dataframe (with geometry info)."""
    
    #Create an empty feature class in the output gdb
    spatial_reference = arcpy.SpatialReference(3005) # BCalbers
    arcpy.CreateFeatureclass_management(output_gdb, feature_class_name, 
                                        geometry_type, spatial_reference=spatial_reference)

    # Add fields from the df to the feature class
    feature_class= os.path.join(output_gdb, feature_class_name)
    fields = data_frame.columns.tolist()
    fields.remove('SHAPE') #remove the geometry field from the list of attributes

    for field_name in fields:
        arcpy.AddField_management(feature_class, field_name, "TEXT")

    # Populate data (geometry+attributes) from df to feature class
    with arcpy.da.InsertCursor(feature_class, ['SHAPE@WKT'] + fields) as cursor:
        for index, row in data_frame.iterrows():
            feature = row['SHAPE']
            values = [row[field] for field in fields]
            rowdata= [feature] + values
            cursor.insertRow(rowdata)    



def main():
    print ('Connect to BCGW.')
    # connection parmaters to BCGW
    driver = 'Oracle in OraClient12Home1'
    server = 'bcgw.bcgov'
    port= '1521'
    dbq= 'idwprod1'
    hostname = 'bcgw.bcgov/idwprod1.bcgov'

    username= 'MLABIADH' #BCGW Username ################## CHANGE THIS################
    password= 'MoezLab8823' #BCGW Password ################## CHANGE THIS################

    connection= connect_to_DB (driver,server,port,dbq,username,password)

    # reporting year
    year = 2023

    print ("\nLoad the SQL queries...")
    sql = load_queries ()

    print ("\nRun the process")
    workspace= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20230815_maanulth_reporting_2023\arcpy_tests' # ################## CHANGE THIS################
    output_gdb = os.path.join(workspace, 'maanulth_proj.gdb')  # ################## CHANGE THIS################

    # iterate through the sqls, run queries and export result to featureclass
    counter= 1
    for k, v in sql.items():
        # add Year and Year-1 parameters to the sql
        print ("..working on SQL {} of {}: {}".format(counter, len(sql),k))
        query = v.format(y= year, prvy=year-1)

        # read the query into a dataframe
        print ("....executing the query")
        df= pd.read_sql(query,connection)

        nbr_rows= df.shape[0] 
        if nbr_rows == 0:
            print ('......query returned 0 results, no featureclass will be exported')
        else:
            print ("....exporting {} results to a feature class".format(nbr_rows))
            arcpy.env.overwriteOutput = True

            # set the output gdb and the feature class name
            feature_class_name = k

            # convert the df to feature class
              ## determine the geometry type.
            wkt_str = df.loc[0, 'SHAPE']

            shape_value = wkt_str.split(" ")[0]
            if shape_value in ['POLYGON', 'MULTIPOLYGON']:
              geometry_type= "POLYGON"
            elif shape_value in ['LINESTRING','MULTILINESTRING']:
                geometry_type= "POLYLINE"
            elif shape_value in ['POINT ','MULTIPOINT']:
                geometry_type= "POINT"

            df_to_featureclass(output_gdb, geometry_type, feature_class_name, df)

        counter+= 1

    print ("\n Processign Completed!")



main()
