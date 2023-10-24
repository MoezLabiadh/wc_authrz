import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd


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
    
    sql['cmb']= """
                SELECT
                  wlw.WATER_LICENSING_WATERSHED_NAME,
                  CAST(IP.INTRID_SID AS NUMBER) INTEREST_PARCEL_ID,
                  CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_ID,
                  DS.FILE_CHR AS FILE_NBR,
                  SG.STAGE_NME AS STAGE,
                  --TT.ACTIVATION_CDE,
                  TT.STATUS_NME AS STATUS,
                  DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
                  --TS.EFFECTIVE_DAT AS EFFECTIVE_DATE,
                  DT.RECEIVED_DAT AS RECEIVED_DATE,
                  TY.TYPE_NME AS TENURE_TYPE,
                  ST.SUBTYPE_NME AS TENURE_SUBTYPE,
                  PU.PURPOSE_NME AS TENURE_PURPOSE,
                  SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
                  --DT.DOCUMENT_CHR,
                  --DT.ENTERED_DAT AS ENTERED_DATE,
                  --DT.COMMENCEMENT_DAT AS COMMENCEMENT_DATE,
                  --DT.EXPIRY_DAT AS EXPIRY_DATE,
                  --IP.AREA_CALC_CDE,
                  ROUND(IP.AREA_HA_NUM,2) AS AREA_HA,
                  DT.LOCATION_DSC
                  --OU.UNIT_NAME
                  --IP.LEGAL_DSC,
                  --CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY
                  --SP.SHAPE
                  
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
                
              JOIN WHSE_WATER_MANAGEMENT.WLS_WATER_LIC_WATERSHEDS_SP wlw
                ON SDO_RELATE (SP.SHAPE, wlw.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                  AND wlw.FWA_WATERSHED_GROUP_CODE = 'CAMB'  
            
            
            WHERE TT.ACTIVATION_CDE= 'ACT'
              AND DT.RECEIVED_DAT > TO_DATE('01/01/2021', 'DD/MM/YYYY')
              
            ORDER BY DT.RECEIVED_DAT DESC
    """
    return sql

if __name__==__name__:
    
    print ('\nConnecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ("\nRunning SQL queries...")
    sql = load_queries ()
    
    df= pd.read_sql(sql['cmb'], connection)