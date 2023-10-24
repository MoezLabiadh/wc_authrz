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
                  --wlw.FWA_WATERSHED_GROUP_CODE,
                  --CAST(IP.INTRID_SID AS NUMBER) INTEREST_PARCEL_ID,
                  --CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_ID,
                  DS.FILE_CHR AS FILE_NBR,
                  SG.STAGE_NME AS STAGE,
                  --TT.ACTIVATION_CDE,
                  TT.STATUS_NME AS STATUS,
                  DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
                  --TS.EFFECTIVE_DAT AS EFFECTIVE_DATE,
                  DT.RECEIVED_DAT AS RECEIVED_DATE,
                  EXTRACT(YEAR FROM DT.RECEIVED_DAT) AS RECEIVED_YEAR,
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
    
    
if __name__==__name__:
    
    print ('\nConnecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ("\nRunning SQL queries...")
    sql = load_queries ()
    
    df= pd.read_sql(sql['cmb'], connection)
    df['RECEIVED_DATE'] =  pd.to_datetime(df['RECEIVED_DATE'], infer_datetime_format=True, errors = 'coerce').dt.date
    
    df.drop_duplicates(['FILE_NBR'], inplace= True)
    
    print ("\nExporting Results...")
    workspace= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20231024_campbellRiver_landApplics'
    generate_report (workspace, [df], ['list'],'20231024_campbellRiv_landsApplics')