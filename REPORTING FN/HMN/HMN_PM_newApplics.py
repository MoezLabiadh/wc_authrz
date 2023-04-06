import os
import cx_Oracle
import datetime
import pandas as pd


def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection



sql= """
SELECT
      CASE pip.CNSLTN_AREA_NAME
        WHEN q'[Hul'qumi'num Nations - Core Territory]'
          THEN 'CORE'
        ELSE 'MARINE'
       END AS HMN_TERRITORY,
      --CAST(IP.INTRID_SID AS NUMBER) INTRID_SID,
      DS.FILE_CHR AS FILE_NBR,
      CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_SID,

      SG.STAGE_NME AS STAGE,
      --TT.ACTIVATION_CDE,
      TT.STATUS_NME AS STATUS,
      DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
      --TS.EFFECTIVE_DAT,
      DT.RECEIVED_DAT AS RECEIVED_DATE,
      TY.TYPE_NME AS TENURE_TYPE,
      ST.SUBTYPE_NME AS TENURE_SUBTYPE,
      PU.PURPOSE_NME AS TENURE_PURPOSE,
      SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
      --DT.DOCUMENT_CHR,
      --DT.ENTERED_DAT,
      --DT.COMMENCEMENT_DAT,
      --DT.EXPIRY_DAT AS DT_EXPIRY_DAT,
      --IP.AREA_CALC_CDE,
      --IP.AREA_HA_NUM,
      DT.LOCATION_DSC,
      --OU.UNIT_NAME,
      --IP.LEGAL_DSC,
      IH.ORGANIZATIONS_LEGAL_NAME AS HOLDER_ORGANNSATION_NAME,
      IH.INDIVIDUALS_FIRST_NAME || ' ' || IH.INDIVIDUALS_LAST_NAME AS HOLDER_INDIVIDUAL_NAME,
      IH.CITY AS HOLDER_CITY,
      IH.REGION_CDE AS HOLDER_REGION,
      IH.COUNTRY_CDE AS HOLDER_COUNTRY,
      PR.WORK_AREA_CODE || PR.WORK_EXTENSION_NUMBER|| PR.WORK_PHONE_NUMBER AS HOLDER_PHONE
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
   JOIN (SELECT MIN (B.ROW_UNIQUEID), 
               B.DISPOSITION_TRANSACTION_SID,
               B.INTERESTED_PARTY_SID,
               B.ORGANIZATIONS_LEGAL_NAME,
               B.INDIVIDUALS_FIRST_NAME,
               B.INDIVIDUALS_LAST_NAME,
               B.CITY,
               B.COUNTRY_CDE,
               B.REGION_CDE
        FROM WHSE_TANTALIS.TA_INTEREST_HOLDER_VW B

        GROUP BY B.DISPOSITION_TRANSACTION_SID,
               B.INTERESTED_PARTY_SID,
               B.ORGANIZATIONS_LEGAL_NAME,
               B.INDIVIDUALS_FIRST_NAME,
               B.INDIVIDUALS_LAST_NAME,
               B.CITY,
               B.COUNTRY_CDE,
               B.REGION_CDE) IH
    ON IH.DISPOSITION_TRANSACTION_SID = DT.DISPOSITION_TRANSACTION_SID
   JOIN WHSE_TANTALIS.TA_INTERESTED_PARTIES PR
    ON PR.INTERESTED_PARTY_SID = IH.INTERESTED_PARTY_SID
   JOIN WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES SP
    ON SP.INTRID_SID = IP.INTRID_SID
   
   JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
    ON SDO_RELATE (pip.SHAPE, SP.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
    
WHERE (pip.CNSLTN_AREA_NAME = q'[Hul'qumi'num Nations - Marine Territory]' 
    OR pip.CNSLTN_AREA_NAME = q'[Hul'qumi'num Nations - Core Territory]' )  
  AND pip.CONTACT_NAME = 'Cowichan Tribes'
  AND SG.STAGE_NME = 'APPLICATION'
  AND TT.STATUS_NME = 'ACCEPTED'
  AND DT.APPLICATION_TYPE_CDE = 'NEW'
  AND SP.SUBPURPOSE_NME = 'PRIVATE MOORAGE'

ORDER BY DT.RECEIVED_DAT DESC
"""

def create_report (df_list, sheet_list,filename):
    """ Exports dataframes to multi-tab excel spreasheet"""

    
    writer = pd.ExcelWriter(filename+'.xlsx',engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]
        workbook = writer.book

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'count'})
        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()
    
    
def main():    

    print ('Connecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    #bcgw_user = 'XXXX'
    bcgw_pwd = os.getenv('bcgw_pwd')
    #bcgw_pwd = 'XXXX'
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ('Running the Query')
    df = pd.read_sql(sql,connection)
    
    df.drop_duplicates(subset="FILE_NBR", keep='first', inplace=True)
    
    df['RECEIVED_DATE'] =  pd.to_datetime(df['RECEIVED_DATE'],
                                        infer_datetime_format=True,
                                        errors = 'coerce').dt.date
    
    print ('Export report')
    create_report ([df], ['result'],'pm_applics_hmn_asof20230217')

main()
