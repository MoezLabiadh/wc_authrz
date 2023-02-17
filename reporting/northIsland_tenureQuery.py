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
      DI.DISTRICT_NAME,
      DS.FILE_CHR AS FILE_NBR,
      CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_SID,
      SG.STAGE_NME AS TENURE_STAGE,
      TT.STATUS_NME AS TENURE_STATUS,
      DT.COMMENCEMENT_DAT AS COMMENCEMENT_DATE,
      DT.EXPIRY_DAT AS EXPIRY_DATE,
      EXTRACT(YEAR FROM DT.EXPIRY_DAT) - EXTRACT(YEAR FROM DT.COMMENCEMENT_DAT) AS TENURE_LENGTH_YEARS,
      TY.TYPE_NME AS TENURE_TYPE,
      ST.SUBTYPE_NME AS TENURE_SUBTYPE,
      PU.PURPOSE_NME AS TENURE_PURPOSE,
      SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
      DT.LOCATION_DSC
      
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

   JOIN WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES SP
    ON SP.INTRID_SID = IP.INTRID_SID
    
   JOIN WHSE_ADMIN_BOUNDARIES.ADM_NR_DISTRICTS_SP DI
    ON  SDO_RELATE (DI.SHAPE,SP.SHAPE ,'mask=ANYINTERACT') = 'TRUE'
      
  WHERE OU.UNIT_NAME = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
    AND DI.DISTRICT_NAME = 'North Island - Central Coast Natural Resource District'
    AND TT.STATUS_NME = 'DISPOSITION IN GOOD STANDING'
    AND DT.COMMENCEMENT_DAT BETWEEN TO_DATE('01/01/2018', 'DD/MM/YYYY') AND TO_DATE('31/12/2022', 'DD/MM/YYYY')
    AND SG.STAGE_NME = 'TENURE'
    AND SP.SUBPURPOSE_NME <> 'LOG HANDLING/STORAGE'
    AND EXTRACT(YEAR FROM DT.EXPIRY_DAT) - EXTRACT(YEAR FROM DT.COMMENCEMENT_DAT) < 10

ORDER BY DT.COMMENCEMENT_DAT DESC
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
  df['EXPIRY_DATE'] =  pd.to_datetime(df['EXPIRY DATE'],
                                      infer_datetime_format=True,
                                      errors = 'coerce').dt.date


  df['COMMENCEMENT_DATE'] =  pd.to_datetime(df['COMMENCEMENT DATE'],
                                      infer_datetime_format=True,
                                      errors = 'coerce').dt.date

  print ('Export report')
  create_report ([df], ['result'],'20230217_northIsland_tenureQuery')


main()
