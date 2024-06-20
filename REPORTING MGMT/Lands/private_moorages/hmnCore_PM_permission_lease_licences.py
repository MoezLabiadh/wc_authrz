import os
import json
import cx_Oracle
import pandas as pd

class OracleConnector:
    def __init__(self, dbname='BCGW'):
        self.dbname = dbname
        self.cnxinfo = self.get_db_cnxinfo()

    def get_db_cnxinfo(self):
        """ Retrieves db connection params from the config file"""
        with open(r'H:\config\db_config.json', 'r') as file:
            data = json.load(file)
        
        if self.dbname in data:
            return data[self.dbname]
        
        raise KeyError(f"Database '{self.dbname}' not found.")
    
    def connect_to_db(self):
        """ Connects to Oracle DB and create a cursor"""
        try:
            self.connection = cx_Oracle.connect(self.cnxinfo['username'], 
                                                self.cnxinfo['password'], 
                                                self.cnxinfo['hostname'], 
                                                encoding="UTF-8")
            self.cursor = self.connection.cursor()
            print  ("..Successffuly connected to the database")
        except Exception as e:
            raise Exception(f'..Connection failed: {e}')

    def disconnect_db(self):
        """Close the Oracle connection and cursor"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            print("....Disconnected from the database")

def load_queries():
    sql= {}
    sql ['prmss']= """
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
               CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY
               --SH.SHAPE
               
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
             TN.STATUS IN ('DISPOSITION IN GOOD STANDING', 'ACCEPTED')
             AND TN.TENURE_SUBPURPOSE = 'PRIVATE MOORAGE' 
             AND TENURE_TYPE = 'PERMISSION'
          
         ORDER BY TN.EFFECTIVE_DATE DESC
        """
        
    sql ['lease']= """
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
               CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY
               --SH.SHAPE
               
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
             TN.STATUS IN ('DISPOSITION IN GOOD STANDING', 'ACCEPTED')
             AND TN.TENURE_SUBPURPOSE = 'PRIVATE MOORAGE' 
             AND TENURE_TYPE = 'LEASE'
          
         ORDER BY TN.EFFECTIVE_DATE DESC
        """        

    sql ['lcnce']= """
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
               CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY
               --SH.SHAPE
               
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
             TN.STATUS IN ('DISPOSITION IN GOOD STANDING', 'ACCEPTED')
             AND TN.TENURE_SUBPURPOSE = 'PRIVATE MOORAGE' 
             AND TENURE_TYPE = 'LICENCE'
          
         ORDER BY TN.EFFECTIVE_DATE DESC
        """   
        
    return sql
        
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
    
    
if __name__ == "__main__": 

    print ('Connecting to BCGW.')
    Oracle = OracleConnector()
    Oracle.connect_to_db()
    connection= Oracle.connection


    print ('Running Queries')
    try:
        sql= load_queries()
        
        df_dict= {}
        
        for k, v in sql.items():
            print (f'...running query: {k}')
            df = pd.read_sql(v,connection)
            
            for col in df.columns:
                if 'DATE' in col:
                    df[col] =  pd.to_datetime(df[col],
                                                infer_datetime_format=True,
                                                errors = 'coerce').dt.date
            
            df_dict[k]= df

    except Exception as e:
        raise Exception(f"Error occurred: {e}")  
        
    finally: 
        Oracle.disconnect_db()    
        
    print ('Export report')
    
    create_report (df_dict.values(), df_dict.keys(),'20240618_hmnCore_PM_permissions_leases_licences')