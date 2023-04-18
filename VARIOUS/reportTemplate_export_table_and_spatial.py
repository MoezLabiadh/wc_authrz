import os
import cx_Oracle
import pandas as pd
import geopandas as gpd

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
    df['geometry'] = gpd.GeoSeries.from_wkt(df['SHAPE'])
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    #df['geometry'] = df['SHAPE'].apply(wkt.loads)
    #gdf = gpd.GeoDataFrame(df, geometry = df['geometry'])
    gdf.crs = "EPSG:" + str(crs)
    del df['SHAPE']
    
    return gdf


def load_sql():
    sql= {}
    sql['aqua'] = """
                SELECT
                      --CAST(IP.INTRID_SID AS NUMBER) AS PARCEL_ID,
                      CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) AS DISPOSITION_TRANSACTION_ID,
                      DS.FILE_CHR AS FILE_NBR,
                      SG.STAGE_NME AS STAGE,
                      TT.ACTIVATION_CDE,
                      TT.STATUS_NME AS STATUS,
                      DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
                      TS.EFFECTIVE_DAT AS EFFECTIVE_DATE,
                      TY.TYPE_NME AS TENURE_TYPE,
                      ST.SUBTYPE_NME AS TENURE_SUBTYPE,
                      PU.PURPOSE_NME AS TENURE_PURPOSE,
                      SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
                      DT.DOCUMENT_CHR,
                      DT.RECEIVED_DAT AS RECEIVED_DATE,
                      --DT.ENTERED_DAT AS ENTERED_DATE,
                      DT.COMMENCEMENT_DAT AS COMMENCEMENT_DATE,
                      DT.EXPIRY_DAT AS EXPIRY_DATE,
                      IP.AREA_CALC_CDE,
                      IP.AREA_HA_NUM AS AREA_HA,
                      DT.LOCATION_DSC,
                      --OU.UNIT_NAME,
                      --IP.LEGAL_DSC,
                      --CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY
                      SDO_UTIL.TO_WKTGEOMETRY(SP.SHAPE) AS SHAPE
                      
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
                
                WHERE TT.ACTIVATION_CDE = 'ACT'
                  AND TT.STATUS_NME = 'DISPOSITION IN GOOD STANDING'
                  AND PU.PURPOSE_NME = 'AQUACULTURE'
                
                ORDER BY TS.EFFECTIVE_DAT DESC
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
    
    

def main():
    print ('Connecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ('Running the Query')
    sql = load_sql()
    df = pd.read_sql(sql['aqua'],connection)
    
    gdf = df_2_gdf (df, 3005)
    
    for col in gdf.columns:
        #if gdf[col].dtype == 'datetime64[ns]':
        if col != 'geometry':
            gdf[col] = gdf[col].astype(str)
    
    
    gdf.to_file('aquaculture_tenures_asof20230418.shp',driver="ESRI Shapefile")
    
    del df['geometry']
    
    #disp = df['DISPOSITION_TRANSACTION_SID'].to_list() 
    print ('Export report')
    create_report ([df], ['result'],'aquaculture_tenures_asof20230418')

main()
