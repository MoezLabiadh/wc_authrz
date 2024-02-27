import warnings
warnings.simplefilter(action='ignore')

import os
import json
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkb, wkt


def get_db_cnxinfo (dbname='BCGW'):
    """ Retrieves db connection params from the config file"""
    
    with open(r'H:\config\db_config.json', 'r') as file:
        data = json.load(file)
        
    if dbname in data:
        cnxinfo = data[dbname]

        return cnxinfo
    
    raise KeyError(f"Database '{dbname}' not found.")
    
    
def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        cursor = connection.cursor()
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection, cursor


def read_query(connection,cursor,query,bvars):
    "Returns a df containing SQL Query results"
    cursor.execute(query, bvars)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
    
    return df   


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


def get_wkb_srid(gdf):
    """Returns WKB objects from gdf"""
    wkb_lib= {}
    
    for index, row in gdf.iterrows():
        name= row['Name']
        wkb_aoi = wkb.dumps(row['geometry'], output_dimension=2)
        wkb_lib[name] = wkb_aoi

    return wkb_lib


def load_queries ():
    sql = {}

    sql ['ah-csrv'] = """
SELECT* FROM(
SELECT
      CAST(IP.INTRID_SID AS NUMBER) INTEREST_PARCEL_ID,
      CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_ID,
      DS.FILE_CHR AS FILE_NBR,
      ROUND(SDO_GEOM.SDO_DISTANCE(SH.SHAPE, SDO_GEOMETRY(:wkb_aoi, 3005), 0.005, 'unit=meter'),0) PROXIMITY,
      SG.STAGE_NME AS STAGE,
      --TT.ACTIVATION_CDE,
      TT.STATUS_NME AS STATUS,
      DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
      TS.EFFECTIVE_DAT AS EFFECTIVE_DATE,
      TY.TYPE_NME AS TENURE_TYPE,
      ST.SUBTYPE_NME AS TENURE_SUBTYPE,
      PU.PURPOSE_NME AS TENURE_PURPOSE,
      SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
      --DT.DOCUMENT_CHR,
      --DT.RECEIVED_DAT AS RECEIVED_DATE,
      --DT.ENTERED_DAT AS ENTERED_DATE,
      DT.COMMENCEMENT_DAT AS COMMENCEMENT_DATE,
      DT.EXPIRY_DAT AS EXPIRY_DATE,
      --IP.AREA_CALC_CDE,
      ROUND(IP.AREA_HA_NUM,2) AS AREA_HA,
      DT.LOCATION_DSC,
      --OU.UNIT_NAME,
      --IP.LEGAL_DSC,
      PR.INTERESTED_PARTY_SID AS INTERESTED_PARTY_ID,
      CONCAT(PR.LEGAL_NAME, PR.FIRST_NAME || ' ' || PR.LAST_NAME) AS CLIENT_NAME_PRIMARY,
      
      --ROUND(SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(SH.SHAPE, SDO_GEOMETRY(:wkb_aoi, 3005), 0.005),0.005, 'unit=HECTARE'),0) OVERLAP_HA,
      SH.SHAPE
      
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
    ON SH.INTRID_SID = IP.INTRID_SID) TN

WHERE 
    TN.STATUS = 'DISPOSITION IN GOOD STANDING' 
    AND SDO_WITHIN_DISTANCE (TN.SHAPE, 
                SDO_GEOMETRY(:wkb_aoi, 3005), 'distance=5000 unit=m') = 'TRUE'
 
ORDER BY TN.EFFECTIVE_DATE DESC
                        """

    return sql


def generate_report (workspace, df_list, sheet_list,filename):
    """ Exports dataframes to multi-tab excel spreasheet"""
    outfile= os.path.join(workspace, filename + '.xlsx')

    writer = pd.ExcelWriter(outfile,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]
        #workbook = writer.book
        worksheet.set_column(0, dataframe.shape[1], 25)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'sum'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()


def run_analysis():
    print ('Connecting to BCGW.')
    cnxinfo= get_db_cnxinfo(dbname='BCGW')
    hostname = cnxinfo['hostname']
    username = cnxinfo['username']
    password = cnxinfo['password']
    connection, cursor = connect_to_DB (username,password,hostname)
    
    wks= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240222_Clayoquot_Project_licenseeInfo'
    aoi= os.path.join(wks, 'inputs', 'Updated_Ahousaht_Conservancy_Jan17_2024_bcAlbers.shp')
    gdf= esri_to_gdf (aoi)
    gdf[['Name', 'geometry']]
    
    print ('\nReading TITAN data')
    titan= aoi= os.path.join(wks, 'inputs', 'TITAN_RPT012.xlsx')
    df_t= pd.read_excel(titan, 'TITAN_RPT012')
    df_t.drop(columns=['CLIENT NAME'], inplace= True)
    
    wkb_lib= get_wkb_srid(gdf)
    
    print('\nRun Queries.')
    sql= load_queries ()
    
    df_dict={}
    for k, v in wkb_lib.items():
        print (f'...working on {k}')
        cursor.setinputsizes(wkb_aoi=cx_Oracle.BLOB)
        bvars = {'wkb_aoi':v}
        df = read_query(connection,cursor,sql ['ah-csrv'],bvars)
        
        df.drop(columns=['SHAPE'], inplace= True)
        
        def modify_proximity(value):
            if value == 0:
                return 'OVERLAP'
            else:
                return f'WITHIN {value} m'
    
        df['PROXIMITY'] = df['PROXIMITY'].apply(modify_proximity)
        
        for col in df.columns:
            if 'DATE' in col:
                df[col] =  pd.to_datetime(df[col], infer_datetime_format=True, errors = 'coerce').dt.date
        
        df= pd.merge(df,df_t, how= 'left', 
                     left_on='DISPOSITION_TRANSACTION_ID',
                     right_on='DTID')
        
        df.drop(columns=['DTID'], inplace= True)
        
        df_dict[k]= df
    
    print('\nExport Report.') 
    outloc= os.path.join(wks, 'outputs')
    outfilename= '20240227_report active_land_tenures'  
    generate_report (outloc, df_dict.values(), df_dict.keys(),outfilename)    
    
run_analysis()   