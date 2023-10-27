import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkb


def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        cursor = connection.cursor()
        print  ("...Successffuly connected to the database")
    except:
        raise Exception('...Connection failed! Please verifiy your login parameters')

    return connection, cursor


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



def get_wkb(gdf):
    """Returns WKB object from gdf"""
    
    geom = gdf['geometry'].iloc[0]
    
    # Check if the geometry is a MultiPolygon
    if geom.geom_type == 'MultiPolygon':
        wkb_aoi = wkb.dumps(geom, output_dimension=2)
    else:
        wkb_aoi = geom.wkb

    return wkb_aoi


def read_query(connection,cursor,query,bvars):
    "Returns a df containing SQL Query results"
    cursor.execute(query, bvars)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
    
    return df  



def load_queries():
    """ Return the SQL queries that will be executed"""
    sql= {}
    
    sql['new']= """
            SELECT* FROM(
            SELECT
                  --CAST(IP.INTRID_SID AS NUMBER) INTEREST_PARCEL_ID,
                  --CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_ID,
                  DS.FILE_CHR AS FILE_NBR,
                  SG.STAGE_NME AS STAGE,
                  TT.STATUS_NME AS STATUS,
                  DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
                  TS.EFFECTIVE_DAT AS EFFECTIVE_DATE,
                  TY.TYPE_NME AS TENURE_TYPE,
                  ST.SUBTYPE_NME AS TENURE_SUBTYPE,
                  PU.PURPOSE_NME AS TENURE_PURPOSE,
                  SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
                  DT.LOCATION_DSC,
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
                ON SH.INTRID_SID = IP.INTRID_SID
                
              JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP FN
                ON SDO_RELATE (SH.SHAPE, FN.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                  AND FN.CNSLTN_AREA_NAME = q'[Hul'qumi'num Nations - Core Territory]'
                  AND FN.CONTACT_NAME = 'Cowichan Tribes' ) TN
            
            WHERE TN.STAGE = 'APPLICATION'
              AND TN.STATUS = 'ACCEPTED'
              AND TN.APPLICATION_TYPE = 'NEW' 
              AND TN.TENURE_SUBPURPOSE = 'PRIVATE MOORAGE'
              AND SDO_RELATE (TN.SHAPE, SDO_GEOMETRY(:wkb_skw, 3005),'mask=ANYINTERACT') = 'TRUE'
             
            ORDER BY TN.EFFECTIVE_DATE DESC
    """
    
    sql['rep']= """
            SELECT* FROM(
            SELECT
                  --CAST(IP.INTRID_SID AS NUMBER) INTEREST_PARCEL_ID,
                  --CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) DISPOSITION_TRANSACTION_ID,
                  DS.FILE_CHR AS FILE_NBR,
                  SG.STAGE_NME AS STAGE,
                  TT.STATUS_NME AS STATUS,
                  DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
                  TS.EFFECTIVE_DAT AS EFFECTIVE_DATE,
                  TY.TYPE_NME AS TENURE_TYPE,
                  ST.SUBTYPE_NME AS TENURE_SUBTYPE,
                  PU.PURPOSE_NME AS TENURE_PURPOSE,
                  SP.SUBPURPOSE_NME AS TENURE_SUBPURPOSE,
                  DT.LOCATION_DSC,
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
                ON SH.INTRID_SID = IP.INTRID_SID
                
              JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP FN
                ON SDO_RELATE (SH.SHAPE, FN.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                  AND FN.CNSLTN_AREA_NAME = q'[Hul'qumi'num Nations - Core Territory]'
                  AND FN.CONTACT_NAME = 'Cowichan Tribes') TN
            
            WHERE TN.STAGE = 'APPLICATION'
              AND TN.STATUS = 'ACCEPTED'
              AND TN.APPLICATION_TYPE = 'REP' 
              AND TN.TENURE_SUBPURPOSE = 'PRIVATE MOORAGE'
            
              AND TN.FILE_NBR NOT IN (SELECT CROWN_LANDS_FILE 
                                     FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW
                                     WHERE TENURE_STATUS = 'DISPOSITION IN GOOD STANDING')
              
              AND SDO_RELATE (TN.SHAPE, SDO_GEOMETRY(:wkb_skw, 3005),'mask=ANYINTERACT') = 'TRUE' 
                      
            ORDER BY TN.EFFECTIVE_DATE DESC
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
    connection, cursor = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print("\nReading SRKW dataset...")
    shp_kw= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\FCBC_VISR\Regional_Datasets\DFO\DFO_SARA_CritHab_2022_KillerWhale.shp'
    gdf_kw= esri_to_gdf (shp_kw)
    
    gdf_skw= gdf_kw.loc[gdf_kw['Population']=='Northeast Pacific Southern Resident']
    
    wkb_skw= get_wkb (gdf_skw)
    
    print ("\nRunning SQL queries...")
    sql = load_queries ()
    
    cursor.setinputsizes(wkb_skw=cx_Oracle.BLOB)
    
    bvars = {'wkb_skw': wkb_skw}
    
    df_new = read_query(connection, cursor, sql['new'], bvars)
    df_new.drop('SHAPE', axis=1, inplace= True)
    df_new['EFFECTIVE_DATE'] =  pd.to_datetime(df_new['EFFECTIVE_DATE'], infer_datetime_format=True, errors = 'coerce').dt.date
    
    cursor.setinputsizes(wkb_skw=cx_Oracle.BLOB)
    
    bvars = {'wkb_skw': wkb_skw}
    
    df_rep = read_query(connection, cursor, sql['rep'], bvars)
    df_rep.drop('SHAPE', axis=1, inplace= True)
    df_rep['EFFECTIVE_DATE'] =  pd.to_datetime(df_rep['EFFECTIVE_DATE'], infer_datetime_format=True, errors = 'coerce').dt.date
    
    #l= ",".join("'"+ str(x)+ "'" for x in df_new['FILE_NBR'].to_list())

    
    print ("\nExporting Results...")
    workspace= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20231027_hmn_srkw_privateMoorage'
    generate_report (workspace, [df_new,df_rep], ['NEW Applics', 'REP Applics'],'20231027_privateMoorage_quwutsunCore_srkw')
