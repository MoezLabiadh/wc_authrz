#-------------------------------------------------------------------------------
# Name:        FN Replacements
#
# Purpose:     This script generates reports and spatial data for FN replacements.
#
# Input(s):    (1) Workspace (folder) where outputs will be generated.
#              (2) Titan report RPT012 (excel file) 
#                  must contain district office, 
#                  and client name and address columns
#              (3) Fiscal Year (e.g 2024)
#              (4) BCGW connection parameters
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     27-09-2023
# Updated:     
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import ast
import cx_Oracle
import pandas as pd
import fiona
import geopandas as gpd
import timeit


def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("..successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


def create_dir (path, dir):
    """ Creates new folder and returns path"""
    try:
      os.makedirs(os.path.join(path,dir))

    except OSError:
        print('Folder {} already exists!'.format(dir))
        pass

    return os.path.join(path,dir)

 
def load_queries ():
    """ Return the SQL queries that will be executed"""
    sql = {}
    sql['tn'] = """ 
                SELECT
                      DS.FILE_CHR AS FILE_NBR,
                      CAST(IP.INTRID_SID AS NUMBER) AS PARCEL_ID,
                      CAST(DT.DISPOSITION_TRANSACTION_SID AS NUMBER) AS DISP_TRANS_ID,
                      pip.CNSLTN_AREA_NAME,
                      SG.STAGE_NME AS STAGE,
                      TT.STATUS_NME AS STATUS,
                      DT.APPLICATION_TYPE_CDE AS APPLICATION_TYPE,
                      TY.TYPE_NME AS TYPE,
                      ST.SUBTYPE_NME AS SUBTYPE,
                      PU.PURPOSE_NME AS PURPOSE,
                      SP.SUBPURPOSE_NME AS SUBPURPOSE,
                      DT.COMMENCEMENT_DAT AS COMMENCEMENT_DATE,
                      DT.EXPIRY_DAT AS EXPIRY_DATE,
                      (EXTRACT(YEAR FROM DT.EXPIRY_DAT) - EXTRACT(YEAR FROM DT.COMMENCEMENT_DAT)) AS TENURE_LENGTH_YRS,
                      ROUND(IP.AREA_HA_NUM,2) AS AREA_HA,
                      SDO_UTIL.TO_WKTGEOMETRY(SP.SHAPE) SHAPE
                      
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
                   
                   -- add FN 
                  JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
                    ON SDO_RELATE (pip.SHAPE, SP.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                
                WHERE OU.UNIT_NAME= 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
                  AND TT.STATUS_NME= 'DISPOSITION IN GOOD STANDING'
                  AND DT.EXPIRY_DAT BETWEEN TO_DATE('01/04/{yr}', 'DD/MM/YYYY') AND TO_DATE('31/03/{nxyr}', 'DD/MM/YYYY')
                
                ORDER BY DT.EXPIRY_DAT ASC
                """
                
    sql['fn']= """
    
                SELECT
                  pip.CNSLTN_AREA_NAME, 
                  pip.CONTACT_ORGANIZATION_NAME, 
                  pip.CONTACT_NAME, 
                  pip.ORGANIZATION_TYPE, 
                  pip.CONTACT_UPDATE_DATE, 
                  pip.CONTACT_TITLE, 
                  pip.CONTACT_ADDRESS, 
                  pip.CONTACT_CITY, 
                  pip.CONTACT_PROVINCE, 
                  pip.CONTACT_POSTAL_CODE,
                  pip.CONTACT_FAX_NUMBER, 
                  pip.CONTACT_PHONE_NUMBER, 
                  pip.CONTACT_EMAIL_ADDRESS
            
            FROM
              WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
            
            WHERE 
              pip.CNSLTN_AREA_NAME IN ({fns})
            
            ORDER BY 
              CNSLTN_AREA_NAME
               """
               
    return sql


def add_max_term (df, terms_file):
    """Adds the Maximum tenure term column to the main df"""
    df_terms = pd.read_excel(terms_file)
    df = pd.merge(df, df_terms,  how='left',
                     left_on=['PURPOSE', 'SUBPURPOSE', 'TYPE', 'SUBTYPE'],
                     right_on=['PURPOSE', 'SUBPURPOSE', 'TYPE', 'SUBTYPE'])

    return df


def add_titan_info (df, titan_report):
    """ Adds data fron TITAN RPT012 to the main df"""
    df_tt = pd.read_excel(titan_report, 'TITAN_RPT012', converters={'FILE #':str})
    
    df_tt.rename(columns={'FILE #':'FILE_NBR'}, inplace=True)
    df_tt.columns = df_tt.columns.str.replace(' ', '_')
    
    df = pd.merge(df, df_tt,  how='left',
                     left_on='DISP_TRANS_ID',
                     right_on='DTID')
    
    df.loc[df['PURPOSE'] == 'AQUACULTURE', 'DISTRICT_OFFICE'] = 'AQUACULTURE'
    df['DISTRICT_OFFICE'] = df['DISTRICT_OFFICE'].fillna(value='NANAIMO')
    
    df.drop(columns=['DTID'], inplace= True)
    
    return df


def summarize_data (df):
    """ Returns Summary of Number of Expiring Files per Consultation Area"""

    df['FILE_NBR'] = df['FILE_NBR'].astype(str)
    groups = df.groupby(['DISTRICT_OFFICE', 'CNSLTN_AREA_NAME'])['FILE_NBR'].apply(list)
    sum_nbr_files = pd.DataFrame(groups)

    sum_nbr_files ['Number of files'] = sum_nbr_files['FILE_NBR'].str.len()

    sum_nbr_files.reset_index(inplace = True)
    sum_nbr_files.index = sum_nbr_files.index + 1

    sum_nbr_files.rename(columns={'FILE_NBR':'List of files',
                        'CNSLTN_AREA_NAME':'Consultation Areas'}, inplace=True)

    cols = ['DISTRICT_OFFICE', 'Consultation Areas','Number of files', 'List of files']
    sum_nbr_files = sum_nbr_files[cols]

    for index, row in sum_nbr_files.iterrows():
        list_tenures = ast.literal_eval(str(row['List of files']))
        str_tenures = ', '.join(x for x in list_tenures)
        sum_nbr_files.loc[index, 'List of files'] = str_tenures

    return sum_nbr_files


def create_fn_contact_list (df, connection, sql):
    """ Returns a df containing FN contact information"""
    fn_lst= list(set(df['CNSLTN_AREA_NAME'].to_list()))
    
    fn_lst= [s.replace("'", "''") for s in fn_lst]
    
    fn_lst_str= ",".join("'" + str(x) + "'" for x in fn_lst)
    
    fn_q= sql['fn'].format(fns= fn_lst_str)
    
    df_fn_con= pd.read_sql(fn_q, connection)
    
    for col in df_fn_con.columns:
        if 'DATE' in col:
            df[col] =  pd.to_datetime(df_fn_con[col], infer_datetime_format=True, 
                                      errors = 'coerce').dt.date


    return df_fn_con
    

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
    

def df_2_gdf (df, crs):
    """ Return a geopandas gdf based on a df with Geometry column"""
    df['SHAPE'] = df['SHAPE'].astype(str)
    df['geometry'] = gpd.GeoSeries.from_wkt(df['SHAPE'])
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    #df['geometry'] = df['SHAPE'].apply(wkt.loads)
    #gdf = gpd.GeoDataFrame(df, geometry = df['geometry'])
    gdf.crs = "EPSG:" + str(crs)
    #del df['SHAPE']
    del gdf['SHAPE']

    return gdf


def export_shapes (df,gdf,fiscal,spatial_path):
    """Exports  KML and SHP replacement files for each FN"""
    office_list = list (set(df['DISTRICT_OFFICE'].tolist()))
    for office in office_list:
        print ('\nExporting Spatial Files for {} office'.format (office))
        office_path = create_dir (spatial_path, office)
        df_office = df.loc[df['DISTRICT_OFFICE'] == office]

        counter = 1
        for index, row in df_office.iterrows():
            fn_name = str(row['Consultation Areas'])
            print ('..Consultation area {} of {}: {}'.format(counter,df_office.shape[0],fn_name))
            list_ten = str(row['List of files']).split(",")
            
            list_strip = [x.strip() for x in list_ten]
            #str_tenures = ",".join("'" + x + "'" for x in list_strip)

            gdf_ext = gdf.loc[(gdf['FILE_NBR'].isin(list_strip)) &
                              (gdf['CNSLTN_AREA_NAME'] == fn_name)]

            export_name = fn_name + '_rep_FY' + str(fiscal)
            change_str = ["'", ' ',"-","/", "(", ")","Final_Agreement_Areas"]
            for c in change_str:
                if c in export_name:
                    if c == "Final_Agreement_Areas":
                        export_name = export_name.replace(c,"FAA")
                    elif c == "'":
                        export_name = export_name.replace(c,"")
                    else:
                        export_name = export_name.replace(c,"_")
                else:
                    pass

            export_name = export_name.replace('__','_')
            fn_path = create_dir (office_path, export_name)
            txt_file = os.path.join(fn_path, 'tenures_list.txt')
            if os.path.isfile(txt_file) == False:
                with open(txt_file, 'w') as f:
                   f.write(export_name + '\n')
                   for file in list_strip:
                      f.write('{}\n'.format(str(file)))
            else:
                pass

            print ('...exporting KML')
            kml_path = create_dir (fn_path, 'KML')
            kml_name = os.path.join(kml_path,export_name+'.kml')
            if not os.path.isfile(kml_name):
                fiona.supported_drivers['KML'] = 'rw'
                gdf_ext.to_file(kml_name, driver='KML')
            else:
                print ('KML already exists!')
                pass

            print ('...exporting SHP')
            shp_path = create_dir (fn_path, 'SHP')
            shp_name = os.path.join(shp_path,export_name+'.shp')
            if not os.path.isfile(shp_name):
                gdf_ext.to_file(shp_name, driver="ESRI Shapefile")

            else:
                print ('SHP already exists!')
                pass

            for root, dirs, files in os.walk(kml_path):
                for file in files:
                    if file.endswith(".metadata.xml"):
                     os.remove(os.path.join(root,file))

            counter += 1
            
            
def main():
    start_t = timeit.default_timer() #start time
    
    workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20230927_fn_replacements_2024_25\sept27'
    terms_file = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\DATASETS\Tenure_Terms\max_tenure_terms.xlsx'
    
    fiscal= 2024
    titan_report = os.path.join(workspace, 'TITAN_RPT012.xlsx')
    
    print ('\nConnecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    sql = load_queries ()
    
    print ("\nRunning the SQL query")
    query= sql['tn'].format(yr= fiscal, nxyr= fiscal+1)
    df_geo= pd.read_sql(query, connection)
    
    df= df_geo.drop(columns=['SHAPE'])
    
    print ("\nProcessing query results...")
    #add max term to df
    df= add_max_term (df, terms_file)
    
    #caluclating total areas (files with multiple parcels)
    df_areas = df.drop_duplicates(subset=['FILE_NBR', 'PARCEL_ID'])
    df_areas = df_areas.groupby('FILE_NBR')['AREA_HA'].sum().reset_index()
    df_areas.rename(columns={'AREA_HA': 'TOTAL_AREA_HA'}, inplace=True)
    df= pd.merge(df, df_areas,how= 'left', on= 'FILE_NBR')
    
    #add info from titan report
    df= add_titan_info (df, titan_report)
    
    df.drop(columns=['PARCEL_ID','AREA_HA','DISP_TRANS_ID'], inplace= True)
    
    #column cleanup
    cols = list(df.columns)
    cols.insert(0, cols.pop(cols.index('DISTRICT_OFFICE')))
    df = df.reindex(columns=cols)
    
    df.drop_duplicates(subset=['FILE_NBR', 'CNSLTN_AREA_NAME'], inplace=True)
        
    for col in df.columns:
        if 'DATE' in col:
            df[col] =  pd.to_datetime(df[col], infer_datetime_format=True, 
                                      errors = 'coerce').dt.date
    
    #create a master list of expiriing tenures
    df_tn= df.drop_duplicates(subset=['FILE_NBR'])
    df_tn.drop(columns=['CNSLTN_AREA_NAME'], inplace= True)
    
    print ("\nGenerating a summary table...")
    #create a summary per file nbr and FN area
    df_sum= summarize_data (df)
    
    print ("\nGenerating the FN Contact List...")
    df_fn_con= create_fn_contact_list (df, connection, sql)
    
    print ("\nExporting Results...")
    out_path = create_dir (workspace, 'OUTPUTS')
    spatial_path = create_dir (out_path, 'SPATAL')
    excel_path = create_dir (out_path, 'SPREADSHEET')
    
    #export spreadsheet
    dfs = [df_tn, df, df_sum, df_fn_con]
    sheets=['Master List- Expring Tenures', 'Master List- FN overlap',
            'Summary- Files per office & FN', 'FN Contact info']
    filename = f'FN_replacements_Fiscal{fiscal}'
    
    generate_report (excel_path, dfs, sheets, filename)
    
    #export spatial files
    gdf = df_2_gdf (df_geo, 3005)
    gdf['EXPIRY_DATE'] = gdf['EXPIRY_DATE'].astype(str)
    gdf['COMMENCEMENT_DATE'] = gdf['COMMENCEMENT_DATE'].astype(str)
    
    export_shapes (df_sum,gdf,fiscal,spatial_path)
    
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print ('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs)) 
    
    
main()
