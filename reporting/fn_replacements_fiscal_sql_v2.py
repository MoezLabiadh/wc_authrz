#-------------------------------------------------------------------------------
# Name:        FN Replacements
#
# Purpose:     This script generates reports and spatial data for FN replacements.
#
# Input(s):    (1) Workspace (folder) where outputs will be generated.
#              (2) Titan report (excel file) - TITAN_RPT012
#              (3) Fiscal Year (e.g 2022)
#              (4) BCGW connection parameters
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     10-08-2022
# Updated:
#-------------------------------------------------------------------------------

import os
import ast
import cx_Oracle
import pandas as pd
import geopandas as gpd
import fiona
import numpy as np
import datetime
from shapely import wkt

#Hide pandas warning
pd.set_option('mode.chained_assignment', None)

def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database")
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


def get_titan_report_date (titan_report):
    """ Returns the date of the input TITAN report"""
    df = pd.read_excel(titan_report,'Info')
    titan_date = df.columns[1]

    return titan_date


def filter_TITAN (titan_report, fiscal):
    """Returns a df of filtered TITAN report"""
    #Read TITAN report into df
    df = pd.read_excel(titan_report, 'TITAN_RPT012',
                       converters={'FILE #':str})
    # Convert expiry date column to datetime format
    df['EXPIRY DATE'] =  pd.to_datetime(df['EXPIRY DATE'],
                                    infer_datetime_format=True,
                                    errors = 'coerce').dt.date

    # Filter the needed data: tenures expiring during fiscal year
    df = df.loc [(df['STAGE'] == 'TENURE') &
                 (df['EXPIRY DATE'] >= datetime.date(fiscal,4,1)) &
                 (df['EXPIRY DATE'] <= datetime.date(fiscal+1,3,31))]

    # Convert commencement date column to datetime format
    df['COMMENCEMENT DATE'] =  pd.to_datetime(df['COMMENCEMENT DATE'],
                                    infer_datetime_format=True,
                                    errors = 'coerce').dt.date
    #Calculate Tenure Length
    df ['diff'] = ((df['EXPIRY DATE'] - df['COMMENCEMENT DATE'] )\
                                  / np.timedelta64(1,'Y'))
    df['TENURE LENGTH'] = df['diff'].fillna(0).round().astype(int)


    #Remove spaces from column names, remove special characters
    df.sort_values(by = ['EXPIRY DATE'], inplace = True)

    df.loc[df['PURPOSE'] == 'AQUACULTURE', 'DISTRICT OFFICE'] = 'AQUACULTURE'
    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
    df.rename(columns={'FILE #':'FILE_NBR'}, inplace=True)
    df['TOTAL AREA'] = df['TOTAL AREA'].round(2)
    df.rename(columns={'TOTAL AREA':'AREA HA'}, inplace=True)
    df.columns = df.columns.str.replace(' ', '_')

    return df


def add_max_term (df, terms_file):
    """Add the Maximum tenure term column to the datataframe"""
    df_terms = pd.read_excel(terms_file)
    df_tn = pd.merge(df, df_terms,  how='left',
                     left_on=['PURPOSE', 'SUBPURPOSE', 'TYPE', 'SUBTYPE'],
                     right_on=['PURPOSE', 'SUBPURPOSE', 'TYPE', 'SUBTYPE'])

    return df_tn


def load_queries ():
    """ Return the SQL queries that will be executed"""
    sql = {}
    sql['fn'] = """ 
                SELECT  
                  ipr.CROWN_LANDS_FILE, pip.CNSLTN_AREA_NAME, pip.CONTACT_ORGANIZATION_NAME, pip.CONTACT_NAME, 
                  pip.ORGANIZATION_TYPE, pip.CONTACT_UPDATE_DATE, pip.CONTACT_TITLE, pip.CONTACT_ADDRESS, 
                  pip.CONTACT_CITY, pip.CONTACT_PROVINCE, pip.CONTACT_POSTAL_CODE,
                  pip.CONTACT_FAX_NUMBER, pip.CONTACT_PHONE_NUMBER, pip.CONTACT_EMAIL_ADDRESS,
                  SDO_UTIL.TO_WKTGEOMETRY(ipr.SHAPE) SHAPE
                
                FROM
                WHSE_TANTALIS.TA_CROWN_TENURES_SVW ipr
                    INNER JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip 
                        ON SDO_RELATE (pip.SHAPE, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                
                WHERE ipr.INTRID_SID IN ({t})
                """
    return sql


def read_query(connection,query):
    "Returns a df containing SQL Query results"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        names = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
                        
        return pd.DataFrame(rows, columns=names)
    
    finally:
        if cursor is not None:
            cursor.close()
            

def get_fn_overlaps (df,connection, sql):
    """Return a df containing Tenures overlapping with IHAs"""
    s_ten = ",".join(str(x) for x in df['INTEREST_PARCEL_ID'].astype(int).tolist())
    query = sql['fn'].format(t= s_ten)
    df_fn = read_query(connection,query)
    
    return df_fn


def df_2_gdf (df, crs):
    """ Return a geopandas gdf based on a df with Geometry column"""
    df['SHAPE'] = df['SHAPE'].astype(str)
    df['geometry'] = df['SHAPE'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry = df['geometry'])
    gdf.crs = "EPSG:" + str(crs)
    del df['SHAPE']
    
    return gdf
    

def summarize_data (df):
    """ Returns Summary of Number of Expiring Files per Consultation Area"""

    df['CROWN_LANDS_FILE'] = df['CROWN_LANDS_FILE'].astype(str)
    groups = df.groupby(['DISTRICT_OFFICE', 'CNSLTN_AREA_NAME'])['CROWN_LANDS_FILE'].apply(list)
    sum_nbr_files = pd.DataFrame(groups)

    sum_nbr_files ['Number of files'] = sum_nbr_files['CROWN_LANDS_FILE'].str.len()

    sum_nbr_files.reset_index(inplace = True)
    sum_nbr_files.index = sum_nbr_files.index + 1

    sum_nbr_files.rename(columns={'CROWN_LANDS_FILE':'List of files',
                        'CNSLTN_AREA_NAME':'Consultation Areas'}, inplace=True)

    cols = ['DISTRICT_OFFICE', 'Consultation Areas','Number of files', 'List of files']
    sum_nbr_files = sum_nbr_files[cols]

    for index, row in sum_nbr_files.iterrows():
        list_tenures = ast.literal_eval(str(row['List of files']))
        str_tenures = ', '.join(x for x in list_tenures)
        sum_nbr_files.loc[index, 'List of files'] = str_tenures

    return sum_nbr_files


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


def export_shapes (df,gdf,fiscal,spatial_path):
    """Exports  KML and SHP replacement files for each FN"""
    office_list = list (set(df['DISTRICT_OFFICE'].tolist()))
    for office in office_list:
        print ('Exporting Spatial Files for {} office'.format (office))
        office_path = create_dir (spatial_path, office)
        df_office = df.loc[df['DISTRICT_OFFICE'] == office]

        counter = 1
        for index, row in df_office.iterrows():
            fn_name = str(row['Consultation Areas'])
            print ('..Consultation area {} of {}: {}'.format(counter,df_office.shape[0],fn_name))
            list_ten = str(row['List of files']).split(",")
            
            list_strip = [x.strip() for x in list_ten]
            #str_tenures = ",".join("'" + x + "'" for x in list_strip)

            gdf_ext = gdf.loc[(gdf['CROWN_LANDS_FILE'].isin(list_strip)) &
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


def main ():
    """Runs the program"""
    
    workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20220809_fn_replacements_2023_24'
    terms_file = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\DATASETS\Tenure_Terms\max_tenure_terms.xlsx'
    titan_report = os.path.join(workspace, 'TITAN_RPT012.xlsx')
    
    titan_date = get_titan_report_date (titan_report)
    print ('Titan report date/time is: {}'.format (titan_date))
    
    print ("Filtering TITAN report...")
    fiscal = 2023
    df_f = filter_TITAN (titan_report,fiscal)
    
    print ('Connecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ("Adding Max term Column...")
    df_ten= add_max_term (df_f, terms_file)
    
    cols = ['DISTRICT_OFFICE','FDISTRICT','FILE_NBR', 'DTID', 'COMMENCEMENT_DATE',
          'EXPIRY_DATE', 'TENURE_LENGTH','MAX_TENURE_TERM','STAGE', 'STATUS', 'APPLICATION_TYPE',
          'TYPE', 'SUBTYPE', 'PURPOSE', 'SUBPURPOSE', 'AREA_HA','LOCATION',
          'CLIENT_NAME', 'ADDRESS_LINE_1', 'ADDRESS_LINE_2','ADDRESS_LINE_3','CITY', 'PROVINCE', 'POSTAL_CODE',
          'COUNTRY','STATE','ZIP_CODE', 'INTEREST_PARCEL_ID']
    
    df_ten = df_ten[cols]
    
    print ("Loading SQL queries...")
    sql = load_queries ()
    
    print ("SQL: Getting overlaps between Tenures and FN territories...")
    df_fn_geo = get_fn_overlaps (df_ten,connection, sql)
    
    df_fn_con = df_fn_geo.drop(columns=['CROWN_LANDS_FILE', 'SHAPE'])
    df_fn_con.drop_duplicates(subset=['CNSLTN_AREA_NAME', 'CONTACT_ORGANIZATION_NAME'], inplace=True)
    df_fn_con.sort_values(by = ['CNSLTN_AREA_NAME'], inplace = True)
    
    df_fn_geo = df_fn_geo[['CROWN_LANDS_FILE', 'CNSLTN_AREA_NAME', 'SHAPE']]
    df_fn_geo.drop_duplicates(subset=['CROWN_LANDS_FILE', 'CNSLTN_AREA_NAME'], inplace=True)
    
    df_fn_geo = pd.merge(df_fn_geo,df_ten, how= 'left', 
                         left_on='CROWN_LANDS_FILE', right_on= 'FILE_NBR')
    df_fn_att = df_fn_geo.loc[:, df_fn_geo.columns != 'SHAPE'] 
    
    print ("Generating a geo-dataframe...")
    gdf = df_2_gdf (df_fn_geo, 3005)
    gdf['EXPIRY_DATE'] = gdf['EXPIRY_DATE'].astype(str)
    gdf['COMMENCEMENT_DATE'] = gdf['COMMENCEMENT_DATE'].astype(str)
    
    print('Generating Summary Statistics...')
    sum_nbr_files = summarize_data(df_fn_att)
    
    print('Exporting Results...')
    out_path = create_dir (workspace, 'OUTPUTS')
    spatial_path = create_dir (out_path, 'SPATAL')
    excel_path = create_dir (out_path, 'SPREADSHEET')
    
    dfs = [df_ten, df_fn_att, sum_nbr_files, df_fn_con]
    sheets=['Master List- Expring Tenures', 'Master List- FN overlap',
            'Summary- Files per office & FN', 'FN Contact info']
    filename = 'FN_replacements_Fiscal2023'
    
    generate_report (excel_path, dfs, sheets, filename)
    export_shapes (sum_nbr_files,gdf,fiscal,spatial_path)
    
     
    print ('Processing completed! Check Output folder for results!')


main ()
