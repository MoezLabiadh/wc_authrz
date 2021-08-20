#-------------------------------------------------------------------------------
# Name:        FN Replacements
#
# Purpose:     This script generates reports and spatial data for FN replacements.
#
# Input(s):    (1) Workspace (folder) where outputs will be generated.
#              (2) Titan report (excel file) - TITAN_RPT012
#              (3) Fiscal Year (e.g 2022)
#              (4) BCGW connection parameters - will be prompt during
#                  script execution
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     20-07-2021
# Updated:
#-------------------------------------------------------------------------------

import os
import ast
import arcpy
import pandas as pd
import numpy as np
import datetime

#Hide pandas warning
pd.set_option('mode.chained_assignment', None)

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
    df['EXPIRY DATE'] = df['EXPIRY DATE'].astype(str)
    df['COMMENCEMENT DATE'] = df['COMMENCEMENT DATE'].astype(str)
    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
    df.rename(columns={'FILE #':'FILE_NBR'}, inplace=True)
    df.columns = df.columns.str.replace(' ', '_')

    return df

def add_max_term (df, terms_file):
    """Add the Maximum tenure term column to the datataframe"""
    df_terms = pd.read_excel(terms_file)
    df_tn = pd.merge(df, df_terms,  how='left',
                     left_on=['PURPOSE', 'SUBPURPOSE', 'TYPE', 'SUBTYPE'],
                     right_on=['PURPOSE', 'SUBPURPOSE', 'TYPE', 'SUBTYPE'])

    return df_tn

def df2gdb (df):
    """Converts a pandas df to a gbd table"""

    #Turn dataframe into a simple np series
    arr = np.array(np.rec.fromrecords(df.values))

    #Create a list of field names from the dataframe
    colnames = [name.encode('UTF8') for name in df.dtypes.index.tolist()]

    #Update column names in structured array
    arr.dtype.names = tuple(colnames)
    #print (arr.dtype)

    #Create the GDB table
    table = 'in_memory\df_table'
    arcpy.da.NumPyArrayToTable(arr, table)

    return table


def create_bcgw_connection(workspace, bcgw_user_name,bcgw_password):
    """Returns a BCGW connnection file"""

    name = 'Temp_BCGW'
    database_platform = 'ORACLE'
    account_authorization  = 'DATABASE_AUTH'
    instance = 'bcgw.bcgov/idwprod1.bcgov'
    username = bcgw_user_name
    password = bcgw_password
    bcgw_conn_path = os.path.join(workspace,'Temp_BCGW.sde')
    if not arcpy.Exists(bcgw_conn_path):
        arcpy.CreateDatabaseConnection_management (workspace,name, database_platform,instance,account_authorization,
                                               username ,password, 'DO_NOT_SAVE_USERNAME')
    else:
        pass

    return bcgw_conn_path


def get_layers (table, bcgw_conn_path, req_fields):
    """Returns layers of FN consultation areas and Expiring tenures"""
    tenure_layer = os.path.join(bcgw_conn_path, 'WHSE_TANTALIS.TA_CROWN_TENURES_SVW')
    tenureLayer = 'tenure_layer'
    arcpy.MakeFeatureLayer_management (tenure_layer, tenureLayer)
    arcpy.AddJoin_management(tenureLayer, 'DISPOSITION_TRANSACTION_SID', table, 'DTID','KEEP_COMMON')

    join_tenures = 'in_memory\_join_tenures'
    arcpy.CopyFeatures_management(tenureLayer, join_tenures)

    expiring_tenures = 'in_memory\_expiring_tenures'
    arcpy.Dissolve_management (join_tenures, expiring_tenures, req_fields,[['TENURE_AREA_IN_HECTARES', "SUM"]])

    FN_layer = os.path.join(bcgw_conn_path, 'WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP')
    consult_layer = 'consult_layer'
    arcpy.MakeFeatureLayer_management(FN_layer, consult_layer)
    arcpy.SelectLayerByLocation_management(consult_layer, 'intersect', expiring_tenures)

    consult_areas = 'in_memory\_consult_areas'
    arcpy.Dissolve_management (consult_layer, consult_areas, 'CNSLTN_AREA_NAME')

    return expiring_tenures, consult_layer, consult_areas


def intersect_FN (expiring_tenures,consult_areas):
    """Returns intersection layer of FN consultation areas and Epiring Tenures"""
    intersect = 'in_memory\_intersect'
    arcpy.SpatialJoin_analysis(expiring_tenures, consult_areas, intersect, 'JOIN_ONE_TO_MANY')

    return intersect

def fc2df (fc, fields):
    """Returns a df based on a Feature Class"""
    arr = arcpy.da.FeatureClassToNumPyArray(
                in_table=fc,
                field_names=fields,
                skip_nulls=False,
                null_value=-99999)

    df = pd.DataFrame (arr)

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


def generate_report (workspace, df_list, fiscal):
    """ Exports dataframes to multi-tab excel spreasheet"""
    sheet_list = ["Master List- Expring Tenures", "Master List- FN overlap",
                  "Summary- Files per office & FN", "FN Contact info"]
    file_name = os.path.join(workspace, 'FN_replacements_Fiscal' + str(fiscal) +'.xlsx')

    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]
        workbook = writer.book

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        if 'Master List' in sheet:
            col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'count'})
        else:
            col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'sum'})

        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()


def export_shapes (df,fc,fiscal,spatial_path):
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
            str_tenures = ",".join("'" + x + "'" for x in list_strip)

            where_clause = """ FILE_NBR in ({}) """.format (str_tenures)
            arcpy.MakeFeatureLayer_management (fc, "tenures_lyr")
            arcpy.SelectLayerByAttribute_management ("tenures_lyr", "NEW_SELECTION", where_clause)

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
            kml_name = os.path.join(kml_path,export_name+'.kmz')
            if not os.path.isfile(kml_name):
                arcpy.LayerToKML_conversion("tenures_lyr", kml_name)
            else:
                print ('KML already exists!')
                pass

            print ('...exporting SHP')
            shp_path = create_dir (fn_path, 'SHP')
            shp_name = os.path.join(shp_path,export_name+'.shp')
            if not os.path.isfile(shp_name):
                arcpy.CopyFeatures_management("tenures_lyr", os.path.join(shp_path,export_name))
            else:
                print ('SHP already exists!')
                pass

            for root, dirs, files in os.walk(kml_path):
                for file in files:
                    if file.endswith(".metadata.xml"):
                     os.remove(os.path.join(root,file))

            arcpy.Delete_management("tenures_lyr")
            counter += 1


def main():
    """Runs the program"""
    arcpy.env.overwriteOutput = True
    arcpy.env.qualifiedFieldNames = False
    workspace = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20210707_FN_replacements_2022_23'
    titan_report = os.path.join(workspace, 'TITAN_RPT012.xlsx')

    titan_date = get_titan_report_date (titan_report)
    print 'Titan report date/time is: {}'.format (titan_date)

    print ("Filtering TITAN report...")
    fiscal = 2022
    df_filter = filter_TITAN (titan_report,fiscal)

    print ("Adding Max term Column...")
    terms_file = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\DATASETS\Tenure_Terms\max_tenure_terms.xlsx'
    req_fields = ['DISTRICT_OFFICE','FDISTRICT','FILE_NBR', 'DTID', 'COMMENCEMENT_DATE',
          'EXPIRY_DATE', 'TENURE_LENGTH','MAX_TENURE_TERM','STAGE', 'STATUS', 'APPLICATION_TYPE',
          'TYPE', 'SUBTYPE', 'PURPOSE', 'SUBPURPOSE','LOCATION',
          'CLIENT_NAME', 'ADDRESS_LINE_1', 'ADDRESS_LINE_2','ADDRESS_LINE_3','CITY', 'PROVINCE', 'POSTAL_CODE',
          'COUNTRY','STATE','ZIP_CODE']

    df_ten= add_max_term (df_filter, terms_file)
    df_ten = df_ten[req_fields]

    print ("Converting df to GBD table...")
    table = df2gdb (df_ten)

    print ('Connecting to BCGW...PLease enter your credentials')
    bcgw_user_name = raw_input("Enter your BCGW username:")
    bcgw_password = raw_input("Enter your BCGW password:")
    bcgw_conn_path = create_bcgw_connection(workspace, bcgw_user_name,bcgw_password)

    print ('Creating Layers: Consultation areas and Expring Tenures')
    expiring_tenures, consult_layer, consult_areas = get_layers (table, bcgw_conn_path, req_fields)

    print('Intersecting Expiring tenures and FN areas...')
    intersect = intersect_FN (expiring_tenures,consult_areas)

    print('Exporting result to dataframe...')

    req_fields.insert(15,'SUM_TENURE_AREA_IN_HECTARES')
    df_exp = fc2df (expiring_tenures, req_fields)
    df_exp.rename(columns={'SUM_TENURE_AREA_IN_HECTARES': 'AREA_HA'}, inplace=True)

    fields_inter = req_fields[:]
    fields_inter.insert(4,'CNSLTN_AREA_NAME')
    df_inter = fc2df (intersect, fields_inter)
    df_inter.rename(columns={'SUM_TENURE_AREA_IN_HECTARES': 'AREA_HA'}, inplace=True)


    # FN contact info
    fields = ['CNSLTN_AREA_NAME','CONTACT_ORGANIZATION_NAME','CONTACT_NAME','ORGANIZATION_TYPE','CONTACT_UPDATE_DATE',
              'CONTACT_TITLE', 'CONTACT_ADDRESS','CONTACT_CITY','CONTACT_PROVINCE','CONTACT_POSTAL_CODE',
              'CONTACT_FAX_NUMBER','CONTACT_PHONE_NUMBER','CONTACT_EMAIL_ADDRESS']
    df_fn = fc2df (consult_layer, fields)
    df_fn.sort_values(by=[fields[0],fields[1]], inplace = True)

    print('Creating Summary Statistics...')
    sum_nbr_files = summarize_data(df_inter)

    out_path = create_dir (workspace, 'OUTPUTS')
    spatial_path = create_dir (out_path, 'SPATAL')
    excel_path = create_dir (out_path, 'SPREADSHEET')


    print('Exporting Results...')
    generate_report (excel_path, [df_exp, df_inter,sum_nbr_files,df_fn], fiscal)
    export_shapes (sum_nbr_files,expiring_tenures,fiscal,spatial_path)

    arcpy.Delete_management('in_memory')

    print ('Processing completed! Check Output folder for results!')


if __name__ == "__main__":
    main()
