#-------------------------------------------------------------------------------
# Name:        HTG Private Moorage Analysis
#
# Purpose:     This script generates a report on ASSIGNEMENTS completed between two dates within Nanwakolas consultation area.
#
# Input(s):    (1) Workspace (folder) where outputs will be generated.
#              (2) Titan Work Legder report (excel file) - TITAN_RPT010
#              (3) Start and Date dates
#              (4) BCGW connection parameters
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     29-06-2021
# Updated:
#-------------------------------------------------------------------------------

import os
import arcpy
import pandas as pd
import numpy as np
from datetime import datetime

#Hide pandas warning
pd.set_option('mode.chained_assignment', None)

def get_titan_report_date (titan_report):
    """ Returns the date of the input TITAN report"""
    df = pd.read_excel(titan_report,'Info')
    titan_date_raw = df.columns[1]
    titan_date_format = df.columns[1].strftime("%Y%m%d")

    return [titan_date_raw,titan_date_format]


def filter_TITAN (titan_report, start_date, end_date):
    """Returns a df of filtered TITAN report"""
    #Read TITAN report into df
    df = pd.read_excel(titan_report, 'TITAN_RPT010',
                       converters={'FILE #':str, 'FILE NUMBER':str})

    df = df.loc [((df['COMPLETED DATE'] >= start_date) &
                  (df['COMPLETED DATE'] <= end_date))]

    #Remove spaces from culomn names, remove special characters
    df.sort_values(by = ['COMPLETED DATE'], inplace = True)
    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
    df['COMPLETED DATE']=df['COMPLETED DATE'].astype(str) # need to convert to string for table creation
    df.rename(columns={'ORG. UNIT\n':'ORG_UNIT'}, inplace=True)
    df.columns = df.columns.str.replace(' ', '_')

    return df

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

def intersect_Nanwakolas (workspace, bcgw_conn_path, table):
    """Returns the spatial intersection tenures and NAN consulatation area"""
    tenure_layer = os.path.join(workspace, 'Temp_BCGW.sde', 'WHSE_TANTALIS.TA_CROWN_TENURES_SVW')
    tenureLayer = 'tenure_layer'
    arcpy.MakeFeatureLayer_management (tenure_layer, tenureLayer)
    arcpy.AddJoin_management(tenureLayer, 'CROWN_LANDS_FILE', table, 'FILE_NUMBER','KEEP_COMMON')

    assign_tenures = 'in_memory\_assign_tenures'
    arcpy.CopyFeatures_management(tenureLayer, assign_tenures)

    FN_layer = os.path.join(workspace, 'Temp_BCGW.sde', 'WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP')
    nanLayer = 'nan_layer'
    where_clause = """ CNSLTN_AREA_NAME = 'Nanwakolas' """
    arcpy.MakeFeatureLayer_management (FN_layer, nanLayer, where_clause)

    assign_tenures_nan= 'in_memory\_assign_tenures_nan'
    arcpy.SpatialJoin_analysis(assign_tenures, nanLayer, assign_tenures_nan,'', 'KEEP_COMMON')

    return assign_tenures_nan


def fc2df (fc, fields):
    """Returns a df based on a Feature Class"""
    arr = arcpy.da.FeatureClassToNumPyArray(
                in_table=fc,
                field_names=fields,
                skip_nulls=False,
                null_value=-99999)

    df = pd.DataFrame (arr)

    return df

def generate_report (workspace, df, start_date, end_date):
    """ Exports dataframes to multi-tab excel spreasheet"""
    drop_fields = ['FDISTRICT', 'DISTRICT_OFFICE','TASK_TYPE', 'USERID_ASSIGNED_TO', 'USERID_ASSIGNED_WORK_UNIT',
                   'OTHER_EMPLOYEES_ASSIGNED_TO', 'OTHER_EMPLOYEES_WORK_UNIT', 'TENURE_STAGE', 'TENURE_STATUS', 'TYPE']

    df_short = df.drop(drop_fields, axis = 1)

    df_list = [df_short, df]
    sheet_list = ["List", "List - more details"]
    strtdate_format = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d")
    enddate_format = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d")
    title = 'assignments_completed_' + strtdate_format+ '_' + enddate_format + '_Nanwakolas'
    file_name = os.path.join(workspace, title +'.xlsx')

    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe.drop_duplicates(subset = ['FILE_NUMBER'], keep = 'last', inplace = True)
        #dataframe.reset_index(drop=False, inplace = True)
        #dataframe.index = dataframe.index + 1
        #dataframe.index.names = ['COUNT']

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
    arcpy.env.overwriteOutput = True
    arcpy.env.qualifiedFieldNames = False
    workspace = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20210628_Nanwakolas_assignement\tests'
    titan_report = os.path.join(workspace, 'TITAN_RPT010.xlsx')

    titan_date = get_titan_report_date (titan_report)
    print 'Titan report date/time is: {}'.format (titan_date[0])

    print ("Filtering TITAN report...")
    start_date = '2018-04-01'
    end_date = '2021-04-01'
    df = filter_TITAN (titan_report,start_date,end_date)

    print ("Converting df to GBD table...")
    table = df2gdb (df)

    print ('Connecting to BCGW...PLease enter your credentials')
    bcgw_user_name = 'MLABIADH'
    bcgw_password = 'MoezLab8813'
    #bcgw_user_name = raw_input("Enter your BCGW username:")
    #bcgw_password = raw_input("Enter your BCGW password:")
    bcgw_conn_path = create_bcgw_connection(workspace, bcgw_user_name,bcgw_password)

    print ("Performing spatial overlay...")
    assign_tenures_nan = intersect_Nanwakolas (workspace, bcgw_conn_path, table)

    print ("Generating the report...")
    fields = ['ORG_UNIT', 'FDISTRICT', 'DISTRICT_OFFICE', 'FILE_NUMBER', 'TASK_TYPE',
              'TASK_DESCRIPTION', 'COMPLETED_DATE', 'USERID_ASSIGNED_TO',
              'USERID_ASSIGNED_WORK_UNIT', 'OTHER_EMPLOYEES_ASSIGNED_TO',
              'OTHER_EMPLOYEES_WORK_UNIT','TENURE_STAGE' , 'TENURE_STATUS', 'TYPE',
              'SUBTYPE', 'PURPOSE', 'SUBPURPOSE', 'LOCATION', 'CLIENT_NAME']

    df= fc2df (assign_tenures_nan,fields)

    generate_report (workspace, df, start_date, end_date)

    arcpy.Delete_management('in_memory')

    print ('Done!')

if __name__ == "__main__":
    main()