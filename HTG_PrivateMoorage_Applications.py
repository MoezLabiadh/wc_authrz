#-------------------------------------------------------------------------------
# Name:        HTG Private Moorage Analysis
#
# Purpose:     This script generates a report on new Privates Moorage
#              applications within Hul'qumi'num Treaty Group (HTG) area.
#
# Input(s):    (1) Workspace (folder) where outputs will be generated.
#              (2) Titan report (excel file ) containing the following mandatory
#                   fields: FILE #, SUBPURPOSE, STATUS, DISTRICT OFFICE, APPLICATION TYPE
#                   other fields may be added to the report as required.
#              (3) BCGW connection parameters
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     15-06-2021
# Updated:
#-------------------------------------------------------------------------------

import os
import arcpy
import pandas as pd
import numpy as np
from datetime import date

#Hide pandas warning
pd.set_option('mode.chained_assignment', None)

def get_titan_report_date (titan_report):
    """ Returns the date of the input TITAN report"""
    df = pd.read_excel(titan_report,'Info')
    titan_date_raw = df.columns[1]
    titan_date_format = df.columns[1].strftime("%Y%m%d")

    return [titan_date_raw,titan_date_format]


def filter_TITAN (titan_report):
    """Returns a df of filtered TITAN report"""
    #Read TITAN report into df
    df = pd.read_excel(titan_report, 'TITAN_RPT012',
                       converters={'FILE #':str, 'RECEIVED DATE':str})

    #Filter PM applications
    df_pm_app = df.loc[(df['SUBPURPOSE'] == 'PRIVATE MOORAGE') &
                       (df['STATUS'] == 'ACCEPTED')]

    #Fill nan for district office
    df_pm_app.loc[df['DISTRICT OFFICE'].isnull(), 'DISTRICT OFFICE'] = 'NANAIMO'

    #Set PRE RNWL to NEW
    df_pm_app.loc[df['APPLICATION TYPE'] == 'PRE RNWL', 'APPLICATION TYPE'] = 'NEW'

    #Remove spaces from culomn names, remove special characters
    df_pm_app.rename(columns={'FILE #':'FILE_NBR'}, inplace=True)
    df_pm_app.columns = df_pm_app.columns.str.replace(' ', '_')

    #Reset index
    df_pm_app = df_pm_app.reset_index(drop=True)
    df_pm_app.index = df_pm_app.index + 1
    df_pm_app.index.names = ['Index']

    return df_pm_app

def df2gdb (df):
    """Converts a pandas df to a gbd table"""

    #Turn dataframe into a simple np series
    arr = np.array(np.rec.fromrecords(df.values))

    #Create a list of field names from the dataframe
    colnames = [name.encode('UTF8') for name in df.dtypes.index.tolist()]

    #Update column names in structured array
    arr.dtype.names = tuple(colnames)

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

def intersect_HTG (bcgw_conn_path, table):
    """Returns the spatial intersection of PM tenures and HTG consulatation areas"""
    tenure_layer = os.path.join(bcgw_conn_path, 'WHSE_TANTALIS.TA_CROWN_TENURES_SVW')
    tenureLayer = 'tenure_layer'
    arcpy.MakeFeatureLayer_management (tenure_layer, tenureLayer)
    arcpy.AddJoin_management(tenureLayer, 'DISPOSITION_TRANSACTION_SID', table, 'DTID','KEEP_COMMON')
    pm_tenures = 'in_memory\pm_tenures'
    arcpy.CopyFeatures_management(tenureLayer, pm_tenures)

    FN_layer = os.path.join(bcgw_conn_path, 'WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP')
    htgLayer = 'htg_layer'
    where_clause = """ CONTACT_NAME in ('Halalt First Nation', 'Lake Cowichan First Nation')
                       AND CONTACT_ORGANIZATION_NAME = 'Halalt First Nation'"""
    arcpy.MakeFeatureLayer_management (FN_layer, htgLayer, where_clause)
    pm_tenures_htg = 'in_memory\pm_tenures_htg'
    arcpy.SpatialJoin_analysis(pm_tenures, htgLayer, pm_tenures_htg)

    return pm_tenures_htg


def fc2df (fc, fields):
    """Returns a df based on a Feature Class"""
    arr = arcpy.da.FeatureClassToNumPyArray(
                in_table=fc,
                field_names=fields,
                skip_nulls=False,
                null_value=-99999)

    htg_df = pd.DataFrame (arr)

    return htg_df

def generate_report (workspace, df, titan_date):
    """ Exports dataframes to multi-tab excel spreasheet"""
    df.rename(columns={"CNSLTN_AREA_NAME": "HTG"}, inplace = True)
    df.loc[df['HTG'] == "Hul'qumi'num Nations - Marine Territory", 'HTG'] = 'MARINE'
    df.loc[df['HTG'] == "Hul'qumi'num Nations - Core Territory", 'HTG'] = 'CORE'
    df.loc[df['HTG'] == "-99999", 'HTG'] = 'OUTSIDE'

    df.drop_duplicates(subset = ['FILE_NBR'], keep = 'last', inplace = True)

    df_summary = pd.pivot_table(df, values='FILE_NBR', index=['HTG','DISTRICT_OFFICE','APPLICATION_TYPE'],
                                aggfunc='count', fill_value=0).reset_index().rename_axis(None, axis=1)
    df_summary.rename(columns={"FILE_NBR": "Number of files", "TYPE": "TENURE TYPE"}, inplace = True)

    df_list = [df, df_summary]
    sheet_list = ["List", "Summary"]
    file_name = os.path.join(workspace, 'HTG_PM_Applications_asof_' + titan_date[1] +'.xlsx')

    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1
        try:
            dataframe.drop('INTEREST_PARCEL_ID', axis=1, inplace=True)
        except:
            pass

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]
        workbook = writer.book

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        if sheet == 'List':
            col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'count'})
        else:
            col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'sum'})

        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()

def main():
    arcpy.env.overwriteOutput = True
    arcpy.env.qualifiedFieldNames = False
    workspace = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20210603_HMN-HTG_private_moorage'
    titan_report = os.path.join(workspace, 'TITAN_RPT012.xlsx')

    titan_date = get_titan_report_date (titan_report)
    print 'Titan report date/time is: {}'.format (titan_date[0])

    print ("Filtering TITAN report...")
    df_pm_app = filter_TITAN (titan_report)

    print ("Converting df to GBD table...")
    table = df2gdb (df_pm_app)

    print ('Connecting to BCGW...PLease enter your credentials')
    bcgw_user_name = raw_input("Enter your BCGW username:")
    bcgw_password = raw_input("Enter your BCGW password:")
    bcgw_conn_path = create_bcgw_connection(workspace, bcgw_user_name,bcgw_password)

    print ("Performing spatial overlay...")
    pm_tenures_htg = intersect_HTG (bcgw_conn_path, table)

    print ("Generating the report...")
    fields = ['DISTRICT_OFFICE', 'FILE_NBR', 'DTID', 'CLIENT_NAME', 'STATUS', 'APPLICATION_TYPE',
              'LOCATION', 'TYPE', 'SUBTYPE', 'PURPOSE', 'SUBPURPOSE', 'RECEIVED_DATE', 'CNSLTN_AREA_NAME']

    df_htg = fc2df (pm_tenures_htg,fields)


    generate_report (workspace, df_htg, titan_date)

    arcpy.Delete_management('in_memory')

    print ('Done!')

if __name__ == "__main__":
    main()
