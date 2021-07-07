#-------------------------------------------------------------------------------
# Name:        Office Zone Proximity Analysis
#
# Purpose:     This script generates a report the proximity of Tenure files
#              (Disposition in Good, new and expired) to each Office Zone.
#
# Input(s):    (1) Workspace (folder) where outputs will be generated.
#              (2) Titan report (excel file ). The script checks if all required
#                  columns are available in TITAN report
#              (3) BCGW connection parameters
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     24-06-2021
# Updated:
#-------------------------------------------------------------------------------


import os
import arcpy
import pandas as pd
import numpy as np
import xlsxwriter
from datetime import date

#Hide pandas warning
pd.set_option('mode.chained_assignment', None)


def check_TITAN_cols (titan_report, req_cols):
    """Checks if required columns exist in TITAN report"""
    df = pd.read_excel (titan_report,'TITAN_RPT012')

    for col in req_cols:
        if col not in df.columns:
            raise Exception ('{} column is missing from the TITAN report.'.format (col))
        else:
            pass

    print ('TITAN report contains all required columns.')


def get_titan_report_date (titan_report):
    """ Returns the date of the input TITAN report"""
    df = pd.read_excel(titan_report,'Info')
    titan_date_raw = df.columns[1]
    titan_date_format = df.columns[1].strftime("%Y%m%d")

    return [titan_date_raw,titan_date_format]


def filter_data (titan_report):
    """Returns filtered dataframes"""
    # read TITAN report into dataframe
    df_titan = pd.read_excel (titan_report,'TITAN_RPT012',
                              converters={'FILE #':str, 'RECEIVED DATE':str,
                                          'EXPIRY DATE':str})

    # fill nan values for district office
    df_titan['DISTRICT OFFICE'] = df_titan['DISTRICT OFFICE'].fillna(value='NANAIMO')

    #Remove spaces from culomn names, remove special characters
    df_titan.rename(columns={'FILE #':'FILE_NBR'}, inplace=True)
    df_titan.columns = df_titan.columns.str.replace(' ', '_')

    # get Disposition in Good Standing (DIG) records
    df_dig = df_titan.loc [(df_titan['STATUS'] == 'DISPOSITION IN GOOD STANDING') &
                           (df_titan['FILE_NBR'] != '0000000') &
                           (df_titan['STAGE'] != 'CROWN GRANT') &
                           (df_titan['TYPE'] != 'TRANSFER OF ADMINISTRATION/CONTROL') &
                           (df_titan['PURPOSE'] != 'AQUACULTURE')]

    # get replacement application records
    df_rep_app = df_titan[(df_titan['STAGE'] == 'APPLICATION') &
                          (df_titan['APPLICATION_TYPE'] == 'REP') &
                          (df_titan['PURPOSE'] != 'AQUACULTURE')]

    # get the expired. REMOVE the DIG.
    df_expired = df_rep_app[(~df_rep_app['FILE_NBR'].isin(df_dig['FILE_NBR'].tolist())) &
                            (df_rep_app['STATUS'] == 'ACCEPTED')]

    df_new_apps = df_titan.loc[(df_titan['STAGE'] == 'APPLICATION') &
                               (df_titan['STATUS'] == 'ACCEPTED') &
                               (df_titan['PURPOSE'] != 'AQUACULTURE') &
                               ((df_titan['APPLICATION_TYPE'] == 'NEW') | (df_titan['APPLICATION_TYPE'] == 'PRE RNWL'))]

    return [df_dig, df_expired, df_new_apps]


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
        arcpy.CreateDatabaseConnection_management (workspace,name, database_platform,instance,
                            account_authorization, username ,password, 'DO_NOT_SAVE_USERNAME')
    else:
        pass

    return bcgw_conn_path


def proximity_alaysis (workspace, bcgw_conn_path, table, zone_layer, zone_dict):
    """Returns the spatial intersection of tenure shapes and District poximity areas"""
    tenure_layer = os.path.join(workspace, 'Temp_BCGW.sde', 'WHSE_TANTALIS.TA_CROWN_TENURES_SVW')
    tenureLayer = 'tenure_layer'
    arcpy.MakeFeatureLayer_management (tenure_layer, tenureLayer)
    arcpy.AddJoin_management(tenureLayer, 'DISPOSITION_TRANSACTION_SID', table, 'DTID','KEEP_COMMON')
    tenures = 'in_memory\pm_tenures'
    arcpy.CopyFeatures_management(tenureLayer, tenures)

    print ('Creating new fields...')
    exist_fields = [f.name for f in arcpy.ListFields(tenures)]
    add_fields = ['ZONE', 'NR_DISTRICT', 'OFFICE_ASSIGN']

    for field in add_fields:
        if field not in exist_fields:
            arcpy.AddField_management(tenures, field, "TEXT", field_length=100)
        else:
            pass

    count_rows = int(arcpy.GetCount_management(tenures).getOutput(0))
    counter = 1

    tenure_fields = ['SHAPE@','CROWN_LANDS_FILE'] + add_fields
    tenure_cursor = arcpy.da.UpdateCursor(tenures, tenure_fields)
    for rowt in tenure_cursor:
        if rowt [0] != None:
            print ("..computing proximity: shape {} of {}".format(counter,count_rows))
            tenure_geometry = rowt[0]
            distance_dict = {}
            area_dic = {}
            counter += 1

            zone_cursor = arcpy.da.SearchCursor(zone_layer, ['SHAPE@','Zone', 'ORG_UNIT_N','Name'])
            for row in zone_cursor:
                zone_geometry = row[0]
                inters_area = tenure_geometry.intersect(zone_geometry, 4).getArea('PLANAR','HECTARES')
                area_dic [str(row[1])] = round(inters_area,2)

                if  sum(area_dic.values()) == 0 :
                    dist = tenure_geometry.distanceTo(zone_geometry)
                    distance_dict [str(row[1])] = round(dist,2)
                else:
                    pass

            if sum(area_dic.values()) == 0 :

                nearest_zone = min(distance_dict, key=lambda k: distance_dict[k])
                #print ('closest zone to {} is {} '.format (str(tenure_cursor[1]),nearest_zone))
                rowt[2] = str(nearest_zone)
                rowt[3] = str(zone_dict.get(nearest_zone)[0])
                rowt[4] = str(zone_dict.get(nearest_zone)[1])
                tenure_cursor.updateRow(rowt)

            else:
                largest_inter_area = max(area_dic, key=lambda k: area_dic[k])
                #print ('closest zone to {} is {} '.format (str(tenure_cursor[1]),largest_inter_area))
                rowt[2] = str(largest_inter_area)
                rowt[3] = str(zone_dict.get(largest_inter_area)[0])
                rowt[4] = str(zone_dict.get(largest_inter_area)[1])
                tenure_cursor.updateRow(rowt)
        else:
             pass

    return tenures

def fc2df (fc, fields):
    """Returns a df based on a Feature Class"""
    arr = arcpy.da.FeatureClassToNumPyArray(
                in_table=fc,
                field_names=fields,
                skip_nulls=False,
                null_value=-99999)

    tenures_df = pd.DataFrame (arr)

    return tenures_df

def generate_report (workspace, df, file_name, titan_date):
    """ Exports dataframes to multi-tab excel spreasheet"""

    df.sort_values(by=['ZONE'], inplace=True)
    df.drop_duplicates(subset = ['FILE_NBR'], keep = 'last', inplace = True)
    df['ZONE'] = 'ZONE ' + df['ZONE'].astype(str)

    df_summary = pd.pivot_table(df, values='FILE_NBR', index=['ZONE','NR_DISTRICT', 'OFFICE_ASSIGN',
                                                              'APPLICATION_TYPE','TYPE','PURPOSE'],
                                aggfunc='count', fill_value=0).reset_index().rename_axis(None, axis=1)

    df_summary.rename(columns={"FILE_NBR": "Number of files", "TYPE": "TENURE TYPE"}, inplace = True)

    df_list = [df, df_summary]
    sheet_list = ["List", "Summary"]
    file_name = os.path.join(workspace, file_name + '_asof_' + titan_date[1] +'.xlsx')

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
    """Runs the program"""
    arcpy.env.overwriteOutput = True
    arcpy.env.qualifiedFieldNames = False

    workspace = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20210607_proximity_analysis'
    titan_report = os.path.join(workspace, 'TITAN_RPT012_20210614.xlsx')

    req_cols = ['DISTRICT OFFICE', 'FILE #', 'DTID', 'STAGE', 'CLIENT NAME', 'STATUS', 'APPLICATION TYPE', 'LOCATION', 'TYPE', 'SUBTYPE',
                'PURPOSE', 'SUBPURPOSE', 'RECEIVED DATE', 'EXPIRY DATE', 'FDISTRICT', 'INTEREST PARCEL ID']

    check_TITAN_cols (titan_report, req_cols)

    titan_date = get_titan_report_date (titan_report)
    print 'Titan report date/time is: {}'.format (titan_date[0])

    print ('Filtering data...')
    dfs = filter_data (titan_report)

    print ("Converting df to GBD table...")
    table = df2gdb (dfs[0])

    print ('Connecting to BCGW...')
    bcgw_user_name = input("Enter your BCGW username:")
    bcgw_password = input("Enter your BCGW password:")
    bcgw_conn_path = create_bcgw_connection(workspace, bcgw_user_name,bcgw_password)

    print ("Performing the proximity analysis...")
    zone_layer = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\DATASETS\local_data.gdb\district_regional_areas'
    zone_dict = {
                '1': ['Haida Gwaii Natural Resource District','Haida Gwaii Staff'],
                '2': ['North Island Natural Resource District','Port McNeil Staff / Nanaimo Office'],
                '3': ['North Island Natural Resource District','Port McNeil Staff / Nanaimo Office'],
                '4': ['Campbell River Natural Resource District','Campbell River Staff / Nanaimo Office'],
                '5-1': ['South Island Natural Resource District','Port Alberni Staff'],
                '5-2': ['South Island Natural Resource District','Lasqueti Is Unit - Port Alberni Staff'],
                '6': ['South Island Natural Resource District','Nanaimo Staff &  Port Alberni for Log Handling / Storage']
                }

    tenures = proximity_alaysis (workspace, bcgw_conn_path, table,zone_layer,zone_dict)

    print ("Generating the report...")
    fields = ['DISTRICT_OFFICE', 'FDISTRICT', 'FILE_NBR', 'DTID', 'CLIENT_NAME', 'STAGE', 'STATUS', 'APPLICATION_TYPE',
              'LOCATION', 'TYPE', 'SUBTYPE', 'PURPOSE', 'SUBPURPOSE', 'RECEIVED_DATE','EXPIRY_DATE', 'ZONE',
              'NR_DISTRICT', 'OFFICE_ASSIGN']

    tenures_df = fc2df (tenures,fields)
    file_name = 'PROXIMITY_DIG'
    generate_report (workspace, tenures_df, file_name, titan_date)

    arcpy.Delete_management('in_memory')

    print ('Done!')

if __name__ == "__main__":
    main()
