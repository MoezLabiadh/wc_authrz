#-------------------------------------------------------------------------------
# Name:        Maanluth Annual Reporting
#
# Purpose:     This script generates information required
#              for Maanluth Annual Reporting
#
# Input(s):    (1) Workspace (folder) where outputs will be generated.
#              (2) Titan report (excel file) - TITAN_RPT010
#              (3) Start Year (e.g 2021)
#              (4) BCGW connection parameters - will be prompt during
#                  script execution
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     20-07-2021
# Updated:
#-------------------------------------------------------------------------------

import os
import arcpy
import pandas as pd
import numpy as np
import datetime

#Hide pandas warning
pd.set_option('mode.chained_assignment', None)


def get_titan_report_date (titan_report):
    """ Returns the date of the input TITAN report"""
    df = pd.read_excel(titan_report,'Info')
    titan_date = df.columns[1]

    return titan_date


def filter_TITAN (titan_report, year):
    """Returns a df of filtered TITAN report"""
    #Read TITAN report into df
    df = pd.read_excel(titan_report, 'TITAN_RPT010',
                       converters={'FILE NUMBER':str,
                                   'OFFER ACCEPTED DATE':str})

    # Convert expiry date column to datetime format
    df['OFFERED DATE'] =  pd.to_datetime(df['OFFERED DATE'],
                                    infer_datetime_format=True,
                                    errors = 'coerce').dt.date

    df['EXPIRY DATE'] =  pd.to_datetime(df['EXPIRY DATE'],
                                    infer_datetime_format=True,
                                    errors = 'coerce').dt.date


    # Filter the needed data: tenures expiring during fiscal year
    df = df.loc [(df['OFFERED DATE'] >= datetime.date(year-1,9,1)) &
                 (df['OFFERED DATE'] <= datetime.date(year,8,31)) &
                 (~df['STATUS'].isin(['CANCELLED', 'EXPIRED']))]

    #Calculate Tenure Length
    df ['diff'] = ((df['EXPIRY DATE'] - df['OFFERED DATE'] )\
                                  / np.timedelta64(1,'Y'))
    df['TENURE LENGTH YEARS'] = df['diff'].fillna(0).round().astype(int)

    #Remove spaces from column names, remove special characters
    df.sort_values(by = ['OFFERED DATE'], inplace = True)
    df['OFFERED DATE'] = df['OFFERED DATE'].astype(str)
    df['EXPIRY DATE'] = df['EXPIRY DATE'].astype(str)
    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
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


def get_offered (bcgw_conn_path,table,workspace):
    """Returns the spatial intersection of offered tenures and Maanulth area"""
    tenures = os.path.join(bcgw_conn_path, 'WHSE_TANTALIS.TA_CROWN_TENURES_SVW')
    tenureLayer = 'tenure_layer'
    arcpy.MakeFeatureLayer_management (tenures, tenureLayer)
    arcpy.AddJoin_management(tenureLayer, 'CROWN_LANDS_FILE', table, 'FILE_NUMBER','KEEP_COMMON')

    offered = 'in_memory\_offered'
    #offered = os.path.join(workspace, 'data.gdb', 'offered_20210811')
    arcpy.CopyFeatures_management(tenureLayer, offered)

    FN_layer = os.path.join(bcgw_conn_path, 'WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP')
    maanLayer = 'maan_layer'
    where_clause = """ CNSLTN_AREA_NAME = 'Maa-nulth First Nations Final Agreement Areas' """
    arcpy.MakeFeatureLayer_management (FN_layer, maanLayer, where_clause)
    offered_maan = 'in_memory\_offered_maan'
    #offered_maan = os.path.join(workspace, 'data.gdb', 'offered_maan_20210811')
    arcpy.SpatialJoin_analysis(offered, maanLayer,offered_maan,'', 'KEEP_COMMON')

    return offered_maan


def intersect_LU (bcgw_conn_path, offered_maan):
    """ Returns the intersection of offered tenures and Landscape Units"""
    offered_maan_lu = 'in_memory\_offered_maan_lu'
    #offered_maan_lu = os.path.join(workspace, 'data.gdb', 'lu_intersect_tempo')
    lu_fc = os.path.join(bcgw_conn_path, 'WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW')
    arcpy.Intersect_analysis([offered_maan,lu_fc], offered_maan_lu)
    arcpy.AddField_management(offered_maan_lu, "AREA_HA", "DOUBLE")
    with arcpy.da.UpdateCursor(offered_maan_lu, ["AREA_HA","SHAPE@AREA"]) as cursor:
        for row in cursor:
            row[0] = row[1]/10000
            cursor.updateRow(row)

    return offered_maan_lu


def intersect_IHA (bcgw_conn_path, offered_maan):
    """ Returns the intersection of offered tenures and Important HArvest Areas"""
    arcpy.DeleteField_management (offered_maan, ['STATUS'])
    offered_maan_iha = 'in_memory\_offered_maan_iha'
    #offered_maan_iha = os.path.join(workspace, 'data.gdb', 'iha_intersect_tempo')
    iha_fc = os.path.join(bcgw_conn_path, 'WHSE_LEGAL_ADMIN_BOUNDARIES.FNT_TREATY_SIDE_AGREEMENTS_SP')
    maan_iha = 'maan_iha'
    where_clause = """ TREATY = 'Maa-nulth'
                       AND AREA_TYPE = 'Important Harvest Area' """
    arcpy.MakeFeatureLayer_management (iha_fc, maan_iha, where_clause)
    arcpy.Intersect_analysis([offered_maan,maan_iha], offered_maan_iha)
    arcpy.AddField_management(offered_maan_iha, "AREA_HA", "DOUBLE")
    with arcpy.da.UpdateCursor(offered_maan_iha, ["AREA_HA","SHAPE@AREA"]) as cursor:
        for row in cursor:
            row[0] = row[1]/10000
            cursor.updateRow(row)

    return offered_maan_iha



def fc2df (fc, fields):
    """Returns a df based on a Feature Class"""
    arr = arcpy.da.FeatureClassToNumPyArray(
                in_table=fc,
                field_names=fields,
                skip_nulls=False,
                null_value=-99999)

    df = pd.DataFrame (arr)

    return df


def sum_lu (df_lu):
    groupby = df_lu.groupby(['LANDSCAPE_UNIT_NAME'])['AREA_HA'].sum().reset_index()
    df_lu_sum = pd.DataFrame(groupby)

    return df_lu_sum


def generate_report (workspace, df_list, sheet_list, year):
    """ Exports dataframes to multi-tab excel spreasheet"""
    file_name = os.path.join(workspace, 'Maanulth_annual_reporting_' + str(year) +'.xlsx')

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
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'count'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()


def main():
    """Runs the program"""
    arcpy.env.overwriteOutput = True
    arcpy.env.qualifiedFieldNames = False
    workspace = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\202110720_Maanulth_reporting\final_aug2021'
    titan_report = os.path.join(workspace, 'TITAN_RPT010.xlsx')

    titan_date = get_titan_report_date (titan_report)
    print 'Titan report date/time is: {}'.format (titan_date)

    print ("Filtering TITAN report...")
    year = 2021
    df_filter = filter_TITAN (titan_report,year)

    print ("Converting df to GBD table...")
    table = df2gdb (df_filter)

    print ('Connecting to BCGW...PLease enter your credentials')
    bcgw_user_name = 'MLABIADH'
    bcgw_password = 'MoezLab8813'
    #bcgw_user_name = raw_input("Enter your BCGW username:")
    #bcgw_password = raw_input("Enter your BCGW password:")
    bcgw_conn_path = create_bcgw_connection(workspace, bcgw_user_name,bcgw_password)

    print ("Retrieving Offered Tenures in Maanulth FN area...")
    offered_maan = get_offered (bcgw_conn_path, table,workspace)
    print ("Performing spatial overlays...")
    print ("..overlay with Landscape Units")
    offered_maan_lu = intersect_LU (bcgw_conn_path, offered_maan)
    print ("..overlay with Important Harvest Areas")
    offered_maan_iha = intersect_IHA (bcgw_conn_path, offered_maan)

    print ("Export dfs...")
    fields = ['FILE_NUMBER', 'DISTRICT_OFFICE', 'TENURE_STAGE','TENURE_STATUS', 'TASK_DESCRIPTION', 'OFFERED_DATE', 'OFFER_ACCEPTED_DATE',
              'EXPIRY_DATE', 'TENURE_LENGTH_YEARS','TYPE','SUBTYPE','PURPOSE','SUBPURPOSE',
              'TENURE_AREA_IN_HECTARES']
    df_tenures = fc2df (offered_maan, fields)

    fields = ['FILE_NUMBER', 'LANDSCAPE_UNIT_NAME','AREA_HA']
    df_lu = fc2df (offered_maan_lu, fields)
    df_lu_sum = sum_lu (df_lu)


    fields = ['FILE_NUMBER','AREA_TYPE', 'TREATY_SIDE_AGREEMENT_ID',
              'TREATY_SIDE_AGREEMENT_AREA_ID','STATUS', 'AREA_HA']
    df_iha = fc2df (offered_maan_iha, fields)

    print ('Generate the report...')
    df_tenures.sort_values (by = ['OFFERED_DATE'], inplace=True)
    df_list = [df_tenures,df_iha,df_lu, df_lu_sum]
    sheet_list = ['Offered Tenures in Maan. area', 'Overlay - IHA',
                  'Overlay - LU', 'LU Area Summary']
    generate_report (workspace, df_list, sheet_list, year)

    arcpy.Delete_management('in_memory')

    print ('Processing completed!')


if __name__ == "__main__":
    main()
