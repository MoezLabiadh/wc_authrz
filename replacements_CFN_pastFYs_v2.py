import os
#import ast
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
    exclude = [893222,930991,930992,935893,931000,931013,931015,937855,935727]

    df_exp = df.loc [(df['STAGE'] == 'TENURE') &
                 (df['STATUS'].isin(['EXPIRED','DISPOSITION IN GOOD STANDING'])) &
                 (~df['DTID'].isin(exclude)) &
                 (df['EXPIRY DATE'] >= datetime.date(fiscal,4,1)) &
                 (df['EXPIRY DATE'] <= datetime.date(fiscal+1,3,31))]

    print ('df_exp has {} rows'.format(df_exp.shape[0]))

    df_rep = df.loc [(df['STAGE'] == 'APPLICATION') &
                 (df['STATUS'] == 'ACCEPTED') &
                 (df['APPLICATION TYPE'] == 'REP')]

    df_rep = df_rep[['FILE #']]
    print ('df_rep has {} rows'.format(df_rep.shape[0]))

    df_merge = pd.merge(df_exp, df_rep,on='FILE #')
    print ("df_merge has {} rows".format (df_merge.shape[0]))


    # Convert commencement date column to datetime format
    df_merge['COMMENCEMENT DATE'] =  pd.to_datetime(df_merge['COMMENCEMENT DATE'],
                                    infer_datetime_format=True,
                                    errors = 'coerce').dt.date
    #Calculate Tenure Length
    #print (df.dtypes)
    df_merge ['diff'] = ((df_merge['EXPIRY DATE'] - df_merge['COMMENCEMENT DATE'] )\
                                  / np.timedelta64(1,'Y'))

    df_merge['TENURE LENGTH'] = df_merge['diff'].fillna(0).round().astype(int)

    #Remove spaces from column names, remove special characters
    df_merge.sort_values(by = ['EXPIRY DATE'], inplace = True)
    df_merge['EXPIRY DATE'] = df_merge['EXPIRY DATE'].astype(str)
    df_merge['COMMENCEMENT DATE'] = df_merge['COMMENCEMENT DATE'].astype(str)
    df_merge.loc[df['PURPOSE'] == 'AQUACULTURE', 'DISTRICT OFFICE'] = 'AQUACULTURE'
    df_merge['DISTRICT OFFICE'] = df_merge['DISTRICT OFFICE'].fillna(value='NANAIMO')
    df_merge.rename(columns={'FILE #':'FILE_NBR'}, inplace=True)
    df_merge.columns = df_merge.columns.str.replace(' ', '_')

    #out = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20211018_Replacements_CoastalFirstNations\tempo.xlsx'
    #df_merge.to_excel (out)

    return df_merge


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
    #table = 'in_memory\df_table'
    table = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20211018_Replacements_CoastalFirstNations\data.gdb\expired_fy21_rep_all'
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


def get_layers (table, bcgw_conn_path):
    """Returns layers of FN consultation areas and Expiring tenures"""
    tenure_layer = os.path.join(bcgw_conn_path, 'WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES')
    tenureLayer = 'tenure_layer'
    arcpy.MakeFeatureLayer_management (tenure_layer, tenureLayer)
    arcpy.AddJoin_management(tenureLayer, 'INTRID_SID', table, 'INTEREST_PARCEL_ID','KEEP_COMMON')

    expiring_tenures = 'in_memory\_expiring_tenures'
    arcpy.CopyFeatures_management(tenureLayer, expiring_tenures)

    FN_layer = os.path.join(bcgw_conn_path, 'WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP')
    consult_layer = 'consult_layer'
    where = """ CONTACT_ORGANIZATION_NAME  in ('Wuikinuxv Nation', 'Metlakatla First Nation' , 'Kitasoo Band' , 'Heiltsuk Nation' , 'Nuxalk Nation' , 'Gitga''at First Nation') """
    arcpy.MakeFeatureLayer_management(FN_layer, consult_layer, where)
    arcpy.SelectLayerByLocation_management(consult_layer, 'intersect', expiring_tenures)

    consult_areas = 'in_memory\_consult_areas'
    arcpy.Dissolve_management (consult_layer, consult_areas, 'CNSLTN_AREA_NAME')

    return expiring_tenures, consult_areas

def intersect_FN (expiring_tenures,consult_areas):
    """Returns intersection layer of FN consultation areas and Epiring Tenures"""
    #intersect = 'in_memory\_intersect'
    intersect = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20211018_Replacements_CoastalFirstNations\data.gdb\replacements_CFN_FY2022'
    arcpy.SpatialJoin_analysis(expiring_tenures, consult_areas, intersect, 'JOIN_ONE_TO_MANY','KEEP_COMMON')

    return intersect

def main():
    """Runs the program"""
    arcpy.env.overwriteOutput = True
    arcpy.env.qualifiedFieldNames = False
    workspace = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20211018_Replacements_CoastalFirstNations'
    titan_report = os.path.join(workspace, 'TITAN_RPT012_all.xlsx')
    titan_tasks = os.path.join(workspace, 'TITAN_RPT010.xlsx')

    titan_date = get_titan_report_date (titan_report)
    print 'Titan report date/time is: {}'.format (titan_date)

    print ("Filtering TITAN report...")
    fiscal = 2021
    df_merge = filter_TITAN (titan_report,fiscal)


    print ("Adding Max term Column...")
    terms_file = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\DATASETS\Tenure_Terms\max_tenure_terms.xlsx'
    req_fields = ['DISTRICT_OFFICE','FILE_NBR', 'DTID', 'INTEREST_PARCEL_ID', 'COMMENCEMENT_DATE',
          'EXPIRY_DATE', 'TENURE_LENGTH','MAX_TENURE_TERM','STAGE', 'STATUS', 'APPLICATION_TYPE',
          'TYPE', 'SUBTYPE', 'PURPOSE', 'SUBPURPOSE','LOCATION',
          'CLIENT_NAME', 'ADDRESS_LINE_1', 'ADDRESS_LINE_2','ADDRESS_LINE_3','CITY', 'PROVINCE', 'POSTAL_CODE',
          'COUNTRY','STATE','ZIP_CODE']

    df_ten= add_max_term (df_merge, terms_file)
    df_ten = df_ten[req_fields]

    print ("Converting df to GBD table...")
    table = df2gdb (df_ten)

    '''
    print ('Connecting to BCGW...PLease enter your credentials')
    bcgw_user_name = 'MLABIADH'
    bcgw_password = 'MoezLab8814'
    #bcgw_user_name = raw_input("Enter your BCGW username:")
    #bcgw_password = raw_input("Enter your BCGW password:")
    bcgw_conn_path = create_bcgw_connection(workspace, bcgw_user_name,bcgw_password)

    print ('Creating Layers: Consultation areas and Expring Tenures')
    expiring_tenures, consult_layer, consult_areas = get_layers (table, bcgw_conn_path, req_fields)

    print('Intersecting Expiring tenures and FN areas...')
    intersect = intersect_FN (expiring_tenures,consult_areas)
    '''
if __name__ == "__main__":
    main()
