#-------------------------------------------------------------------------------
# Name:        Office Assignement Analysis
#
# Purpose:     This script generates a report on Tenure files (Disposition in Good,
#              new and expired) assigned to each District Office.
#
# Input(s):    (1) Workspace (folder) where outputs will be generated.
#              (2) Titan report (excel file ). The script checks if all required
#                  columns are available in TITAN report
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     20-06-2021
# Updated:
#-------------------------------------------------------------------------------

import os
import pandas as pd
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
                              converters={'FILE #':str})

    # fill nan values for district office
    df_titan['DISTRICT OFFICE'] = df_titan['DISTRICT OFFICE'].fillna(value='NANAIMO')

    #Remove spaces from culomn names, remove special characters
    df_titan.rename(columns={'FILE #':'FILE_NBR'}, inplace=True)
    df_titan.columns = df_titan.columns.str.replace(' ', '_')

    # Remove AQUACULTURE tenures ***CHANGE IF REQUIRED***
    df_titan = df_titan.loc[(df_titan['PURPOSE'] != 'AQUACULTURE')]

    # get Disposition in Good Standing (DIG) records
    df_dig = df_titan.loc [(df_titan['STATUS'] == 'DISPOSITION IN GOOD STANDING') &
                           (df_titan['FILE_NBR'] != '0000000')]

    # get replacement application records
    df_rep_app = df_titan[(df_titan['STAGE'] == 'APPLICATION') &
                          (df_titan['APPLICATION_TYPE'] == 'REP')]


    # get the expired. REMOVE the DIG.
    df_expired = df_rep_app[(~df_rep_app['FILE_NBR'].isin(df_dig['FILE_NBR'].tolist())) &
                            (df_rep_app['STATUS'] == 'ACCEPTED')]

    df_new_apps = df_titan.loc[(df_titan['STAGE'] == 'APPLICATION') &
                               (df_titan['STATUS'] == 'ACCEPTED') &
                               ((df_titan['APPLICATION_TYPE'] == 'NEW') | (df_titan['APPLICATION_TYPE'] == 'PRE RNWL'))]

    return [df_dig, df_expired, df_new_apps]


def compute_summary (n,df_list):
    """Return summary dfs"""
    #Summary all files per Distrcit office
    df_sum_all = pd.pivot_table(df_list[n], values='FILE_NBR', index=['DISTRICT_OFFICE'],
                                aggfunc='count', fill_value=0).reset_index().rename_axis(None, axis=1)
    df_sum_all.rename(columns={"FILE_NBR": "Number of files"}, inplace = True)

    #Summary Application types per Distrcit office
    df_sum_appType = pd.pivot_table(df_list[n], values='FILE_NBR', index=['DISTRICT_OFFICE', 'APPLICATION_TYPE'],
                                    aggfunc='count', fill_value=0).reset_index().rename_axis(None, axis=1)
    df_sum_appType.rename(columns={"FILE_NBR": "Number of files"}, inplace = True)

    #Summary Tenure types per Distrcit office
    df_sum_tenureType = pd.pivot_table(df_list[n], values='FILE_NBR', index=['DISTRICT_OFFICE','TYPE'],
                                       aggfunc='count', fill_value=0).reset_index().rename_axis(None, axis=1)
    df_sum_tenureType.rename(columns={"FILE_NBR": "Number of files", "TYPE": "TENURE TYPE"}, inplace = True)

    #Summary Tenure ppurposes per Distrcit office
    df_sum_purpose = pd.pivot_table(df_list[n], values='FILE_NBR', index=['DISTRICT_OFFICE','PURPOSE','TYPE'],
                                    aggfunc='count', fill_value=0).reset_index().rename_axis(None, axis=1)
    df_sum_purpose.rename(columns={"FILE_NBR": "Number of files", "TYPE": "TENURE TYPE"}, inplace = True)

    return [df_sum_all, df_sum_appType, df_sum_tenureType, df_sum_purpose]


def create_report (df_list, sheet_list, file_name):
    """ Exports dataframes to multi-tab excel spreasheet"""
    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1
        try:
            dataframe.drop('ORG._UNIT', axis=1, inplace=True)
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
    """Runs the program"""
    workspace = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20210607_proximity_analysis'
    titan_report = os.path.join(workspace, 'TITAN_RPT012.xlsx')

    req_cols = ['DISTRICT OFFICE', 'FILE #', 'DTID', 'STAGE', 'CLIENT NAME', 'STATUS', 'APPLICATION TYPE', 'LOCATION', 'TYPE', 'SUBTYPE',
                'PURPOSE', 'SUBPURPOSE', 'RECEIVED DATE', 'EXPIRY DATE', 'FDISTRICT']
    check_TITAN_cols (titan_report, req_cols)

    titan_date = get_titan_report_date (titan_report)
    print 'Titan report date/time is: {}'.format (titan_date[0])

    print ('Filtering data...')
    dfs = filter_data (titan_report)
    sheet_list = ['List', 'Summary OFFICE', 'Summary APPLICATION TYPE',
                  'Summary TENURE TYPE', 'Summary TENURE PURPOSE']

    print ('Computing Summaries and exporting reports...')
    for n in range (0,3):
        print ('...report {} of 3'.format (n+1))
        df_sums = compute_summary (n, dfs)
        df_list =  df_sums
        df_list.insert(0,dfs[n])
        if n == 0:
            file_name = 'OFFICE_ASSIGN_DIG'
        elif n == 1:
            file_name = 'OFFICE_ASSIGN_APP_EXPIRED'
        elif n == 2:
            file_name = 'OFFICE_ASSIGN_APP_NEW'

        file_path = os.path.join(workspace, 'outputs', file_name + '_asof_' + titan_date[1] + '.xlsx')
        create_report (df_list, sheet_list, file_path)

    print ('Done!')

if __name__ == "__main__":
    main()

