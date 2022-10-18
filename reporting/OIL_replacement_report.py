#-------------------------------------------------------------------------------
# Name:        OIL Replacement Report
#
# Purpose:     This script generates the OIL Replacement Report.
#              The report is generated Quarterly.
#              
#
# Input(s):    (1) Workspace (folder) where inputs are located.
#              (2) Titan reports: RPT009, RPT011
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     18-10-2022
# Updated:
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import datetime as dt
import numpy as np
import pandas as pd


def get_titan_report_date (rpt009):
    """ Returns the date of the input TITAN report"""
    df = pd.read_excel(rpt009,'Info')
    titan_date = df.columns[1].strftime("%Y%m%d")
   
    return titan_date


def load_reports (rpt009, rpt011):
    """Loads TITAN Reports"""
    df_rpt11 = pd.read_excel (rpt011,'TITAN_RPT011',
                                  converters={'FILE #':str})

    df_rpt09 = pd.read_excel (rpt009,'TITAN_RPT009',
                                  converters={'FILE NUMBER':str})
    
    df_rpt11.rename(columns={'FILE #':'FILE NUMBER'}, inplace=True)
    
    return df_rpt11, df_rpt09 


def get_expired_tenures (df_rpt11, df_rpt09):
    """Retireve Expired tenures and information"""
    df_dig = df_rpt11.loc[(df_rpt11['STATUS'] == 'DISPOSITION IN GOOD STANDING')]
        
    
    df_rep = df_rpt09.loc[(df_rpt09['TASK DESCRIPTION'] == 'REPLACEMENT APPLICATION') &
                          (df_rpt09['STATUS'] == 'ACCEPTED') &
                          (df_rpt09['COMPLETED DATE'].isnull())]
    
    df_rep.drop('EXPIRY DATE', axis=1, inplace=True)
    
    df_exp = df_rpt11.loc[(df_rpt11['STAGE'] == 'TENURE') &
                          (df_rpt11['STATUS'] == 'EXPIRED') &
                          (~df_rpt11['FILE NUMBER'].isin(df_dig['FILE NUMBER'].tolist()))]
    
    df_exp.sort_values(by='EXPIRY DATE', ascending=False, inplace= True)
    df_exp.drop_duplicates(subset=['FILE NUMBER'], keep='first', inplace= True )
    df_exp.drop('RECEIVED DATE', axis=1, inplace=True)
    df_exp = df_exp[['FILE NUMBER','EXPIRY DATE', 'RENT']]
    
    df_exp = df_exp.loc[(df_exp['FILE NUMBER'].isin(df_rep['FILE NUMBER'].tolist()))]
    
    df = pd.merge(df_exp, df_rep, how='left', on= 'FILE NUMBER')
    
    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
    df.loc[df['PURPOSE'] == 'AQUACULTURE', 'DISTRICT OFFICE'] = 'AQUA'
    
    df.rename(columns={'FDISTRICT':'GEOGRAPHIC LOCATION',
                             'RENT': 'MOST RECENT RENTAL AMOUNT'}, inplace=True)

    return df

def add_cols (df):
    """Adding informations forr the report"""
    df['QUEUE'] = 'YES'
    
    df['RECEIVED DATE'] =  pd.to_datetime(df['RECEIVED DATE'],
                                     infer_datetime_format=True,
                                     errors = 'coerce').dt.date
    df['LAND STATUS DATE'] =  pd.to_datetime(df['LAND STATUS DATE'],
                                     infer_datetime_format=True,
                                     errors = 'coerce').dt.date
    
    df ['TODAY']  = dt.datetime.combine(dt.date.today(), dt.datetime.min.time())
    df ['DAYS SINCE EXPIRY'] = df ['TODAY'] - df['EXPIRY DATE']
    
    df ['YRS SINCE EXPIRY'] = df ['DAYS SINCE EXPIRY']/ np.timedelta64(1,'Y')
    df ['YRS SINCE EXPIRY']= df ['YRS SINCE EXPIRY'].apply(np.floor)
    df ['UNBILLED USE OF CROWN LAND'] = df ['YRS SINCE EXPIRY'] * df ['MOST RECENT RENTAL AMOUNT'] 
    
    df ['DAYS SINCE EXPIRY'] = (df ['DAYS SINCE EXPIRY'] / np.timedelta64(1, 'D')).astype(int)
    
    
    df ['PERIOD OF EXPIRY'] = 'N/A'
    
    for i, row in df.iterrows():
        d = row['DAYS SINCE EXPIRY']
        if 1 < d <= 30:
            df.at[i,'PERIOD OF EXPIRY'] = '1-30 days past Expiry'       
        elif 1 < d < 30:
            df.at[i,'PERIOD OF EXPIRY'] = '1-30 days past Expiry'            
        elif 30 < d <= 120:
            df.at[i,'PERIOD OF EXPIRY'] = '30-120 days past Expiry'
        elif 120 < d < 365:
            df.at[i,'PERIOD OF EXPIRY'] = '120-365 days past Expiry'        
        elif 365 < d <= 730:
            df.at[i,'PERIOD OF EXPIRY'] = '1-2 Yrs past Expiry'  
        elif 730 < d <= 1095:
            df.at[i,'PERIOD OF EXPIRY'] = '2-3 Yrs past Expiry'  
        elif d > 1095:
            df.at[i,'PERIOD OF EXPIRY'] = '>3 Yrs past Expiry' 
            
    #cleanup columns
    df['EXPIRY DATE'] =  pd.to_datetime(df['EXPIRY DATE'],
                               infer_datetime_format=True,
                               errors = 'coerce').dt.date
    return df
        
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
   
    
def main():    
    workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20221017_expiredTenures_report_Shawn'
    rpt011 = os.path.join(workspace, 'TITAN_RPT011.xlsx')
    rpt009 = os.path.join(workspace, 'TITAN_RPT009.xlsx')
    
    print ('Loading Titan Reports...')
    titan_date = get_titan_report_date (rpt009)
    df_rpt11, df_rpt09 = load_reports (rpt009, rpt011)
    
    print ('Retrieving Expired Tenures...')
    df = get_expired_tenures (df_rpt11, df_rpt09 )
    
    print ('Adding information...')
    df = add_cols (df)
    
    cols = ['FILE NUMBER', 'GEOGRAPHIC LOCATION', 'DISTRICT OFFICE',
            'USERID ASSIGNED WORK UNIT', 'CLIENT NAME', 'LOCATION',
            'TYPE', 'SUBTYPE', 'PURPOSE', 'SUBPURPOSE', 'RECEIVED DATE',
            'PRIORITY CODE', 'QUEUE','COMMENTS', 'LAND STATUS DATE',
            'EXPIRY DATE', 'YRS SINCE EXPIRY', 'PERIOD OF EXPIRY', 
            'MOST RECENT RENTAL AMOUNT', 'UNBILLED USE OF CROWN LAND']
    
    df = df[cols]
    
    print ('Generating the report...')
    filename = 'replacement_report_{}'.format(titan_date)
    generate_report (workspace, [df], ['REPLACEMETS'],filename)
    
    print ('Processing Completed!')
    
main()
