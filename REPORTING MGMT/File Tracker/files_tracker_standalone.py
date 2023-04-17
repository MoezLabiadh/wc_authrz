
#-------------------------------------------------------------------------------
# Name:        Lands Files Tracker
#
# Purpose:     This script generates lands files tracking reports: backlog
#               and active files.
#
# Input(s):    (1) ATS processing time report (excel).
#              (2) Titan workledger report (excel) - RPT009
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     17-04-2023
# Updated:
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
from datetime import date


def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection


def import_ats (ats_f):
    """Reads the ATS report into a df"""
    df = pd.read_excel(ats_f)
    
    df['File Number'] = df['File Number'].fillna(0)
    df['File Number'] = df['File Number'].astype(str)
    
    df.rename(columns={'Comments': 'ATS Comments'}, inplace=True)
    
    df.loc[(df['Accepted Date'].isnull()) & 
       (df['Rejected Date'].isnull()) & 
       (df['Submission Review Complete Date'].notnull()),
       'Accepted Date'] = df['Submission Review Complete Date']
    
    for index,row in df.iterrows():
        z_nbr = 7 - len(str(row['File Number']))
        df.loc[index, 'File Number'] = z_nbr * '0' + str(row['File Number'])
        
    for col in df:
        if 'Date' in col:
            df[col] =  pd.to_datetime(df[col],
                                   infer_datetime_format=True,
                                   errors = 'coerce').dt.date
        elif 'Unnamed' in col:
            df.drop(col, axis=1, inplace=True)
        
        else:
            pass
            
    
    return df


def import_titan (tnt_f):
    """Reads the Titan work ledger report into a df"""
    df = pd.read_excel(tnt_f,'TITAN_RPT009',
                       converters={'FILE NUMBER':str})
    
    tasks = ['NEW APPLICATION','REPLACEMENT APPLICATION','AMENDMENT','ASSIGNMENT']
    df = df.loc[df['TASK DESCRIPTION'].isin(tasks)]
    
    df.rename(columns={'COMMENTS': 'TANTALIS COMMENTS'}, inplace=True)
 
    del_col = ['ORG. UNIT','MANAGING AGENCY','BCGS','LEGAL DESCRIPTION',
              'FDISTRICT','ADDRESS LINE 1','ADDRESS LINE 2','ADDRESS LINE 3',
              'CITY','PROVINCE','POSTAL CODE','COUNTRY','STATE','ZIP CODE']
    
    for col in df:
        if 'DATE' in col:
            df[col] =  pd.to_datetime(df[col],
                                   infer_datetime_format=True,
                                   errors = 'coerce').dt.date
        elif 'Unnamed' in col:
            df.drop(col, axis=1, inplace=True)
        
        elif col in del_col:
            df.drop(col, axis=1, inplace=True)
            
        else:
            pass
            
    df.loc[df['PURPOSE'] == 'AQUACULTURE', 'DISTRICT OFFICE'] = 'AQUACULTURE'
    df.loc[df['DISTRICT OFFICE'] == 'COURTENAY', 'DISTRICT OFFICE'] = 'AQUACULTURE'
    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
    
    return df

def create_rpt_01(df_tnt,df_ats):
    """ Creates Report 01- New Files in FCBC, not accepted"""
    ats_a = df_ats.loc[df_ats['Authorization Status'] == 'Active']
    #active = ats_a['File Number'].to_list()
    
    df_01= ats_a.loc[(ats_a['Received Date'].notnull()) & (ats_a['Accepted Date'].isnull())]
    
     
    df_01['tempo_join_date']= df_01['Accepted Date'].astype('datetime64[Y]')
    df_tnt['tempo_join_date']= df_tnt['CREATED DATE'].astype('datetime64[Y]')
    
    df_01 = pd.merge(df_01, df_tnt, how='left',
                     left_on=['File Number','tempo_join_date'],
                     right_on=['FILE NUMBER','tempo_join_date'])
    
    df_01.sort_values(by=['Received Date'], ascending=False, inplace=True)
    df_01.reset_index(drop = True, inplace = True)

    return df_01


def create_rpt_02(df_tnt,df_ats):
    """ Creates Report 02- New Files accepted by FCBC, not assigned to a Land Officer"""
    ats_r = df_ats.loc[df_ats['Authorization Status'].isin(['Closed', 'On Hold'])]
    notactive = ats_r['File Number'].to_list()
    
    df_02= df_tnt.loc[(df_tnt['TASK DESCRIPTION'] == 'NEW APPLICATION') &
                      (~df_tnt['FILE NUMBER'].isin(notactive)) &
                      (df_tnt['USERID ASSIGNED TO'].isnull()) &
                      (df_tnt['STATUS'] == 'ACCEPTED')]
      
    df_02['tempo_join_date']= df_02['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_02 = pd.merge(df_02, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_02.sort_values(by=['CREATED DATE'], ascending=False, inplace=True)
    df_02.reset_index(drop = True, inplace = True)
    
    return df_02


def create_rpt_03(df_tnt,df_ats):
    """ Creates Report 03- Expired Tenures autogenerated as replacement application 
                           and not assigned to an LO for Replacement"""
    
    ats_r = df_ats.loc[df_ats['Authorization Status'].isin(['Closed', 'On Hold'])]
    
    files_r = ats_r['File Number'].to_list()
    
    df_03= df_tnt.loc[(df_tnt['TASK DESCRIPTION']== 'REPLACEMENT APPLICATION') &
                      (df_tnt['STATUS']== 'ACCEPTED') &
                      (~df_tnt['FILE NUMBER'].isin(files_r)) &
                      (df_tnt['USERID ASSIGNED TO'].isnull())]
    
    df_03['tempo_join_date']= df_03['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
        
        
    df_03 = pd.merge(df_03, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
        
    df_03.sort_values(by=['RECEIVED DATE'], ascending=False, inplace=True)
    df_03.reset_index(drop = True, inplace = True)
    
    return df_03


def create_rpt_04(df_tnt,df_ats):
    """ Creates Report 04- Unassigned Assignments"""
    ats_r = df_ats.loc[df_ats['Authorization Status'].isin(['Closed', 'On Hold'])]
    notactive = ats_r['File Number'].to_list()
    
    df_04= df_tnt.loc[(df_tnt['TASK DESCRIPTION'] == 'ASSIGNMENT') &
                      (~df_tnt['FILE NUMBER'].isin(notactive)) &
                      (df_tnt['USERID ASSIGNED TO'].isnull()) &
                      (df_tnt['STATUS'] == 'ACCEPTED')]
      
    df_04['tempo_join_date']= df_04['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_04 = pd.merge(df_04, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_04.sort_values(by=['CREATED DATE'], ascending=False, inplace=True)
    df_04.reset_index(drop = True, inplace = True)
    
    return df_04


def create_rpt_05 (df_tnt,df_ats):
    """ Creates Report 05- Files Assigned to LO, work in progress"""
    df_ats = df_ats.loc[df_ats['Authorization Status'].isin(['Active','Closed'])]
    
    df_05= df_tnt.loc[(df_tnt['USERID ASSIGNED TO'].notnull()) &
                      (df_tnt['REPORTED DATE'].isnull()) &
                      (df_tnt['TASK DESCRIPTION'].isin(['NEW APPLICATION', 'REPLACEMENT APPLICATION'])) &            
                      (df_tnt['STATUS'] == 'ACCEPTED')]

    df_05['tempo_join_date']= df_05['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_05 = pd.merge(df_05, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_05.sort_values(by=['CREATED DATE'], ascending=False, inplace=True)
    df_05.reset_index(drop = True, inplace = True)
    
    return df_05


def create_rpt_06 (df_tnt,df_ats):
    """ Creates Report 06- Files placed on hold by an LO"""
    df_ats = df_ats.loc[(df_ats['Authorization Status']== 'On Hold') &
                        (df_ats['Accepted Date'].notnull())]
    hold_l = df_ats['File Number'].to_list()
    
    df_06= df_tnt.loc[(df_tnt['STATUS'] == 'ACCEPTED') & 
                      (df_tnt['FILE NUMBER'].isin(hold_l))]

    df_06['tempo_join_date']= df_06['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_06 = pd.merge(df_06, df_ats, how='left',
                     left_on=['FILE NUMBER'],
                     right_on=['File Number'])
    
    df_06.sort_values(by=['CREATED DATE'], ascending=False, inplace=True)
    df_06.reset_index(drop = True, inplace = True)
    
    return df_06


def create_rpt_07 (df_tnt,df_ats):
    """ Creates Report 07- Files with LUR Complete, awaiting approval of recommendation"""
    df_ats = df_ats.loc[df_ats['Authorization Status'].isin(['Active','Closed'])]
    
    df_07= df_tnt.loc[(df_tnt['REPORTED DATE'].notnull()) &
                    (df_tnt['ADJUDICATED DATE'].isnull()) &
                    (df_tnt['TASK DESCRIPTION'].isin(['NEW APPLICATION', 'REPLACEMENT APPLICATION'])) &
                    (df_tnt['STATUS'] == 'ACCEPTED')]

    df_07['tempo_join_date']= df_07['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_07 = pd.merge(df_07, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_07.sort_values(by=['REPORTED DATE'], ascending=False, inplace=True)
    df_07.reset_index(drop = True, inplace = True)
    
    return df_07


def create_rpt_08 (df_tnt,df_ats):
    """ Creates Report 08- Files with decision made, awaiting offer"""
    df_ats = df_ats.loc[df_ats['Authorization Status'].isin(['Active','Closed'])]
    
    df_08= df_tnt.loc[(df_tnt['ADJUDICATED DATE'].notnull()) &
                     (df_tnt['OFFERED DATE'].isnull()) &
                     (df_tnt['TASK DESCRIPTION'].isin(['NEW APPLICATION', 'REPLACEMENT APPLICATION'])) &            
                     (df_tnt['STATUS'] == 'ACCEPTED')]

    df_08['tempo_join_date']= df_08['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_08 = pd.merge(df_08, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_08.sort_values(by=['ADJUDICATED DATE'], ascending=False, inplace=True)
    df_08.reset_index(drop = True, inplace = True)
    
    return df_08


def create_rpt_09 (df_tnt,df_ats):
    """ Creates Report 09- Files with offer made, awaiting acceptance"""
    df_ats = df_ats.loc[df_ats['Authorization Status'].isin(['Active','Closed'])]
    df_09= df_tnt.loc[(df_tnt['OFFERED DATE'].notnull()) &
                      (df_tnt['OFFER ACCEPTED DATE'].isnull())&
                      (df_tnt['TASK DESCRIPTION'].isin(['NEW APPLICATION', 'REPLACEMENT APPLICATION'])) &                      
                      (df_tnt['STATUS'] == 'OFFERED')]
    
    df_09['tempo_join_date']= df_09['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_09 = pd.merge(df_09, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_09.sort_values(by=['OFFERED DATE'], ascending=False, inplace=True)
    df_09.reset_index(drop = True, inplace = True)
    
    return df_09


def create_rpt_10 (df_tnt,df_ats):
    """ Creates Report 10- Files with offer accepted"""
    df_ats = df_ats.loc[df_ats['Authorization Status'].isin(['Active','Closed'])]
    df_10= df_tnt.loc[(df_tnt['OFFER ACCEPTED DATE'].notnull()) &
                     (df_tnt['TASK DESCRIPTION'].isin(['NEW APPLICATION', 'REPLACEMENT APPLICATION'])) &                      
                      (df_tnt['STATUS'] == 'OFFER ACCEPTED')]
    
    df_10['tempo_join_date']= df_10['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_10 = pd.merge(df_10, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_10.sort_values(by=['OFFER ACCEPTED DATE'], ascending=False, inplace=True)
    df_10.reset_index(drop = True, inplace = True)
    
    return df_10


def set_rpt_colums (df_ats, dfs):
    """ Set the report columns"""
    cols = ['Region Name',
         'Business Area',
         'DISTRICT OFFICE',
         'FILE NUMBER',
         'TYPE',
         'SUBTYPE',
         'PURPOSE',
         'SUBPURPOSE',
         'Authorization Status',
         'STATUS',
         'TASK DESCRIPTION',
         'FCBC Assigned To',
         'USERID ASSIGNED TO',
         'OTHER EMPLOYEES ASSIGNED TO',
         'FN Consultation Lead',
         'Adjudication Lead',
         'PRIORITY CODE',
         'Received Date',
         'RECEIVED DATE',
         'Accepted Date',
         'CREATED DATE',
         'Acceptance Complete Net Processing Time',
         'Submission Review Complete Date',
         'Submission Review Net Processing Time',
         'LAND STATUS DATE',
         'First Nation Start Date',
         'First Nation Completion Date',
         'FN Consultation Net Time',
         'Technical Review Complete Date',
         'REPORTED DATE',
         'Technical Review Complete Net Processing Time',
         'ADJUDICATED DATE',
         'OFFERED DATE',
         'OFFER ACCEPTED DATE',
         'Total Processing Time',
         'Total On Hold Time',
         'Net Processing Time',
         'CLIENT NAME',
         'LOCATION',
         'TANTALIS COMMENTS',
         'ATS Comments']
    
    dfs[0] = dfs[0][list(df_ats.columns)[:14]]
    dfs[0].columns = [x.upper() for x in dfs[0].columns]
    
    dfs_f = [dfs[0]]   
    
    for df in dfs[1:]:
        df = df[cols]
        df['Region Name'] = 'WEST COAST'
        df['Business Area'] = 'LANDS'

        df.rename({'Authorization Status': 'ATS STATUS', 
                   'STATUS': 'TANTALIS STATUS',
                   'TASK DESCRIPTION': 'APPLICATION TYPE',
                   'USERID ASSIGNED TO': 'EMPLOYEE ASSIGNED TO',
                   'Received Date': 'ATS RECEIVED DATE',
                   'RECEIVED DATE': 'TANTALIS RECEIVED DATE'}, 
                  axis=1, inplace=True)

        df.columns = [x.upper() for x in df.columns]
        
        dfs_f.append (df)
    
    return dfs_f
        

def create_summary_rpt (dfs_f):
    """Creates a summary report - 1st page"""
    rpt_ids = ['rpt_01',
               'rpt_02',
               'rpt_03',
               'rpt_04',
               'rpt_05',
               'rpt_06',
               'rpt_07',
               'rpt_08',
               'rpt_09',
               'rpt_10']
    
    rpt_nmes = ['New Files in FCBC, not accepted',
                'New Files accepted by FCBC, not assigned to a Land Officer',
                'Expired Tenures autogenerated as replacement applications, not assigned to an LO',
                'Unassigned Assignments',
                'Files Assigned to LO, work in progress',
                'Files placed on hold by an LO',
                'Files with LUR Complete, awaiting approval of recommendation',
                'Files with decision made, awaiting offer',
                'Files with offer made, awaiting acceptance',
                'Files with offer accepted']
    
    #rpt_gen = ['Y']* len(rpt_nmes)
    
    rpt_fls = [df.shape[0] for df in dfs_f]
    
    df_00 = pd.DataFrame({'REPORT ID': rpt_ids,
                           'REPORT TITLE' : rpt_nmes,
                           'TOTAL NBR OF FILES': rpt_fls})
    
    return df_00, rpt_ids
 
    
def compute_stats (dfs_f,df_00,rpt_ids):
    """Compute stats: Number of files per region and report"""
    df_grs = []
    for df in dfs_f[1:]:
        df_gr = df.groupby('DISTRICT OFFICE')['REGION NAME'].count().reset_index()
        df_gr.sort_values(by=['DISTRICT OFFICE'], inplace = True)
        df_gr_pv = pd.pivot_table(df_gr, values='REGION NAME',
                        columns=['DISTRICT OFFICE'], fill_value=0)
        df_grs.append (df_gr_pv)
    
    df_grs_o = pd.concat(df_grs).reset_index(drop=True)
    df_grs_o.fillna(0, inplace=True)
    df_grs_o['REPORT ID'] = rpt_ids[1:]
    
    df_stats = pd.merge(df_00,df_grs_o, how='left',on='REPORT ID')
    
    return df_stats


def compute_plot (df_stats,filename):
    """Computes a barplot of number of nbr applications per rpt_id and office """
    df_pl = df_stats[['REPORT ID','AQUACULTURE', 'CAMPBELL RIVER', 
                      'NANAIMO', 'PORT ALBERNI','PORT MCNEILL', 'HAIDA GWAII']]
    
    df_pl = df_pl = df_pl[1:]
    
    ax = df_pl.plot.bar(x= 'REPORT ID',stacked=True, rot=0,figsize=(15, 8))
    ax.set_ylabel("Nbr of Files")
    ax.set_xlabel("Report ID")  
    
    fig = ax.get_figure()
    fig.savefig(filename+'_plot')
   

def create_report (df_list, sheet_list,filename):
    """ Exports dataframes to multi-tab excel spreasheet"""

    
    writer = pd.ExcelWriter(filename+'.xlsx',engine='xlsxwriter')

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
    
    print ('\nConnecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    #connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    
    print ('\nReading Input files')
    print('...ats report')
    ats_f = 'ats_20230411.xlsx'
    print('...titan report')
    df_ats = import_ats (ats_f)
    tnt_f = 'TITAN_RPT009.xlsx'
    df_tnt = import_titan (tnt_f)
    
    
    print('\nCreating Reports.')
    dfs = []
    
    print('...report 01')
    df_01 = create_rpt_01 (df_tnt,df_ats)
    dfs.append(df_01)
    
    print('...report 02')
    df_02 = create_rpt_02 (df_tnt,df_ats)
    dfs.append(df_02)
    
    print('...report 03')
    df_03 = create_rpt_03 (df_tnt,df_ats)
    dfs.append(df_03)
    
    print('...report 04')
    df_04 = create_rpt_04 (df_tnt,df_ats)
    dfs.append(df_04)
    
    print('...report 05')
    df_05 = create_rpt_05 (df_tnt,df_ats)
    dfs.append(df_05)
    
    print('...report 06')
    df_06 = create_rpt_06 (df_tnt,df_ats)
    dfs.append(df_06)
    
    print('...report 07')
    df_07 = create_rpt_07 (df_tnt,df_ats)
    dfs.append(df_07)
    
    print('...report 08')
    df_08 = create_rpt_08 (df_tnt,df_ats)
    dfs.append(df_08)
    
    print('...report 09')
    df_09 = create_rpt_09 (df_tnt,df_ats)
    dfs.append(df_09)
    
    print('...report 10')
    df_10 = create_rpt_10 (df_tnt,df_ats)
    dfs.append(df_10)
    
    print('\nFormatting Report columns')
    dfs_f = set_rpt_colums (df_ats, dfs)
    
    print('\nCreating a Summary Report')
    df_00, rpt_ids = create_summary_rpt (dfs_f)
    df_stats = compute_stats (dfs_f,df_00,rpt_ids)
    
    print('\nExporting the Final Report')
    dfs_f.insert(0, df_stats)
    rpt_ids.insert(0, 'Summary')
    
    today = date.today().strftime("%Y%m%d")
    filename = today + '_landFiles_tracker_betaVersion'
    
    compute_plot (df_stats,filename)
    create_report (dfs_f, rpt_ids,filename)

main()
