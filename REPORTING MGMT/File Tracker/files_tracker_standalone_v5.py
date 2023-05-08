
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
# Created:     2023-05-08
# Updated:
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
#import numpy as np
from datetime import date, datetime, timedelta


def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection


def import_ats_oh (ats_oh_f):
    df = pd.read_excel(ats_oh_f, skiprows=range(1,14))
    
    new_header = df.iloc[0] 
    df = df[1:] 
    df.columns = new_header 
    df['On Hold End Date']=''
    cols_onh = ['Project Number','On Hold Start Date', 
                'On Hold End Date','Reason For Hold']
    
    df = df[cols_onh]
    
    return df


def import_ats_bf (ats_bf_f):
    df = pd.read_excel(ats_bf_f, skiprows=range(1,13))
    
    new_header = df.iloc[0] 
    df = df[1:] 
    df.columns = new_header 
    cols_onh = ['Project Number','Authorization Assigned To', 
                'Bring Forward Date']
    
    df = df[cols_onh]
    
    return df


def import_ats_pt (ats_pt_f, df_onh,df_bfw):
    """Reads the ATS report into a df"""
    df = pd.read_excel(ats_pt_f)
    
    df['File Number'] = df['File Number'].fillna(0)
    df['File Number'] = df['File Number'].astype(str)
    
    df.rename(columns={'Comments': 'ATS Comments'}, inplace=True)
    
    df.loc[(df['Accepted Date'].isnull()) & 
       (df['Rejected Date'].isnull()) & 
       (df['Submission Review Complete Date'].notnull()),
       'Accepted Date'] = df['Submission Review Complete Date']
    
    df['Decision-making Office Name'].fillna(df['Intake Office Name'], inplace=True)
    df.loc[df['Authorization Type'].str.contains('Aquaculture'), 
           'Decision-making Office Name'] = 'Aquaculture'
    
    df['Decision-making Office Name'] = df['Decision-making Office Name'].str.upper()
    
    for index,row in df.iterrows():
        z_nbr = 7 - len(str(row['File Number']))
        df.loc[index, 'File Number'] = z_nbr * '0' + str(row['File Number'])
        
    #add on-hold cols
    df = pd.merge(df, df_onh, how='left', on='Project Number')
    
    #add bring-forward cols
    df = pd.merge(df, df_bfw, how='left', on='Project Number')
    
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


def calculate_metrics(df , grp_col, mtr_ids):
    """ Calculates Median and Mean metrics and return in df"""
    df_mtrs = []
    for mtr_id in mtr_ids:
        df_mtr = df.groupby(grp_col)[[mtr_id]].agg(['median', 'mean'])
        df_mtr.fillna(0, inplace=True)
        
        df_mtr.columns = [mtr_id+'_med',mtr_id+'_avg']
        
        df_mtr = df_mtr.reset_index()
        
        offices = ['AQUACULTURE','CAMPBELL RIVER','HAIDA GWAII',
                   'NANAIMO', 'PORT ALBERNI','PORT MCNEILL']
        
        if set(offices) != set(df_mtr['DISTRICT OFFICE'].unique()):
            new_rows = pd.DataFrame({'DISTRICT OFFICE': offices})
            df_mtr = pd.merge(new_rows, df_mtr, how='outer', on='DISTRICT OFFICE')
            df_mtr = df_mtr.fillna(0)
        else:
            df_mtr = df_mtr.sort_values(by='DISTRICT OFFICE')        
        
        
        df_mtr = pd.melt(df_mtr, id_vars=[grp_col])
        
        df_mtr = df_mtr.pivot_table(values='value', 
                                    index='variable', 
                                    columns=grp_col)
        
        vals = []
        for col in df_mtr.columns:
            vals.extend(df_mtr[col].to_list())
            
        mtr_cols = ['avg AQ','med AQ','avg CR','med CR',
                    'avg HG','med HG','avg NA','med NA',
                    'avg PA','med PA','avg PM','med PM']
        
        df_mtr = pd.DataFrame(data=[vals], columns=mtr_cols)

        df_mtr['avg WC'] = df.loc[df[mtr_id] != 0, mtr_id].mean()
        df_mtr['med WC'] = df.loc[df[mtr_id] != 0, mtr_id].median()

        df_mtr.fillna(0, inplace=True)        
        df_mtr = df_mtr.round().astype(int)
        
        df_mtr['METRIC ID'] = mtr_id
        
        df_mtrs.append(df_mtr)
    
    df_mtr = pd.concat(df_mtrs)
 
    
    return df_mtr



def create_rpt_01(df_tnt,df_ats):
    """ Creates Report 01- Files with FCBC"""
    ats_a = df_ats.loc[df_ats['Authorization Status'] == 'Active']
    #active = ats_a['File Number'].to_list()
    
    df_01= ats_a.loc[(ats_a['Received Date'].notnull()) &
                     (ats_a['Submission Review Complete Date'].isnull())]
    
    
                      
    df_01['tempo_join_date']= df_01['Accepted Date'].astype('datetime64[Y]')
    df_tnt['tempo_join_date']= df_tnt['CREATED DATE'].astype('datetime64[Y]')
    
    df_01 = pd.merge(df_01, df_tnt, how='left',
                     left_on=['File Number','tempo_join_date'],
                     right_on=['FILE NUMBER','tempo_join_date'])
    
    df_01= df_01.loc[(df_01['STATUS'].isnull())]
    
    df_01.sort_values(by=['Received Date'], ascending=False, inplace=True)
    df_01.reset_index(drop = True, inplace = True)
    
    df_01['DISTRICT OFFICE'] = df_01['Decision-making Office Name'] 
    
    #Calulcate metrics
    df_01_nw= df_01.loc[df_01['Authorization Type']!='Replacements']
    df_01_rp= df_01.loc[df_01['Authorization Type']=='Replacements']
    
    today = date.today()
    df_01['mtr01'] = (today - df_01['Received Date']).dt.days
    df_01_nw['mtr01'] = (today - df_01_nw['Received Date']).dt.days
    df_01_rp['mtr01'] = (today - df_01_rp['Received Date']).dt.days
    
    metrics = ['mtr01']
    df_01_mtr_nw = calculate_metrics(df_01_nw , 'DISTRICT OFFICE',metrics)
    df_01_mtr_rp = calculate_metrics(df_01_rp , 'DISTRICT OFFICE',metrics )
    

    return df_01,df_01_nw,df_01_rp,df_01_mtr_nw,df_01_mtr_rp


def create_rpt_02(df_tnt,df_ats):
    """ Creates Report 02- Files in Queue"""
    ats_r = df_ats.loc[df_ats['Authorization Status'].isin(['Closed', 'On Hold'])]
    notactive = ats_r['File Number'].to_list()
    
    df_02= df_tnt.loc[(df_tnt['TASK DESCRIPTION'].isin(['NEW APPLICATION','REPLACEMENT APPLICATION'])) &
                      (~df_tnt['FILE NUMBER'].isin(notactive)) &
                      (df_tnt['OTHER EMPLOYEES ASSIGNED TO'].str.contains('WCR_', na=False) | 
                       df_tnt['OTHER EMPLOYEES ASSIGNED TO'].isnull()) &
                      (df_tnt['STATUS'] == 'ACCEPTED')]
      
    df_02['tempo_join_date']= df_02['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_02 = pd.merge(df_02, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_02.sort_values(by=['CREATED DATE'], ascending=False, inplace=True)
    df_02.reset_index(drop = True, inplace = True)

    #Calulcate metrics
    df_02_nw= df_02.loc[df_02['TASK DESCRIPTION']=='NEW APPLICATION']
    df_02_rp= df_02.loc[df_02['TASK DESCRIPTION']=='REPLACEMENT APPLICATION']
    
    today = date.today()
    
    df_02['mtr02'] = (df_02['Submission Review Complete Date'] - df_02['Received Date']).dt.days
    df_02['mtr03'] = (today - df_02['Submission Review Complete Date']).dt.days

    df_02_nw['mtr02'] = (df_02_nw['Submission Review Complete Date'] - df_02_nw['Received Date']).dt.days
    df_02_nw['mtr03'] = (today - df_02_nw['Submission Review Complete Date']).dt.days

    df_02_rp['mtr02'] = (df_02_rp['Submission Review Complete Date'] - df_02_rp['Received Date']).dt.days
    df_02_rp['mtr03'] = (today - df_02_rp['Submission Review Complete Date']).dt.days
    
    metrics= ['mtr02','mtr03']
    df_02_mtr_nw = calculate_metrics(df_02_nw , 'DISTRICT OFFICE',metrics) 
    df_02_mtr_rp = calculate_metrics(df_02_rp , 'DISTRICT OFFICE',metrics) 
    
    return df_02,df_02_nw,df_02_rp,df_02_mtr_nw,df_02_mtr_rp


def create_rpt_03 (df_tnt,df_ats):
    """ Creates Report 03- Files in Active Review"""
    df_ats = df_ats.loc[df_ats['Authorization Status'].isin(['Active','Closed'])]
    
    df_03= df_tnt.loc[((~df_tnt['OTHER EMPLOYEES ASSIGNED TO'].str.contains('WCR_',na=False)) & 
                       (df_tnt['OTHER EMPLOYEES ASSIGNED TO'].notnull())) &
                      (df_tnt['REPORTED DATE'].isnull()) &
                      (df_tnt['TASK DESCRIPTION'].isin(['NEW APPLICATION', 'REPLACEMENT APPLICATION'])) &            
                      (df_tnt['STATUS'] == 'ACCEPTED')]

    df_03['tempo_join_date']= df_03['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_03 = pd.merge(df_03, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_03.sort_values(by=['CREATED DATE'], ascending=False, inplace=True)
    df_03.reset_index(drop = True, inplace = True)

    #Calulcate metrics
    today = pd.to_datetime(date.today())
    

    df_03['Bring Forward Date'] = pd.to_datetime(df_03['Bring Forward Date']
                                                 .fillna(pd.NaT), errors='coerce')
    df_03['Submission Review Complete Date'] = pd.to_datetime(df_03['Submission Review Complete Date']
                                                              .fillna(pd.NaT), errors='coerce')
    df_03['First Nation Start Date'] = pd.to_datetime(df_03['First Nation Start Date']
                                                      .fillna(pd.NaT), errors='coerce')
    df_03['First Nation Completion Date'] = pd.to_datetime(df_03['First Nation Completion Date']
                                                           .fillna(pd.NaT), errors='coerce')

    df_03_nw= df_03.loc[df_03['TASK DESCRIPTION']=='NEW APPLICATION']
    df_03_rp= df_03.loc[df_03['TASK DESCRIPTION']=='REPLACEMENT APPLICATION']    
    
    df_03['mtr04'] = (df_03['Bring Forward Date'] - df_03['Submission Review Complete Date']).dt.days
    df_03['mtr05'] = (today - df_03['First Nation Start Date']).dt.days
    df_03['mtr06'] = (df_03['First Nation Completion Date'] - df_03['First Nation Start Date']).dt.days
    df_03['mtr07'] = (today - df_03['Bring Forward Date']).dt.days

    df_03_nw['mtr04'] = (df_03_nw['Bring Forward Date'] - df_03_nw['Submission Review Complete Date']).dt.days
    df_03_nw['mtr05'] = (today - df_03_nw['First Nation Start Date']).dt.days
    df_03_nw['mtr06'] = (df_03_nw['First Nation Completion Date'] - df_03_nw['First Nation Start Date']).dt.days
    df_03_nw['mtr07'] = (today - df_03_nw['Bring Forward Date']).dt.days

    df_03_rp['mtr04'] = (df_03_rp['Bring Forward Date'] - df_03_rp['Submission Review Complete Date']).dt.days
    df_03_rp['mtr05'] = (today - df_03_rp['First Nation Start Date']).dt.days
    df_03_rp['mtr06'] = (df_03_rp['First Nation Completion Date'] - df_03_rp['First Nation Start Date']).dt.days
    df_03_rp['mtr07'] = (today - df_03_rp['Bring Forward Date']).dt.days
    
    metrics= ['mtr04','mtr05','mtr06','mtr07']
    df_03_mtr_nw = calculate_metrics(df_03_nw , 'DISTRICT OFFICE', metrics ) 
    df_03_mtr_rp = calculate_metrics(df_03_rp , 'DISTRICT OFFICE', metrics ) 


    return df_03,df_03_nw,df_03_rp,df_03_mtr_nw,df_03_mtr_rp


def create_rpt_04 (df_tnt,df_ats):
    """ Creates Report 04- Files Awaiting Decision"""
    df_ats = df_ats.loc[df_ats['Authorization Status'].isin(['Active','Closed'])]
    
    df_04= df_tnt.loc[(df_tnt['REPORTED DATE'].notnull()) &
                    (df_tnt['ADJUDICATED DATE'].isnull()) &
                    (df_tnt['TASK DESCRIPTION'].isin(['NEW APPLICATION', 'REPLACEMENT APPLICATION'])) &
                    (df_tnt['STATUS'] == 'ACCEPTED')]

    df_04['tempo_join_date']= df_04['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_04 = pd.merge(df_04, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_04.sort_values(by=['REPORTED DATE'], ascending=False, inplace=True)
    df_04.reset_index(drop = True, inplace = True)

    #Calulcate metrics
    today = pd.to_datetime(date.today())

    df_04['Bring Forward Date'] = pd.to_datetime(df_04['Bring Forward Date']
                                                 .fillna(pd.NaT), errors='coerce')

    df_04['REPORTED DATE'] = pd.to_datetime(df_04['REPORTED DATE']
                                                 .fillna(pd.NaT), errors='coerce') 
    
    df_04_nw= df_04.loc[df_04['TASK DESCRIPTION']=='NEW APPLICATION']
    df_04_rp= df_04.loc[df_04['TASK DESCRIPTION']=='REPLACEMENT APPLICATION']        
    
    df_04['mtr08'] = (df_04['Bring Forward Date'] - df_04['REPORTED DATE']).dt.days
    df_04['mtr09'] = (today - df_04['REPORTED DATE']).dt.days

    df_04_nw['mtr08'] = (df_04_nw['Bring Forward Date'] - df_04_nw['REPORTED DATE']).dt.days
    df_04_nw['mtr09'] = (today - df_04_nw['REPORTED DATE']).dt.days

    df_04_rp['mtr08'] = (df_04_rp['Bring Forward Date'] - df_04_rp['REPORTED DATE']).dt.days
    df_04_rp['mtr09'] = (today - df_04_rp['REPORTED DATE']).dt.days
    
    metrics= ['mtr08','mtr09']
    df_04_mtr_nw = calculate_metrics(df_04_nw , 'DISTRICT OFFICE', metrics )  
    df_04_mtr_rp = calculate_metrics(df_04_rp , 'DISTRICT OFFICE', metrics ) 
    
    return df_04,df_04_nw,df_04_rp,df_04_mtr_nw,df_04_mtr_rp


def create_rpt_05 (df_tnt,df_ats):
    """ Creates Report 05- Files Awaiting Offer"""
    df_ats = df_ats.loc[df_ats['Authorization Status'].isin(['Active','Closed'])]
    
    df_05= df_tnt.loc[(df_tnt['ADJUDICATED DATE'].notnull()) &
                     (df_tnt['OFFERED DATE'].isnull()) &
                     (df_tnt['TASK DESCRIPTION'].isin(['NEW APPLICATION', 'REPLACEMENT APPLICATION'])) &            
                     (df_tnt['STATUS'] == 'ACCEPTED')]

    df_05['tempo_join_date']= df_05['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_05 = pd.merge(df_05, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_05.sort_values(by=['ADJUDICATED DATE'], ascending=False, inplace=True)
    df_05.reset_index(drop = True, inplace = True)

    #Calulcate metrics
    df_05_nw= df_05.loc[df_05['TASK DESCRIPTION']=='NEW APPLICATION']
    df_05_rp= df_05.loc[df_05['TASK DESCRIPTION']=='REPLACEMENT APPLICATION']  
    
    today = date.today()

    df_05['mtr10'] = (df_05['ADJUDICATED DATE'] - df_05['REPORTED DATE']).dt.days
    df_05['mtr11'] = (today - df_05['ADJUDICATED DATE']).dt.days

    df_05_nw['mtr10'] = (df_05_nw['ADJUDICATED DATE'] - df_05_nw['REPORTED DATE']).dt.days
    df_05_nw['mtr11'] = (today - df_05_nw['ADJUDICATED DATE']).dt.days
    
    df_05_rp['mtr10'] = (df_05_rp['ADJUDICATED DATE'] - df_05_rp['REPORTED DATE']).dt.days
    df_05_rp['mtr11'] = (today - df_05_rp['ADJUDICATED DATE']).dt.days
    
    metrics= ['mtr10','mtr11']
    df_05_mtr_nw = calculate_metrics(df_05_nw , 'DISTRICT OFFICE', metrics )  
    df_05_mtr_rp = calculate_metrics(df_05_rp , 'DISTRICT OFFICE', metrics )  
    
    return df_05,df_05_nw,df_05_rp,df_05_mtr_nw,df_05_mtr_rp


def create_rpt_06 (df_tnt,df_ats):
    """ Creates Report 06- Files awaiting Offer Acceptance"""
    df_ats = df_ats.loc[df_ats['Authorization Status'].isin(['Active','Closed'])]
    df_06= df_tnt.loc[(df_tnt['OFFERED DATE'].notnull()) &
                      (df_tnt['OFFER ACCEPTED DATE'].isnull())&
                      (df_tnt['TASK DESCRIPTION'].isin(['NEW APPLICATION', 'REPLACEMENT APPLICATION'])) &                      
                      (df_tnt['STATUS'] == 'OFFERED')]
    
    df_06['tempo_join_date']= df_06['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_06 = pd.merge(df_06, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_06.sort_values(by=['OFFERED DATE'], ascending=False, inplace=True)
    df_06.reset_index(drop = True, inplace = True)

    #Calulcate metrics
    df_06_nw= df_06.loc[df_06['TASK DESCRIPTION']=='NEW APPLICATION']
    df_06_rp= df_06.loc[df_06['TASK DESCRIPTION']=='REPLACEMENT APPLICATION']  
    
    today = date.today()

    df_06['mtr12'] = (df_06['OFFERED DATE'] - df_06['ADJUDICATED DATE']).dt.days
    df_06['mtr13'] = (today - df_06['OFFERED DATE']).dt.days

    df_06_nw['mtr12'] = (df_06_nw['OFFERED DATE'] - df_06_nw['ADJUDICATED DATE']).dt.days
    df_06_nw['mtr13'] = (today - df_06_nw['OFFERED DATE']).dt.days
    
    df_06_rp['mtr12'] = (df_06_rp['OFFERED DATE'] - df_06_rp['ADJUDICATED DATE']).dt.days
    df_06_rp['mtr13'] = (today - df_06_rp['OFFERED DATE']).dt.days
    
    metrics= ['mtr12','mtr13']
    df_06_mtr_nw = calculate_metrics(df_06_nw , 'DISTRICT OFFICE', metrics )  
    df_06_mtr_rp = calculate_metrics(df_06_rp , 'DISTRICT OFFICE', metrics )  
    
    return df_06,df_06_nw,df_06_rp,df_06_mtr_nw,df_06_mtr_rp


def create_rpt_07 (df_tnt,df_ats):
    """ Creates Report 07- Files with Offer Accepted"""
    df_ats = df_ats.loc[df_ats['Authorization Status'].isin(['Active','Closed'])]
    df_07= df_tnt.loc[(df_tnt['OFFER ACCEPTED DATE'].notnull()) &
                     (df_tnt['TASK DESCRIPTION'].isin(['NEW APPLICATION', 'REPLACEMENT APPLICATION'])) &                      
                      (df_tnt['STATUS'] == 'OFFER ACCEPTED')]
    
    df_07['tempo_join_date']= df_07['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_07 = pd.merge(df_07, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_07.sort_values(by=['OFFER ACCEPTED DATE'], ascending=False, inplace=True)
    df_07.reset_index(drop = True, inplace = True)

    #Calulcate metrics
    df_07_nw= df_07.loc[df_07['TASK DESCRIPTION']=='NEW APPLICATION']
    df_07_rp= df_07.loc[df_07['TASK DESCRIPTION']=='REPLACEMENT APPLICATION']  
    
    today = date.today()

    df_07['mtr14'] = (df_07['OFFER ACCEPTED DATE'] - df_07['OFFERED DATE']).dt.days
    df_07['mtr15'] = (today - df_07['OFFER ACCEPTED DATE']).dt.days

    df_07_nw['mtr14'] = (df_07_nw['OFFER ACCEPTED DATE'] - df_07_nw['OFFERED DATE']).dt.days
    df_07_nw['mtr15'] = (today - df_07_nw['OFFER ACCEPTED DATE']).dt.days
    
    df_07_rp['mtr14'] = (df_07_rp['OFFER ACCEPTED DATE'] - df_07_rp['OFFERED DATE']).dt.days
    df_07_rp['mtr15'] = (today - df_07_rp['OFFER ACCEPTED DATE']).dt.days
    
    metrics= ['mtr14','mtr15']
    df_07_mtr_nw = calculate_metrics(df_07_nw , 'DISTRICT OFFICE', metrics )  
    df_07_mtr_rp = calculate_metrics(df_07_rp , 'DISTRICT OFFICE', metrics )  
    
    return df_07,df_07_nw,df_07_rp,df_07_mtr_nw,df_07_mtr_rp


def create_rpt_08 (df_tnt,df_ats):
    """ Creates Report 08- Files Completed"""
    df_ats = df_ats.loc[df_ats['Authorization Status'].isin(['Active','Closed'])]
    
    date_30_days_ago = datetime.now() - timedelta(days=30)
    date_30_days_ago = date_30_days_ago.date()
    
    df_08= df_tnt.loc[(df_tnt['COMPLETED DATE'].notnull()) &
                      (df_tnt['COMPLETED DATE'] > date_30_days_ago) &
                      (df_tnt['TASK DESCRIPTION'].isin(['NEW APPLICATION', 'REPLACEMENT APPLICATION'])) &
                      (df_tnt['STATUS'] == 'DISPOSITION IN GOOD STANDING')]
    
    df_08['tempo_join_date']= df_08['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_08 = pd.merge(df_08, df_ats, how='left',
                     left_on=['FILE NUMBER','tempo_join_date'],
                     right_on=['File Number','tempo_join_date'])
    
    df_08.sort_values(by=['COMPLETED DATE'], ascending=False, inplace=True)
    df_08.reset_index(drop = True, inplace = True)

    #Calulcate metrics
    df_08_nw= df_08.loc[df_08['TASK DESCRIPTION']=='NEW APPLICATION']
    df_08_rp= df_08.loc[df_08['TASK DESCRIPTION']=='REPLACEMENT APPLICATION'] 
    
    #today = date.today()

    df_08['mtr16'] = (df_08['COMPLETED DATE'] - df_08['ADJUDICATED DATE']).dt.days
    df_08['mtr17'] = (df_08['COMPLETED DATE'] - df_08['RECEIVED DATE']).dt.days

    df_08_nw['mtr16'] = (df_08_nw['COMPLETED DATE'] - df_08_nw['ADJUDICATED DATE']).dt.days
    df_08_nw['mtr17'] = (df_08_nw['COMPLETED DATE'] - df_08_nw['RECEIVED DATE']).dt.days

    df_08_rp['mtr16'] = (df_08_rp['COMPLETED DATE'] - df_08_rp['ADJUDICATED DATE']).dt.days
    df_08_rp['mtr17'] = (df_08_rp['COMPLETED DATE'] - df_08_rp['RECEIVED DATE']).dt.days
    
    metrics= ['mtr16','mtr17']
    df_08_mtr_nw = calculate_metrics(df_08_nw , 'DISTRICT OFFICE', metrics ) 
    df_08_mtr_rp = calculate_metrics(df_08_rp , 'DISTRICT OFFICE', metrics )  

    
    return df_08,df_08_nw,df_08_rp,df_08_mtr_nw,df_08_mtr_rp


def create_rpt_09 (df_tnt,df_ats):
    """ Creates Report 09 - Files On Hold"""
    df_ats = df_ats.loc[(df_ats['Authorization Status']== 'On Hold') &
                        (df_ats['Accepted Date'].notnull())]
    hold_l = df_ats['File Number'].to_list()
    
    df_09= df_tnt.loc[(df_tnt['STATUS'] == 'ACCEPTED') & 
                      (df_tnt['FILE NUMBER'].isin(hold_l))]

    df_09['tempo_join_date']= df_09['CREATED DATE'].astype('datetime64[Y]')
    df_ats['tempo_join_date']= df_ats['Accepted Date'].astype('datetime64[Y]')
    
    df_09 = pd.merge(df_09, df_ats, how='left',
                     left_on=['FILE NUMBER'],
                     right_on=['File Number'])
    
    df_09.sort_values(by=['CREATED DATE'], ascending=False, inplace=True)
    df_09.reset_index(drop = True, inplace = True)

    #Calulcate metrics
    today = pd.to_datetime(date.today())

    df_09['On Hold Start Date'] = pd.to_datetime(df_09['On Hold Start Date']
                                                 .fillna(pd.NaT), errors='coerce')

    df_09['On Hold End Date'] = pd.to_datetime(df_09['On Hold End Date']
                                                 .fillna(pd.NaT), errors='coerce') 

    df_09_nw= df_09.loc[df_09['TASK DESCRIPTION']=='NEW APPLICATION']
    df_09_rp= df_09.loc[df_09['TASK DESCRIPTION']=='REPLACEMENT APPLICATION'] 
    
    df_09['mtr18'] = (today - df_09['On Hold Start Date']).dt.days
    df_09['mtr19'] = (df_09['On Hold End Date'] - df_09['On Hold Start Date']).dt.days

    df_09_nw['mtr18'] = (today - df_09_nw['On Hold Start Date']).dt.days
    df_09_nw['mtr19'] = (df_09_nw['On Hold End Date'] - df_09_nw['On Hold Start Date']).dt.days

    df_09_rp['mtr18'] = (today - df_09_rp['On Hold Start Date']).dt.days
    df_09_rp['mtr19'] = (df_09_rp['On Hold End Date'] - df_09_rp['On Hold Start Date']).dt.days
    
    metrics= ['mtr18','mtr19']
    df_09_mtr_nw = calculate_metrics(df_09_nw , 'DISTRICT OFFICE', metrics )  
    df_09_mtr_rp = calculate_metrics(df_09_rp , 'DISTRICT OFFICE', metrics ) 
    
    return df_09,df_09_nw,df_09_rp,df_09_mtr_nw,df_09_mtr_rp


def set_rpt_colums (dfs):
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
         'Authorization Assigned To',
         'OTHER EMPLOYEES ASSIGNED TO',
         'USERID ASSIGNED TO',
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
         'Bring Forward Date',
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
         'On Hold Start Date',
         'On Hold End Date',
         'Reason For Hold',
         'Total On Hold Time',
         'Net Processing Time',
         'COMPLETED DATE',
         'CLIENT NAME',
         'LOCATION',
         'TANTALIS COMMENTS',
         'ATS Comments']

    
    df_rpts = []   
    
    for df in dfs:
        for col in cols:
            if col not in df.columns:
                df[col] = pd.Series(dtype='object')
        
        df = df[cols+df.filter(regex='^mtr').columns.tolist()]
        df['Region Name'] = 'WEST COAST'
        df['Business Area'] = 'LANDS'

        df.rename({'Authorization Status': 'ATS STATUS', 
                   'STATUS': 'TANTALIS STATUS',

                   'TASK DESCRIPTION': 'APPLICATION TYPE',
                   'OTHER EMPLOYEES ASSIGNED TO':'FIELD EMPLOYEE',
                   'USERID ASSIGNED TO': 'EXAMINER NAME',
                   'Received Date': 'ATS RECEIVED DATE',
                   'RECEIVED DATE': 'TANTALIS RECEIVED DATE'}, 
                  axis=1, inplace=True)

        df.columns = [x.upper() for x in df.columns]
        
        df_rpts.append (df)
    
    return df_rpts
        

def create_summary_rpt (df_rpts):
    """Creates a summary  -Nbr of Files"""
    rpt_ids = ['rpt01','rpt02','rpt03','rpt04',
               'rpt05','rpt06','rpt07','rpt08','rpt09']
    
    df_grs = []
    for df in df_rpts:
        df_gr = df.groupby('DISTRICT OFFICE')['REGION NAME'].count().reset_index()
        df_gr.sort_values(by=['DISTRICT OFFICE'], inplace = True)
        df_gr_pv = pd.pivot_table(df_gr, values='REGION NAME',
                        columns=['DISTRICT OFFICE'], fill_value=0)
        df_grs.append (df_gr_pv)
    
    df_sum_rpt = pd.concat(df_grs).reset_index(drop=True)
    df_sum_rpt.fillna(0, inplace=True)
    
    df_sum_rpt['files WC'] = df_sum_rpt.sum(axis=1)
    
    df_sum_rpt['REPORT ID'] = rpt_ids
    
    df_sum_rpt.rename({'AQUACULTURE': 'files AQ',
                       'CAMPBELL RIVER': 'files CR',
                       'HAIDA GWAII':'files HG',
                       'NANAIMO': 'files NA',
                       'PORT ALBERNI': 'files PA',
                       'PORT MCNEILL': 'files PM'}, axis=1, inplace=True)
    
    return df_sum_rpt,rpt_ids



def create_summary_mtr(df_mtrs):
    """Creates a summary- Nbr of Days"""


    df_sum_mtr = pd.concat(df_mtrs)
    df_sum_mtr = df_sum_mtr.reset_index(drop=True)
    

    return df_sum_mtr


def create_summary_all(template,df_sum_rpt,df_sum_mtr):
    """Create a Summary of Nbr files and days"""
    df_tmp = pd.read_excel(template)
    
    df_sum_all = pd.merge(df_tmp,df_sum_rpt,
                          how='left',
                          on='REPORT ID')
    
    df_sum_all = pd.merge(df_sum_all,df_sum_mtr,
                          how='left',
                          on='METRIC ID')

    
    sum_cols = ['REPORT ID', 'REPORT NAME', 'METRIC ID', 'METRIC NAME', 'files WC',
                'avg WC', 'med WC', 'files AQ','avg AQ', 'med AQ','files CR','avg CR', 
                'med CR','files HG','avg HG','med HG','files NA','avg NA', 'med NA',
                'files PA','avg PA', 'med PA','files PM','avg PM', 'med PM']
        
    df_sum_all= df_sum_all[sum_cols]  
    
    return df_sum_all
    

def compute_plot_rpt (df_stats,filename):
    """Computes a barplot of number of nbr applications per rpt_id and office """
    df_pl = df_stats[['REPORT ID','AQUACULTURE', 'CAMPBELL RIVER', 
                      'NANAIMO', 'PORT ALBERNI','PORT MCNEILL', 'HAIDA GWAII']]
    
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

        col_names = [{'header': col_name} for col_name in dataframe.columns[:]]

        worksheet.add_table(0, 0, dataframe.shape[0], dataframe.shape[1]-1, {
            'columns': col_names})

    writer.save()
    writer.close()    



    
#def main():
    
print ('\nConnecting to BCGW.')
hostname = 'bcgw.bcgov/idwprod1.bcgov'
bcgw_user = os.getenv('bcgw_user')
bcgw_pwd = os.getenv('bcgw_pwd')
#connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)


print ('\nReading Input files')

print('...titan report')
tnt_f = 'TITAN_RPT009.xlsx'
df_tnt = import_titan (tnt_f)

print ('...ats report: on-hold')
ats_oh_f = '20230502_ats_onh.xlsx'
df_onh= import_ats_oh (ats_oh_f)

print ('...ats report: bring-forward')
ats_bf_f = '20230502_ats_bfw.xlsx'
df_bfw= import_ats_bf (ats_bf_f)

print('...ats report: processing time')
ats_pt_f = '20230502_ats_pt.xlsx'
df_ats = import_ats_pt (ats_pt_f, df_onh,df_bfw)

print('\nCreating Reports.')
dfs = []
dfs_nw = []
dfs_rp = []
df_mtrs_nw = []
df_mtrs_rp = []
    
print('...report 01')
df_01,df_01_nw,df_01_rp,df_01_mtr_nw,df_01_mtr_rp= create_rpt_01 (df_tnt,df_ats)
dfs.append(df_01)
dfs_nw.append(df_01_nw)
dfs_rp.append(df_01_rp)
df_mtrs_nw.append(df_01_mtr_nw)
df_mtrs_rp.append(df_01_mtr_rp)

print('...report 02')
df_02,df_02_nw,df_02_rp,df_02_mtr_nw,df_02_mtr_rp = create_rpt_02 (df_tnt,df_ats)
dfs.append(df_02)
dfs_nw.append(df_02_nw)
dfs_rp.append(df_02_rp)
df_mtrs_nw.append(df_02_mtr_nw)
df_mtrs_rp.append(df_02_mtr_rp)


print('...report 03')
df_03,df_03_nw,df_03_rp,df_03_mtr_nw,df_03_mtr_rp = create_rpt_03 (df_tnt,df_ats)
dfs.append(df_03)
dfs_nw.append(df_03_nw)
dfs_rp.append(df_03_rp)
df_mtrs_nw.append(df_03_mtr_nw)
df_mtrs_rp.append(df_03_mtr_rp)

print('...report 04')
df_04,df_04_nw,df_04_rp,df_04_mtr_nw,df_04_mtr_rp = create_rpt_04(df_tnt,df_ats)
dfs.append(df_04)
dfs_nw.append(df_04_nw)
dfs_rp.append(df_04_rp)
df_mtrs_nw.append(df_04_mtr_nw)
df_mtrs_rp.append(df_04_mtr_rp)

print('...report 05')
df_05,df_05_nw,df_05_rp,df_05_mtr_nw,df_05_mtr_rp = create_rpt_05 (df_tnt,df_ats)
dfs.append(df_05)
dfs_nw.append(df_05_nw)
dfs_rp.append(df_05_rp)
df_mtrs_nw.append(df_05_mtr_nw)
df_mtrs_rp.append(df_05_mtr_rp)

print('...report 06')
df_06,df_06_nw,df_06_rp,df_06_mtr_nw,df_06_mtr_rp = create_rpt_06 (df_tnt,df_ats)
dfs.append(df_06)
dfs_nw.append(df_06_nw)
dfs_rp.append(df_06_rp)
df_mtrs_nw.append(df_06_mtr_nw)
df_mtrs_rp.append(df_06_mtr_rp)

print('...report 07')
df_07,df_07_nw,df_07_rp,df_07_mtr_nw,df_07_mtr_rp = create_rpt_07 (df_tnt,df_ats)
dfs.append(df_07)
dfs_nw.append(df_07_nw)
dfs_rp.append(df_07_rp)
df_mtrs_nw.append(df_07_mtr_nw)
df_mtrs_rp.append(df_07_mtr_rp)

print('...report 08')
df_08,df_08_nw,df_08_rp,df_08_mtr_nw,df_08_mtr_rp= create_rpt_08 (df_tnt,df_ats)
dfs.append(df_08)
dfs_nw.append(df_08_nw)
dfs_rp.append(df_08_rp)
df_mtrs_nw.append(df_08_mtr_nw)
df_mtrs_rp.append(df_08_mtr_rp)

print('...report 09')
df_09,df_09_nw,df_09_rp,df_09_mtr_nw,df_09_mtr_rp = create_rpt_09 (df_tnt,df_ats)
dfs.append(df_09)
dfs_nw.append(df_09_nw)
dfs_rp.append(df_09_rp)
df_mtrs_nw.append(df_09_mtr_nw)
df_mtrs_rp.append(df_09_mtr_rp)

print('\nFormatting Report columns')
df_rpts = set_rpt_colums (dfs)
df_rpts_nw = set_rpt_colums (dfs_nw)
df_rpts_rp = set_rpt_colums (dfs_rp)

print('\nCreating a Summary page')
df_sum_rpt_nw,rpt_ids = create_summary_rpt (df_rpts_nw)
df_sum_rpt_rp,rpt_ids = create_summary_rpt (df_rpts_rp)

df_sum_mtr_nw= create_summary_mtr(df_mtrs_nw)
df_sum_mtr_rp= create_summary_mtr(df_mtrs_rp)

template = 'TEMPLATE/rpt_template.xlsx'
df_sum_all_nw= create_summary_all(template,df_sum_rpt_nw,df_sum_mtr_nw)
df_sum_all_rp= create_summary_all(template,df_sum_rpt_rp,df_sum_mtr_rp)


print('\nExporting the Final Report')
df_list = [df_sum_all_nw,df_sum_all_rp] + df_rpts 
sheet_list = ['Summary - NEW Applics','Summary - REP Applics'] + rpt_ids


today = date.today().strftime("%Y%m%d")
filename = today + '_landFiles_tracker_betaVersion'

#compute_plot_rpt (df_stats,filename)
create_report (df_list, sheet_list,filename)

#main()
