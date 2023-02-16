import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd


def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection



def prep_data(f_eug,f_new):
    df_eug = pd.read_excel(f_eug, 'Existing Use Applications2', usecols="A:AG")
    df_new = pd.read_excel(f_new, 'Active Applications',converters={'File Number':str})
    

    types= ['Water Licence - Ground','Water Licence',
            'Water Licence - Surface','Water Licence - Ground / Surface']
    
    df_new = df_new.loc[df_new['Application Type'].isin(types)]
    
    df_new.columns = map(str.upper, df_new.columns)
    
    df_eug.rename(columns={'FILE_NO': 'FILE NUMBER', 
                           'ATS_PROJECT': 'ATS NUMBER'}, inplace=True)
    
    df_eug ['APPLICATION TYPE'] = 'Existing Use - Groundwater'
    df_new ['ATS NUMBER'] = ''
    
    cols = ['APPLICATION TYPE','FILE NUMBER','ATS NUMBER', 
            'APPLICANT','PURPOSE','STATUS','LATITUDE', 'LONGITUDE']
    
    df_eug = df_eug[cols]
    df_new = df_new[cols]
    
    df = pd.concat([df_new,df_eug])
    df.reset_index(drop=True, inplace=True)
    
    df ['DECISION TIMEFRAME'] = ''
    df ['WITHIN_KFN'] = 'NO'
    
    df.dropna(subset=['LATITUDE', 'LONGITUDE'], inplace=True)
    
    df.columns = df.columns.str.replace(' ', '_')
    
    return df


def add_kfn_info(df,connection,sql):
    for index, row in df.iterrows():
        print('...working on row {}'.format(index))
        long = row['LONGITUDE']
        lat = row['LATITUDE']
        
        query = sql.format(lat=lat,long=long)
        df_s = pd.read_sql(query,connection)
        
        if df_s.shape[0] > 0:
            df.at[index,'WITHIN_KFN'] = 'YES'
        else:
            pass
        
    return df


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
    print ('Preparing Input dataset')
    f_eug = 'workingCopy_Existing_Use_Groundwater.xlsx'
    f_new = 'workingCopy_Water Application Ledger.xlsx'
    df = prep_data(f_eug,f_new)
    
    print ('Connecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    #bcgw_user = 'XXXX'
    bcgw_pwd = os.getenv('bcgw_pwd')
    #bcgw_pwd = 'XXXX'
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    sql = """
    SELECT CNSLTN_AREA_NAME
    
    FROM WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
    
    WHERE pip.CNSLTN_AREA_NAME = q'[K'omoks First Nation]'
          AND SDO_RELATE (pip.SHAPE, SDO_GEOMETRY('POINT({long} {lat})', 4326),
                          'mask=ANYINTERACT') = 'TRUE'
    """
    
    print ('Adding KFN info')
    df = add_kfn_info(df,connection,sql)
    
    print ('Export results')
    df = df.loc[df['WITHIN_KFN']== 'YES']
    
    df['UNIQUE_ID'] = df['FILE_NUMBER']
    df['UNIQUE_ID'].fillna(df['ATS_NUMBER'], inplace=True)
    df = df[ ['UNIQUE_ID'] + [ col for col in df.columns if col != 'UNIQUE_ID' ]]
    
    create_report ([df], ['Water Applics - KFN territory'],'20230215_waterApplics_KFN')

main()

