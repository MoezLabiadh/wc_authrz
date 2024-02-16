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


def read_input_file (f):
    df = pd.read_excel(f)

    for index,row in df.iterrows():
        z_nbr = 7 - len(str(row['Landfile #']))
        df.loc[index, 'Landfile #'] = z_nbr * '0' + str(row['Landfile #'])
    
    return df


def load_sql ():
    sql = {}
    sql['aqa'] = """
                SELECT cl.CROWN_LANDS_FILE, cl.TENURE_STAGE, cl.TENURE_SUBTYPE, cl.TENURE_SUBPURPOSE, 
                       round(SDO_GEOM.SDO_AREA(cl.SHAPE, 0.005, 'unit=HECTARE'),2) AS AREA_HECTARE
                
                FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW cl
                
                WHERE cl.CROWN_LANDS_FILE IN ({})
                
                ORDER BY cl.CROWN_LANDS_FILE, cl.TENURE_STAGE
                """

    
    return sql


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
    print ('Connecting to BCGW')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    #bcgw_user = 'XXXX'
    bcgw_pwd = os.getenv('bcgw_pwd')
    #bcgw_pwd = 'XXXX'
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    f= '2023-03-06 SGA list for Moez for tenure size.xlsx'
    
    df = read_input_file (f)
    
    files = ",".join("'" + str(x) + "'" for x in df['Landfile #'].to_list())
    
    print ('Loading Queries')
    sql= load_sql()
    
    print ('Executing  Queries')
    query = sql['aqa'].format(files)
    df_q = pd.read_sql(query,connection)
    
    df_q.drop_duplicates(subset=['CROWN_LANDS_FILE'], keep='first', inplace=True)
    
    df = pd.merge(df,df_q,left_on='Landfile #', right_on='CROWN_LANDS_FILE')
    
    df.drop('CROWN_LANDS_FILE', axis=1, inplace=True)
    
    print ('Summarizing results')
    df_sum = df.groupby('Membership Name').sum().reset_index()
    
    print ('Exporting report')
    dfs = [df,df_sum]
    sheets = ['Tenure Areas', 'Area per Tenure Holder']
    
    create_report (dfs, sheets,'20230307_aquaTenures_area_stats')

main()
