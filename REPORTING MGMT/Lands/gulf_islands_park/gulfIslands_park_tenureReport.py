import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
from datetime import date
from load_sqls import load_queries


def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        cursor = connection.cursor()
        print  ("...Successffuly connected to the database")
    except:
        raise Exception('...Connection failed! Please verifiy your login parameters')

    return connection, cursor




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
    
    
if __name__==__name__:
    
    print ('\nConnecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection, cursor = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ("\nRunning SQL queries...")
    sql = load_queries ()
    
    dfs=[]
    sheets= []
    
    nbr_queries= len(sql)
    counter= 1
    for k, v in sql.items():
        print(f"....running query {counter} of {nbr_queries}: {k}")

        df= pd.read_sql(sql[k], connection)
        
        for col in df.columns:
            if 'DATE' in col:
                df[col] =  pd.to_datetime(df[col], infer_datetime_format=True, errors = 'coerce').dt.date
            
        dfs.append(df)
        sheets.append(k)
        
        counter+= 1
    
    print ("\nExporting the report...")
    wks= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20231128_gulfIslands_query_lance'
    
    today = date.today().strftime('%Y%m%d')
    filename= today+'_gulfIlands_park_tenureReport'
    
    generate_report (wks, dfs, sheets, filename)
    
