import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
#import datetime as dt


def get_titan_report_date (report):
    """ Returns the date of the input TITAN report"""
    df = pd.read_excel(report,'Info')
    titan_date = df.columns[1].strftime("%Y%m%d")
   
    return titan_date



def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection



def read_query(connection,query):
    "Returns a df containing SQL Query results"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        names = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=names)
    
    finally:
        if cursor is not None:
            cursor.close()
            


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
    workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20221024_fnQuery_template'
    
    print ('Connecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    #bcgw_user = 'XXXXXXXXXX'
    bcgw_pwd = os.getenv('bcgw_pwd')
    #bcgw_pwd = 'XXXXXXXXXX'
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ('Running the SQL Query...')
    
    sql = """
        SELECT pl.INTRID_SID, fn.CNSLTN_AREA_NAME, fn.CONTACT_ORGANIZATION_NAME
        
        FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW pl,
         WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP fn 
        
        WHERE pl.RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
          AND pl.TENURE_STAGE = 'TENURE'
          AND pl.TENURE_STATUS = 'DISPOSITION IN GOOD STANDING'
          AND fn.CONTACT_ORGANIZATION_NAME IN ('Ahousaht First Nation', 'Hesquiaht First Nation', 'Maa-nulth First Nations Final Agreement Areas', 
                                               'Mowachaht/Muchalaht First Nation', 'Tseshaht First Nation' ,'Tla-o-qui-aht First Nation')
                                  
          AND SDO_RELATE (pl.SHAPE, fn.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
    
          """
    df_fn = read_query(connection,sql)
    
    print('Reading TITAN report...')
    titan_rep = os.path.join (workspace, 'TITAN_RPT012.xlsx' )
    df_tn = pd.read_excel (titan_rep,'TITAN_RPT012', converters={'FILE #':str})
    
    print('Joining tables...')
    df = pd.merge (df_fn, df_tn, how='inner', 
                   left_on='INTRID_SID', right_on='INTEREST PARCEL ID')
    df.drop('INTRID_SID', axis=1, inplace=True)
    
    print ('Exporting the final report...')
    titan_date = get_titan_report_date (titan_rep)
    filename = 'Tenure_FN_overlap_report_{}'.format(titan_date)
    generate_report (workspace, [df], ['TENURE FN OVERLAP'],filename)
    
    print ('Processing Completed!')


main ()
