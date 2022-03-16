import os
import cx_Oracle
import pandas as pd


def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


def get_queries ():
    """returns the sql queries to run buy the tool"""
    sql= {}
    sql['DIG'] = """
                       SELECT*

                       FROM WHSE_TANTALIS.TA_CROWN_TENURES_VW ten
                       
                       WHERE ten.RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
                         AND ten.TENURE_PURPOSE = 'AQUACULTURE'
                         AND ten.TENURE_STAGE = 'TENURE'
                         AND ten.TENURE_STATUS = 'DISPOSITION IN GOOD STANDING'
                 """
                 
    sql['NEW'] = """
                  SELECT*
                
                  FROM WHSE_TANTALIS.TA_CROWN_TENURES_VW ten
                  
                  WHERE ten.RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
                    AND ten.TENURE_PURPOSE = 'AQUACULTURE'
                    AND ten.TENURE_STAGE = 'APPLICATION'
                    AND ten.TENURE_STATUS = 'ACCEPTED'
                    AND ten.APPLICATION_TYPE_CDE = 'NEW'
                """

    sql['REP'] = """
            SELECT*
            
            FROM WHSE_TANTALIS.TA_CROWN_TENURES_VW ten
            
            WHERE ten.RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
              AND ten.TENURE_PURPOSE = 'AQUACULTURE'
              AND ten.TENURE_STAGE = 'APPLICATION'
              AND ten.TENURE_STATUS = 'ACCEPTED'
              AND ten.APPLICATION_TYPE_CDE = 'REP'
              AND ten.CROWN_LANDS_FILE IN (
                                           SELECT ten.CROWN_LANDS_FILE
            
                                          FROM WHSE_TANTALIS.TA_CROWN_TENURES_VW ten
            
                                          WHERE ten.RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
                                            AND ten.TENURE_PURPOSE = 'AQUACULTURE'
                                            AND ten.TENURE_STAGE = 'TENURE'
                                            AND ten.TENURE_STATUS = 'DISPOSITION IN GOOD STANDING'
                                          )
         """

    sql['EXP'] = """
            SELECT*
            
            FROM WHSE_TANTALIS.TA_CROWN_TENURES_VW ten
            
            WHERE ten.RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
              AND ten.TENURE_PURPOSE = 'AQUACULTURE'
              AND ten.TENURE_STAGE = 'APPLICATION'
              AND ten.TENURE_STATUS = 'ACCEPTED'
              AND ten.APPLICATION_TYPE_CDE = 'REP'
              AND ten.CROWN_LANDS_FILE NOT IN (
                                           SELECT ten.CROWN_LANDS_FILE
            
                                          FROM WHSE_TANTALIS.TA_CROWN_TENURES_VW ten
            
                                          WHERE ten.RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
                                            AND ten.TENURE_PURPOSE = 'AQUACULTURE'
                                            AND ten.TENURE_STAGE = 'TENURE'
                                            AND ten.TENURE_STATUS = 'DISPOSITION IN GOOD STANDING'
                                          )
         """
         
         
    return sql
         
         
def read_query(connection,query):
    "Returns a df containing results of SQL Query "
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        names = [ x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=names)
    
    finally:
        if cursor is not None:
            cursor.close()
  
            
def generate_report (workspace, df_list, sheet_list, filename):
    """ Exports dataframes to multi-tab excel spreasheet"""
    file_name = os.path.join(workspace, str(filename) + '.xlsx')

    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]
        #workbook = writer.book

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'count'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    #writer.close()
            
def main ():
    workspace = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20220316_aquaList_Amber'
    titan_report = os.path.join(workspace, 'TITAN_RPT012.xlsx')
    df_ti = pd.read_excel (titan_report,'TITAN_RPT012', converters={'FILE #':str})
    
    print ("Connect to BCGW")    
    bcgw_host = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,bcgw_host)
    
    print ("Execute SQL Query")
    sql = get_queries ()
    
    cols = ['FILE #', 'DTID',  'STAGE', 'STATUS','STATUS CHANGED DATE', 'APPLICATION TYPE',
            'TYPE', 'SUBTYPE', 'PURPOSE', 'SUBPURPOSE','CLIENT NAME', 'LOCATION']

    
    sheet_list = [] 
    df_list = []  
           
    for k, v in sql.items():  
        sheet_list.append(k)
        df = read_query(connection,v)
        df = pd.merge(df,df_ti, how='inner', left_on ='DISPOSITION_TRANSACTION_SID', right_on='DTID')
        df = df[cols]
        df.drop_duplicates('FILE #', keep='first', inplace=True)
        df_list.append(df)
    
    print ('Export results')
    filename = 'AquaStats_testSQL'
    generate_report (workspace, df_list, sheet_list, filename)
    
    print ('Processing Completed!!')


main()
    
