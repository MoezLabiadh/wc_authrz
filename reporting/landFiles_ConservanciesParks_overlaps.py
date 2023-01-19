
import os
import timeit
import cx_Oracle
import pandas as pd


def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        cursor = connection.cursor()
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection, cursor



def read_query(connection,cursor,query):
    "Returns a df containing SQL Query results"
    cursor.execute(query)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
    
    return df



def generate_report (workspace, df_list, sheet_list,filename):
    """ Exports dataframes to multi-tab excel spreasheet"""
    outfile= os.path.join(workspace, filename + '.xlsx')

    writer = pd.ExcelWriter(outfile,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]
        #workbook = writer.book

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'sum'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()
    
    
def load_queries ():
    sql = {}
    sql['cons'] = """
    
            SELECT a.CROWN_LANDS_FILE, a.DISPOSITION_TRANSACTION_SID, a.INTRID_SID, a.TENURE_STAGE, a.TENURE_STATUS, 
                   a.APPLICATION_TYPE_CDE, a.TENURE_TYPE, a.TENURE_SUBTYPE, a.TENURE_PURPOSE, a.TENURE_SUBPURPOSE, a.TENURE_EXPIRY, a.TENURE_LOCATION,
                   b.CONSERVANCY_AREA_NAME,
                   ROUND((SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(a.SHAPE,b.SHAPE, 0.05), 0.05, 'unit=HECTARE')/ 
                          SDO_GEOM.SDO_AREA(a.SHAPE, 0.05, 'unit=HECTARE'))*100, 0.05) OVERLAP_PERCENT
                          
            FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a, 
                 WHSE_TANTALIS.TA_CONSERVANCY_AREAS_SVW b
            
            WHERE a.RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
                 AND a.TENURE_TYPE in ('LICENCE', 'LEASE')
                 AND SDO_RELATE (a.SHAPE, b.SHAPE ,'mask=ANYINTERACT') = 'TRUE'
                """   
                
    sql['provpark'] = """
    
                SELECT a.CROWN_LANDS_FILE, a.DISPOSITION_TRANSACTION_SID, a.INTRID_SID, a.TENURE_STAGE, a.TENURE_STATUS, 
                    a.APPLICATION_TYPE_CDE, a.TENURE_TYPE, a.TENURE_SUBTYPE, a.TENURE_PURPOSE, a.TENURE_SUBPURPOSE, a.TENURE_EXPIRY, a.TENURE_LOCATION,
                    b.PROTECTED_LANDS_DESIGNATION, b.PROTECTED_LANDS_NAME, b.PARK_CLASS,
                    ROUND((SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(a.SHAPE,b.SHAPE, 0.05), 0.05, 'unit=HECTARE')/ 
                            SDO_GEOM.SDO_AREA(a.SHAPE, 0.05, 'unit=HECTARE'))*100, 0.05) OVERLAP_PERCENT
                    
                FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a, 
                    WHSE_TANTALIS.TA_PARK_ECORES_PA_SVW b

                WHERE a.RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
                    AND a.TENURE_TYPE in ('LICENCE', 'LEASE')
                    AND PROTECTED_LANDS_DESIGNATION = 'PROVINCIAL PARK'
                    AND SDO_RELATE (a.SHAPE, b.SHAPE ,'mask=ANYINTERACT') = 'TRUE'
                """                 

    sql['natpark'] = """
    
                SELECT a.CROWN_LANDS_FILE, a.DISPOSITION_TRANSACTION_SID, a.INTRID_SID, a.TENURE_STAGE, a.TENURE_STATUS, 
                       a.APPLICATION_TYPE_CDE, a.TENURE_TYPE, a.TENURE_SUBTYPE, a.TENURE_PURPOSE, a.TENURE_SUBPURPOSE, a.TENURE_EXPIRY, a.TENURE_LOCATION,
                       b.ENGLISH_NAME, b.LOCAL_NAME,
                       ROUND((SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(a.SHAPE,b.GEOMETRY, 0.05), 0.05, 'unit=HECTARE')/ 
                             SDO_GEOM.SDO_AREA(a.SHAPE, 0.05, 'unit=HECTARE'))*100, 0.05) OVERLAP_PERCENT
                       
                FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a, 
                     WHSE_ADMIN_BOUNDARIES.CLAB_NATIONAL_PARKS b
                
                WHERE a.RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
                     AND a.TENURE_TYPE in ('LICENCE', 'LEASE')
                     AND SDO_RELATE (a.SHAPE, b.GEOMETRY ,'mask=ANYINTERACT') = 'TRUE'
                """ 
                

    sql['expr'] = """
    
            SELECT INTRID_SID,CROWN_LANDS_FILE
                   
            FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW 
            
            WHERE RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
                 AND TENURE_STAGE = 'APPLICATION'
                 AND TENURE_STATUS = 'ACCEPTED'
                 AND APPLICATION_TYPE_CDE = 'REP'
                 AND CROWN_LANDS_FILE NOT IN (SELECT CROWN_LANDS_FILE
                                              FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW 
                                              WHERE TENURE_STATUS IN ('DISPOSITION IN GOOD STANDING','OFFERED','OFFER ACCEPTED'))
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
    start_t = timeit.default_timer() #start time
                   
    print ('Connecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection, cursor = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ("Load Queries")
    sql = load_queries ()
    
    print ("Execute The Conservancies Query")
    df_cons = read_query(connection,cursor,sql['cons'])
    df_cons = df_cons.loc[df_cons['OVERLAP_PERCENT'] > 0]
    
    print ("Execute the Provincial Parks Query")
    df_provpark = read_query(connection,cursor,sql['provpark'])
    df_provpark = df_provpark.loc[df_provpark['OVERLAP_PERCENT'] > 0]
    
    print ("Execute the National Parks Query")
    df_natpark = read_query(connection,cursor,sql['natpark'])
    df_natpark= df_natpark.loc[df_natpark['OVERLAP_PERCENT'] > 0]
    
    
    print ("Execute The Expired Tenures Query")
    df_expr= read_query(connection,cursor,sql['expr'])
    expr_list = df_expr['CROWN_LANDS_FILE'].to_list()
    
    print ('Set Expired tenures')
    expr_list = df_expr['CROWN_LANDS_FILE'].to_list()
    df_cons['APPLICATION_TYPE_CDE'][df_cons['CROWN_LANDS_FILE'].isin(expr_list)==True] = 'REP - EXPIRED'
    df_provpark['APPLICATION_TYPE_CDE'][df_provpark['CROWN_LANDS_FILE'].isin(expr_list)==True] = 'REP - EXPIRED'
    df_natpark['APPLICATION_TYPE_CDE'][df_natpark['CROWN_LANDS_FILE'].isin(expr_list)==True] = 'REP - EXPIRED'
    
    
    print ("Export report")
    dfs = [df_cons,df_provpark,df_natpark]
    sheets = ['Conservancies Overlaps','Provincial Parks Overlaps', 'National Parks Overlaps']
    filename = 'landFiles_ConservanciesParks_overlaps_20230118'
    workspace= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20230118_tenures_conservancies_shawn'
    
    generate_report (workspace, dfs, sheets,filename)
    
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print ('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))
    
main()
