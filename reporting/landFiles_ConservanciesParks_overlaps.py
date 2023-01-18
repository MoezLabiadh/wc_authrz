
import os
import timeit
import pyodbc
import pandas as pd


def connect_to_DB (connection_string):
    """ Returns a connection to Oracle database"""
    try:
        connection = pyodbc.connect(connection_string)
        print  ("...Successffuly connected to the database")
    except:
        raise Exception('...Connection failed! Please check your connection parameters')

    return connection


def read_query(connection,query):
    "Returns a df containing SQL Query results"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        cols = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame.from_records(rows, columns=cols)
    
    finally:
        if cursor is not None:
            cursor.close()



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
                   ROUND((SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(a.SHAPE,b.SHAPE, 5), 5, 'unit=SQ_M')/ 
                          a.FEATURE_AREA_SQM)*100, 2) OVERLAP_PERCENT
                          
            FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a, 
                 WHSE_TANTALIS.TA_CONSERVANCY_AREAS_SVW b
            
            WHERE a.RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
                 AND a.TENURE_TYPE in ('LICENCE', 'LEASE')
                 AND SDO_RELATE (b.SHAPE, a.SHAPE ,'mask=ANYINTERACT') = 'TRUE'
                """   
                
    sql['park'] = """
    
            SELECT a.CROWN_LANDS_FILE, a.DISPOSITION_TRANSACTION_SID, a.INTRID_SID, a.TENURE_STAGE, a.TENURE_STATUS, 
                   a.APPLICATION_TYPE_CDE, a.TENURE_TYPE, a.TENURE_SUBTYPE, a.TENURE_PURPOSE, a.TENURE_SUBPURPOSE, a.TENURE_EXPIRY, a.TENURE_LOCATION,
                   b.PROTECTED_LANDS_DESIGNATION, b.PROTECTED_LANDS_NAME, b.PARK_CLASS,
                   ROUND((SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(a.SHAPE,b.SHAPE, 5), 5, 'unit=SQ_M')/ 
                          a.FEATURE_AREA_ SQM)*100, 2) OVERLAP_PERCENT
                   
            FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a, 
                 WHSE_TANTALIS.TA_PARK_ECORES_PA_SVW b
            
            WHERE a.RESPONSIBLE_BUSINESS_UNIT = 'VI - LAND MGMNT - VANCOUVER ISLAND SERVICE REGION'
                 AND a.TENURE_TYPE in ('LICENCE', 'LEASE')
                 AND SDO_RELATE (b.SHAPE, a.SHAPE ,'mask=ANYINTERACT') = 'TRUE'
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


      

        
start_t = timeit.default_timer() #start time
               
print (" Connect to BCGW")    
driver = 'Oracle in OraClient12Home1'
server = 'bcgw.bcgov'
port= '1521'
dbq= 'idwprod1'
hostname = 'bcgw.bcgov/idwprod1.bcgov'
bcgw_user = 'XXX'
bcgw_pwd = 'XXX'
connection_string ="""
            DRIVER={driver};
            SERVER={server}:{port};
            DBQ={dbq};
            Uid={uid};
            Pwd={pwd}
                """.format(driver=driver,server=server, port=port,
                            dbq=dbq,uid=bcgw_user,pwd=bcgw_pwd)
connection = connect_to_DB (connection_string)

print ("Load Queries")
sql = load_queries ()

print ("Execute The Conservancies Query")
df_cons = read_query(connection,sql['cons'])
df_cons = df_cons.loc[df_cons['OVERLAP_PERCENT'] > 0]

print ("Execute The Parks Query")
df_park = read_query(connection,sql['park'])
df_park = df_park.loc[df_park['OVERLAP_PERCENT'] > 0]

print ("Execute The Expired Tenures Query")
df_expr= read_query(connection,sql['expr'])
expr_list = df_expr['CROWN_LANDS_FILE'].to_list()

print ('Set Expired tenures')
expr_list = df_expr['CROWN_LANDS_FILE'].to_list()
df_cons['APPLICATION_TYPE_CDE'][df_cons['CROWN_LANDS_FILE'].isin(expr_list)==True] = 'REP - EXPIRED'
df_park['APPLICATION_TYPE_CDE'][df_park['CROWN_LANDS_FILE'].isin(expr_list)==True] = 'REP - EXPIRED'


print ("Export report")
dfs = [df_cons,df_park]
sheets = ['Conservancies Overlaps','Parks Overlaps']
filename = 'landFiles_ConservanciesParks_overlaps_20230118'
workspace= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20230118_tenures_conservancies_shawn'

generate_report (workspace, dfs, sheets,filename)


finish_t = timeit.default_timer() #finish time
t_sec = round(finish_t-start_t)
mins = int (t_sec/60)
secs = int (t_sec%60)
print ('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))
    
