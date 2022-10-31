"""
Name:        Automatic Status Tool - LITE version! DRAFT

Purpose:     This script checks for overlaps between an AOI and BCGW datasets
             specified in the AST common datasets spreadsheet. 
             
Note         - For now the script supports only AOIs stored in the TANTALIS Crown Tenure table/view. 
               Support for User defined AOIs (shp, featureclass) will be added soon.
               
             - The script returns a Dictionnary containing the overlap results. Functions will 
               be added to support writing results to spreadsheet.

Arguments:   - BCGW username
             - BCGW password
             - File number
             - Disposition ID
             - Parcel ID
           
Author:      MLABIADH

Created:     2022-10-28

"""



import warnings
warnings.simplefilter(action='ignore')

import os
import re
import cx_Oracle
import pandas as pd



def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

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
   
            

def get_table_cols (item,df_stat):
    """Returns table and field names from the AST common datasets spreadsheet"""
    df_stat = df_stat.loc[df_stat['Featureclass_Name(valid characters only)'] == item]
    df_stat.fillna(value='nan',inplace=True)

    table = df_stat['Datasource'].iloc[0]

    fields = []
    fields.append('b.' + str(df_stat['Fields_to_Summarize'].iloc[0].strip()))

    for f in range (2,7):
        for i in df_stat['Fields_to_Summarize' + str(f)].tolist():
            if i != 'nan':
                fields.append('b.' + str(i.strip()))
                
                
    # TEMPORARY FIXES: for incorrect column names in the AST spreadsheet
    if 'b.ROAD_NAME' in fields:
        fields.remove('b.ROAD_NAME')
        fields.append('b.ROAD_SECTION_NAME')

    if 'b.DAM_FILE_NO' in fields:
        fields.remove('b.DAM_FILE_NO')
        fields.append('b.DAM_FILE_NUMBER')
                    
    if item == 'OGC Road Permit Areas':
        fields.remove('b.STATUS')
        fields.append('b.CONSTRUCTION_DESC')
                    

    cols = ','.join(x for x in fields)
    
    # TEMPORARY FIX:  for empty column names in the AST input spreadsheet
    if cols == 'b.nan':
        cols = 'b.OBJECTID'
    

    return table, cols


            

def get_def_query (item,df_stat):
    """Returns an ORacle SQL formatted def query (if any) from the AST common datasets spreadsheet"""
    df_stat = df_stat.loc[df_stat['Featureclass_Name(valid characters only)'] == item]
    df_stat.fillna(value='nan',inplace=True)

    def_query = df_stat['Definition_Query'].iloc[0].strip()

    def_query = def_query.strip()
    
    if def_query == 'nan':
        def_query = " "
        
    else:
       def_query = def_query.replace('"', '')
       def_query = re.sub(r'(\bAND\b)', r'\1 b.', def_query)
       def_query = re.sub(r'(\bOR\b)', r'\1 b.', def_query)
       
       if def_query[0] == "(":
           def_query = def_query.replace ("(", "(b.") 
           def_query = "(" + def_query + ")"
       else:
           def_query = "b." + def_query
    
       def_query = 'AND ' + def_query
    
    
    return def_query



def get_radius (item, df_stat):
    """Returns the buffer distance (if any) from the AST common datasets spreadsheet"""
    df_stat = df_stat.loc[df_stat['Featureclass_Name(valid characters only)'] == item]
    df_stat.fillna(value=0,inplace=True)
    df_stat['Buffer_Distance'] = df_stat['Buffer_Distance'].astype(int)
    radius = df_stat['Buffer_Distance'].iloc[0]
    
    return radius




def load_queries ():
    sql = {}
    sql ['geomCol'] = """
                    SELECT column_name GEOM_NAME
                    
                    FROM  ALL_SDO_GEOM_METADATA
                    
                    WHERE owner = '{owner}'
                        AND table_name = '{tab_name}'
                        
                    """                   
                    
    sql ['intersect'] = """
                    SELECT {cols}
                    
                    FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a, {table} b
                    
                    WHERE a.CROWN_LANDS_FILE = '{file_nbr}'
                        AND a.DISPOSITION_TRANSACTION_SID = {disp_id}
                        AND a.INTRID_SID = {parcel_id}
                        
                        AND SDO_ANYINTERACT (b.{geom_col}, a.SHAPE) = 'TRUE'
                        
                        {def_query}
                        """
                        
    sql ['buffer'] = """
                    SELECT {cols}
                    
                    FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a, {table} b
                    
                    WHERE a.CROWN_LANDS_FILE = '{file_nbr}'
                        AND a.DISPOSITION_TRANSACTION_SID = {disp_id}
                        AND a.INTRID_SID = {parcel_id}
                        
                        AND SDO_WITHIN_DISTANCE (b.{geom_col}, a.SHAPE,'distance = {radius}') = 'TRUE'
                        
                        {def_query}    
                    """ 
    return sql



def get_geom_colname (connection,table,geomQuery):
    """ Returns the geometry column of BCGW table name: can be either SHAPE or GEOMETRY"""
    el_list = table.split('.')

    geomQuery = geomQuery.format(owner =el_list[0].strip(),tab_name =el_list[1].strip())
    df_g = read_query(connection,geomQuery)
    
    geom_col = df_g['GEOM_NAME'].iloc[0]

    return geom_col


def execute_status ():
    """Executes the ST light process """
    print ('Connecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    #bcgw_user = 'XXXX'
    bcgw_pwd = os.getenv('bcgw_pwd')
    #bcgw_pwd = 'XXXX'
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ('\nReading User Inputs.')
    in_fileNbr = '1415320'
    in_dispID = 944973
    in_prclID = 978528
    print ('....input File Number: {}'.format(in_fileNbr))
    print ('....input Disposition ID: {}'.format(in_dispID))
    print ('....input Parcel ID: {}'.format(in_prclID))
    
    print ('\nLoading SQL queries')
    sql = load_queries ()
    
    print ('\nReading the AST Common datasets spreadsheet.')
    wksp_ast = r'\\giswhse.env.gov.bc.ca\WHSE_np\corp\script_whse\python\Utility_Misc\Ready\statusing_tools_arcpro'
    common_xls = os.path.join(wksp_ast,'statusing_input_spreadsheets', 'one_status_common_datasets.xlsx')
    df_stat = pd.read_excel(common_xls)
    df_stat.dropna(how='all', inplace=True)
    
    
    print ('\nRunning the analysis.')
    results = {} # this dictionnary will hold the overlay results
    item_count = df_stat.shape[0]
    counter = 1
    for index, row in df_stat.iterrows():
        item = row['Featureclass_Name(valid characters only)']
        
        print ('\n****working on item {} of {}: {}***'.format(counter,item_count,item))
        
        print ('.....getting table and column names')
        table, cols = get_table_cols (item,df_stat)
        
        print ('.....getting geometry column name')
        geomQuery = sql ['geomCol']
        geom_col = get_geom_colname (connection,table,geomQuery)
        
        print ('.....getting definition query (if any)')
        def_query = get_def_query (item,df_stat)
    
        print ('.....getting buffer distance (if any)')
        radius = get_radius (item, df_stat)  
        
        print ('.....running the Intersect query')
        query_intr = sql ['intersect'].format(cols= cols,
                                         table= table,
                                         file_nbr= in_fileNbr,
                                         disp_id= in_dispID,
                                         parcel_id= in_prclID,
                                         def_query= def_query,
                                         geom_col= geom_col)
        
        df_intr = read_query(connection,query_intr)
        df_intr ['OVERLAY RESULT'] = 'INTERSECT'
        
        if radius > 0:
            print ('.....running the Buffer query')
            query_buf= sql ['buffer'].format(cols= cols,
                                             table= table,
                                             file_nbr= in_fileNbr,
                                             disp_id= in_dispID,
                                             parcel_id= in_prclID,
                                             def_query= def_query,
                                             geom_col= geom_col,
                                             radius= radius)
            
            df_buf = read_query(connection,query_buf)
            df_buf ['OVERLAY RESULT'] = 'WITHIN {} m'.format(str(radius))   
            
            df_all =  pd.concat([df_intr, df_buf])
            df_all.sort_values(by='OVERLAY RESULT', ascending=True, inplace = True)
            df_cols = list(df_all.columns)
            df_all.drop_duplicates(subset=df_cols, keep='first', inplace=True)
            
        else:
            df_all = df_intr
        
        ov_nbr = df_all.shape[0]
        print ('....Number of Overlay Features: {}'.format(ov_nbr))
        # add the dataframe to the resuls dictionnary
        results[item] =  df_all
        
     
        counter += 1
    
    return results
        
        

results = execute_status()
