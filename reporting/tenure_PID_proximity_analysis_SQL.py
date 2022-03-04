import os
import cx_Oracle
import numpy as np
import pandas as pd



def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


def get_tenures (xlsx):
    """Returns a df containing the list of Tenures"""
    return pd.read_excel(xlsx)
    #strr = ",".join (str(x) for x in df['INTEREST_PARCEL_ID'].tolist())


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
            
            
def evaluate_proximity (df):
    """Evaluates proximity rules"""
    df.drop(df[df['PROXIMITY_METERS'] > 30].index, inplace=True)

    parcels = [x for x in df['INTRID_SID'].unique()]
    
    dfs = []
    for parcel in parcels:
        df_temp = df.loc[df['INTRID_SID']== parcel]
        df_temp['EVALUATION'] = np.where(df_temp['PROXIMITY_METERS'] == 0,'TENURE INTERSECTS PID', 'EVALUATE')
        
        if "TENURE INTERSECTS PID" in df_temp['EVALUATION'].to_list():
            df_temp.drop(df_temp[df_temp['EVALUATION'] == 'EVALUATE'].index, inplace=True)
            
        else:
            
            df_temp['EVALUATION'] = np.where(((df_temp['EVALUATION'] ==  'EVALUATE') &  
                                   (df_temp['PROXIMITY_METERS'] ==  df_temp['PROXIMITY_METERS'].min())), 'CLOSEST PID', 'FARTHER PID' )
            
        dfs.append(df_temp)
        
    
    return pd.concat(dfs)
           

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
    workspace = r'\\...\PM_dataTruthing\20220304_PIDs'

    print (" Connect to BCGW")    
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    username = 'XXXX'
    password = 'XXXX'
    connection = connect_to_DB (username,password,hostname)
    
    print (" Get the Tenures")
    xlsx = os.path.join(workspace, 'pm_gm_list.xlsx')
    df_ten = get_tenures (xlsx)
    
    print ("Execute SQL Query")
    
    sql = """
           SELECT ten.INTRID_SID, pp.PID, pp.OWNER_TYPE,  ROUND(SDO_NN_DISTANCE(1),1) PROXIMITY_METERS 

           FROM WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_SVW pp,
                WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES ten
     
          WHERE ten.INTRID_SID in ({p_list})
                         
           AND pp.OWNER_TYPE = 'Private'
           AND SDO_NN(pp.SHAPE, ten.SHAPE, 'sdo_num_res={n_neighbor}' ,1) = 'TRUE'
            """
            
    parcels = ",".join (str(x) for x in df_ten['INTEREST_PARCEL_ID'].tolist())
    query = sql.format(p_list=parcels, n_neighbor=3)
    df_sql = read_query(connection,query)
    
    print ('Merge dataframes')
    df_res = pd.merge(df_ten,df_sql,how='left', left_on='INTEREST_PARCEL_ID',right_on='INTRID_SID')
    
    print ('Evaluate Proximity')
    df_final = evaluate_proximity (df_res)
    df_final =df_final[['FILE_NBR', 'PID','PROXIMITY_METERS', 'EVALUATION']]
    
    unique_vals = df_final['FILE_NBR'].nunique()
    print (unique_vals)
   
    
    print ('Export results')
    filename = "sql_results"
    generate_report (workspace, [df_final], ['Tenure-PID Proximity Query'], filename)


main ()
