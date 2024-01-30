import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
#from shapely import wkb


def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        cursor = connection.cursor()
        print  ("....Successffuly connected to the database")
    except:
        raise Exception('....Connection failed! Please check your login parameters')

    return connection, cursor


def read_query(connection,cursor,query,bvars):
    "Returns a df containing SQL Query results"
    cursor.execute(query, bvars)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
    
    return df   


def esri_to_gdf (aoi):
    """Returns a Geopandas file (gdf) based on 
       an ESRI format vector (shp or featureclass/gdb)"""
    
    if '.shp' in aoi: 
        gdf = gpd.read_file(aoi)
    
    elif '.gdb' in aoi:
        l = aoi.split ('.gdb')
        gdb = l[0] + '.gdb'
        fc = os.path.basename(aoi)
        gdf = gpd.read_file(filename= gdb, layer= fc)
        
    else:
        raise Exception ('Format not recognized. Please provide a shp or featureclass (gdb)!')
    
    return gdf


def df_2_gdf (df, crs):
    """ Return a geopandas gdf based on a df with Geometry column"""
    df['SHAPE'] = df['SHAPE'].astype(str)
    df['geometry'] = gpd.GeoSeries.from_wkt(df['SHAPE'])
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    #df['geometry'] = df['SHAPE'].apply(wkt.loads)
    #gdf = gpd.GeoDataFrame(df, geometry = df['geometry'])
    gdf.crs = "EPSG:" + str(crs)
    del df['SHAPE']
    
    return gdf


def load_queries ():
    sql = {}

    sql ['aoi'] = """
                    SELECT SDO_UTIL.TO_WKTGEOMETRY(a.SHAPE) SHAPE
                    
                    FROM  WHSE_TANTALIS.TA_CROWN_TENURES_SVW a
                    
                    WHERE a.CROWN_LANDS_FILE = {file_nbr}
                        AND a.DISPOSITION_TRANSACTION_SID = {disp_id}
                  """
                  
    sql ['geomCol'] = """
                SELECT column_name GEOM_NAME
                    
                FROM  ALL_SDO_GEOM_METADATA
                    
                WHERE owner = :owner
                 AND table_name = :tab_name
                        
                    """                   
                                         
    sql ['proximity'] = """
                SELECT '{name}' AS FEATURE,
                        {cols} AS UNIQUE_ID, 
                        ROUND(SDO_GEOM.SDO_DISTANCE(SDO_CS.TRANSFORM(b.{geom_col}, 1000003005, 3005), a.SHAPE, 0.05),2) PROXIMITY_METERS
    
                FROM
                  WHSE_TANTALIS.TA_CROWN_TENURES_SVW a
                  INNER JOIN {table} b
                    ON SDO_WITHIN_DISTANCE (b.{geom_col}, a.SHAPE, 'distance=500 unit=m') = 'TRUE'
                     
                WHERE a.CROWN_LANDS_FILE= '{file_nbr}'
                  AND a.DISPOSITION_TRANSACTION_SID = {disp_id}
                      {def_query}  
                        """

    return sql


def get_geom_colname (connection,cursor,table,geomQuery):
    """ Returns the geometry column of BCGW table name: can be either SHAPE or GEOMETRY"""
    el_list = table.split('.')

    bvars_geom = {'owner':el_list[0].strip(),
                  'tab_name':el_list[1].strip()}
    df_g = read_query(connection,cursor,geomQuery, bvars_geom)
    
    geom_col = df_g['GEOM_NAME'].iloc[0]

    return geom_col


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

        worksheet.set_column(0, 0, 28)
        worksheet.set_column(1, dataframe.shape[1], 38)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'sum'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()
    
    
def run_analysis ():
    """ Runs statusing"""
    print ('Connecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection, cursor = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ('Reading tool inputs.')
    workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKPLACE_2024\20240129_hg_manualStatuses'
    rule_xls = os.path.join(workspace,'inputs','status_datasets.xlsx')
    df_stat = pd.read_excel(rule_xls, 'rules2')
    df_stat.fillna(value='nan',inplace=True)
    
    sql = load_queries ()
    
    print ('Running Analysis.')
    
    file_nbr= '6403921'
    disp_id= 936716
    
    
    df_dict = {} 
    counter = 1
    for index, row in df_stat.iterrows(): 
        name = row['Name']
        table = row['Dataset']
        cols = row['Columns']
        
        print ("\n...Running analysis {} of {}: {}".format(counter,df_stat.shape[0],name))
    
        if row['Where'] != 'nan':
            def_query = 'AND ' + row['Where']
        else:
            def_query = ' '
        
        if table.startswith('WHSE'):
            geomQuery = sql ['geomCol']
            geom_col= get_geom_colname (connection,cursor,table,geomQuery)
            
            query = sql ['proximity'].format(file_nbr= file_nbr, 
                                             disp_id= disp_id,
                                             name= name,
                                             cols=cols,
                                             table=table,
                                             def_query=def_query, 
                                             geom_col=geom_col)
    
            df = pd.read_sql(query, connection)
           
        else:
            gdf_trg = esri_to_gdf (table)
           
            query_aoi= sql['aoi']  .format(file_nbr= file_nbr, disp_id= disp_id)      
            df_aoi= pd.read_sql(query_aoi, connection)
            gdf_aoi= df_2_gdf (df_aoi, 3005)    
            
            gdf_intr = gpd.overlay(gdf_aoi, gdf_trg, how='intersection')
            df_intr = pd.DataFrame(gdf_intr)
            df_intr['PROXIMITY_METERS']= 0
            
            buf_dfs= []
            for radius in [50,500]:
                aoi_buf = gdf_aoi.buffer(radius)
                gdf_aoi_buf = gpd.GeoDataFrame(gpd.GeoSeries(aoi_buf))
                gdf_aoi_buf = gdf_aoi_buf.rename(columns={0:'geometry'}).set_geometry('geometry')
                gdf_aoi_buf_ext = gpd.overlay(gdf_aoi, gdf_aoi_buf, how='symmetric_difference')  
                gdf_buf= gpd.overlay(gdf_aoi_buf_ext, gdf_trg, how='intersection')
                    
                df_buf = pd.DataFrame(gdf_buf)
                df_buf ['PROXIMITY_METERS'] = radius
                
                buf_dfs.append(df_buf)
            
            df =  pd.concat([df_intr]+buf_dfs).reset_index()
            
            cols_d= []
            cols_lst= cols.split(",")
            for col in cols_lst:
                cols_d.append(col)
            cols_d.append('PROXIMITY_METERS')   
            df= df[cols_d]
            
            df.rename(columns={'BlockID': 'UNIQUE_ID'}, inplace=True)
            df.insert(0, 'FEATURE', name)
    
        if df.shape [0] < 1:
            df = df.append({df.columns[0] : name}, ignore_index=True)
        
        df ['RESULT'] = ''    
        for i, row in df.iterrows():
            d = row['PROXIMITY_METERS']
            if d == 0:
                df.at[i,'RESULT'] = 'OVERLAP'       
            elif 0 < d <= 50:
                df.at[i,'RESULT'] = 'WITHIN 50 m'            
            elif 50 < d <= 500:
                df.at[i,'RESULT'] = 'WITHIN 500 m'    
        
            
        df.drop_duplicates(subset= ['UNIQUE_ID','RESULT'], inplace=True)
        df.drop(columns=['PROXIMITY_METERS'], inplace=True)
        
        df_dict[name] = df
        counter += 1
    
    
    df_res= pd.concat(df_dict.values()).reset_index()
    
    df_pivot= pd.pivot_table(df_res, 
                             index=['FEATURE'],
                             columns='RESULT',
                             values= 'UNIQUE_ID',
                             aggfunc=lambda x: ', '.join(map(str, x)),).reset_index()
    
    df_pivot.drop(columns=[('')], inplace= True)
    df_pivot['COMMENT']= ''
        
    print ('Exporting the report')    
    out_path= os.path.join(workspace,'outputs')
    filename= f'proximityAnalysis_fileNbr{file_nbr}_dispID{disp_id}'
    generate_report (out_path, [df_pivot], ['proximityAnalysis'],filename)

run_analysis ()