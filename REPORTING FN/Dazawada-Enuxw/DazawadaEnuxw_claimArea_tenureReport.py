import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkb
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



def get_wkb(gdf):
    """Returns WKB object from gdf"""
    
    geom = gdf['geometry'].iloc[0]
    
    # Check if the geometry is a MultiPolygon
    if geom.geom_type == 'MultiPolygon':
        wkb_aoi = wkb.dumps(geom, output_dimension=2)
    else:
        wkb_aoi = geom.wkb
    
    return wkb_aoi


def read_query(connection,cursor,query,bvars):
    "Returns a df containing SQL Query results"
    cursor.execute(query, bvars)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
    
    return df 


if __name__==__name__:
    
    print ('\nConnecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection, cursor = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print("\nReading the Claim Area dataset...")
    wks= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20231030_DazawadaEnuxw_claimArea'
    clm_ar= os.path.join(wks, 'test_shape.shp')
    gdf_clm= esri_to_gdf (clm_ar)
    
    wkb_aoi= get_wkb (gdf_clm)
    
    print ("\nRunning SQL queries...")
    sql = load_queries ()
    
    res_dfs={}
    sheets= []
    
    nbr_queries= len(sql)
    counter= 1
    for k, v in sql.items():
        print(f"....running query {counter} of {nbr_queries}: {k}")
        cursor.setinputsizes(wkb_aoi=cx_Oracle.BLOB)
        bvars = {'wkb_aoi': wkb_aoi}
        
        df= read_query(connection, cursor, sql[k], bvars)
        
        df.drop('SHAPE', axis=1, inplace= True)
        
        for col in df.columns:
            if 'DATE' in col:
                df[col] =  pd.to_datetime(df[col], infer_datetime_format=True, errors = 'coerce').dt.date
            
        res_dfs[k]= df
        sheets.append(k)
        
        counter+= 1
        
    
    
    
    
    
    
    
    