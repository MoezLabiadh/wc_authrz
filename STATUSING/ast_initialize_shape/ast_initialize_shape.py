import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
from datetime import date


def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("..successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


def create_dir (path, dir):
    """ Creates new folder and returns path"""
    try:
      os.makedirs(os.path.join(path,dir))
      print('...folder {} created!'.format(dir))

    except OSError:
        print('...folder {} already exists!'.format(dir))
        pass

    return os.path.join(path,dir)


def df_2_gdf (df, crs):
    """ Return a geopandas gdf based on a df with Geometry column"""
    df['SHAPE'] = df['SHAPE'].astype(str)
    df['geometry'] = gpd.GeoSeries.from_wkt(df['SHAPE'])
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    #df['geometry'] = df['SHAPE'].apply(wkt.loads)
    #gdf = gpd.GeoDataFrame(df, geometry = df['geometry'])
    gdf.crs = "EPSG:" + str(crs)
    #del df['SHAPE']
    del gdf['SHAPE']
    
    return gdf



if __name__ == "__main__":
    in_tenure_nbr =   input('Enter File Number:')
    in_tenure_nbr= str(in_tenure_nbr).strip()
    
    print("\nCreate Statusing folders")
    stat_dir= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20231108_landStatusing_folder_script\Lands_Statusing'
    tn_stat_dir= create_dir (stat_dir, in_tenure_nbr)
    this_month= date.today().strftime("%b%Y").lower()
    month_dir= create_dir (tn_stat_dir, this_month)
    
    print ("\nConnect to BCGW")    
    bcgw_host = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,bcgw_host)
    
    print ("\nExecute SQL Query")
    
    sql = f"""
           SELECT
               CROWN_LANDS_FILE, DISPOSITION_TRANSACTION_SID, INTRID_SID, 
               TENURE_STAGE, TENURE_STATUS,
               SDO_UTIL.TO_WKTGEOMETRY(SHAPE) SHAPE
                  
           FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW
     
          WHERE CROWN_LANDS_FILE =  '{in_tenure_nbr}'"""
          
    df_geo= pd.read_sql(sql, connection)
    
    print ("\nCreate a spatial file")
    gdf = df_2_gdf (df_geo, 3005)
    
    print ("\nExport the spatial file(s)")
    disps= list(gdf['DISPOSITION_TRANSACTION_SID'].unique())
    prcls= gdf['INTRID_SID'].to_list()
    
    
    
    if len(disps)==1:
        print (f"..This file has {len(disps)} active dispositions")
        if len(prcls)==1:
            print (f"...This file has {len(prcls)} parcel ")
            gdf.to_file(os.path.join(month_dir, in_tenure_nbr+'.shp'))
            
        else:
            for prcl in prcls:
                prcl_str= in_tenure_nbr + '_parcel'+ str(prcl)
                prcl_dir= create_dir (month_dir, prcl_str)
                
                gdf_prcl= gdf.loc[gdf['INTRID_SID']==prcl]
                gdf_prcl.to_file(os.path.join(prcl_dir, prcl_str+'.shp'))
                
    else:
        print ("..This file has MULTIPLE active dispositions. Check file.")
            
#1415503 (1 parcel)    
#1409871 (2 parcels)          
            