import os
import geopandas as gpd
from sqlalchemy import create_engine
from datetime import datetime


def connect_to_pgsql (user, password, host, dbname):
    """ Connects to a PostgreSQL database using SQLAlchemy"""
    engine = create_engine(f'postgresql://{user}:{password}@{host}/{dbname}')
    
    return engine


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


def gdf_to_postgis(engine, gdf, table_name):
    """ Writes a geodataframe to a pgsql/postgis table"""
    gdf.columns = gdf.columns.str.lower().str.replace(' ', '_')
    
    gdf.to_postgis(name= table_name, 
                   con= engine, 
                   if_exists= 'replace', 
                   index= False)
    

    
if __name__ == '__main__':
    
    host= 'localhost'
    dbname= 'wc_data'
    user= os.getenv('pg_user')
    password= os.getenv('pg_pwd')

    engine= connect_to_pgsql(user, password, host, dbname)
    
    wks= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\DATASETS\DFO\MPAs\MPAs_updates'
    mpas_shp_name= '20231019updated_MPAs.shp'
    mpas_shp_path= os.path.join(wks, mpas_shp_name)
    
    gdf= esri_to_gdf (mpas_shp_path)
    
    today = datetime.today().strftime("%Y%m%d")
    table_name= 'mpas'
    
    gdf_to_postgis(engine, gdf, table_name)
    