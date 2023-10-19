import os
import pandas as pd
from sqlalchemy import create_engine, text


def connect_to_pgsql (user, password, host, dbname):
    """ Connects to a PostgreSQL database using SQLAlchemy"""
    engine = create_engine(f'postgresql://{user}:{password}@{host}/{dbname}')
    connection= engine.connect()
    
    return connection


sql= """
        SELECT 
        	aq.year,
        	aq.harvest_ar, 
        	mp.name_e,
        	ROUND((ST_Area(ST_Intersection(aq.geom, mp.geometry))/10000)::numeric,2) AS intersection_area_ha
        
        FROM aquaplants_wild_harvest_areas AS aq
        	JOIN mpas AS mp
        		ON ST_Intersects(aq.geom, mp.geometry)
        	
        WHERE  mp.name_e LIKE '%Caamano%';
     """

def execute_query (connection, sql):
    """Executes an sql and returns results in a df"""
    
    return pd.read_sql(text(sql), connection)





if __name__ == '__main__':
    
    host= 'localhost'
    dbname= 'wc_data'
    user= os.getenv('pg_user')
    password= os.getenv('pg_pwd')

    connection= connect_to_pgsql(user, password, host, dbname)
    
    df= execute_query (connection, sql)

