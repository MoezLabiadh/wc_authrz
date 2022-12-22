import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
import folium



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


def df_2_gdf (df, crs):
    """ Return a geopandas gdf based on a df with Geometry column"""
    df['SHAPE'] = df['SHAPE'].astype(str)
    df['geometry'] = gpd.GeoSeries.from_wkt(df['SHAPE'])
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.crs = "EPSG:" + str(crs)
    del gdf['SHAPE']
    
    return gdf



def make_status_map (gdf_aoi, gdf_intr, col_lbl, item, workspace):
    """ Generates HTML Interactive maps of AOI and intersection geodataframes"""
    m = gdf_aoi.explore(
         tooltip= False,
         style_kwds=dict(fill= False, color="red", weight=3),
         name="AOI")

    gdf_intr.explore(
         m=m,
         column= col_lbl, # make choropleth based on rhis column
         tooltip= col_lbl, # show column value in tooltip (on hover)
         popup=True, # show all values in popup (on click)
         cmap="Dark2",  
         style_kwds=dict(color="gray"), # use black outline
         name=item)
	
    folium.TileLayer('stamenterrain', control=True).add_to(m)
    folium.LayerControl().add_to(m)
    
    out_html = os.path.join(workspace,item + '.html')
    m.save(out_html)
    
    

def load_queries ():
    sql = {}

    sql ['aoi'] = """
                    SELECT SDO_UTIL.TO_WKTGEOMETRY(a.SHAPE) SHAPE
                    
                    FROM  WHSE_TANTALIS.TA_CROWN_TENURES_SVW a
                    
                    WHERE a.CROWN_LANDS_FILE = '1404764'
                        AND a.DISPOSITION_TRANSACTION_SID = 937294
                        AND a.INTRID_SID = 970611
                  """
                                        
                    
    sql ['intersect'] = """
                    SELECT b.PID, b.OWNER_TYPE, b.PARCEL_CLASS, 
                           SDO_UTIL.TO_WKTGEOMETRY(b.SHAPE) SHAPE
                    
                    FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a, 
                         WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_FA_SVW b
                    
                    WHERE a.CROWN_LANDS_FILE = '1404764'
                        AND a.DISPOSITION_TRANSACTION_SID = 937294
                        AND a.INTRID_SID = 970611
                        AND b.OWNER_TYPE = 'Private'
                        
                        AND SDO_RELATE (b.SHAPE, a.SHAPE,'mask=ANYINTERACT') = 'TRUE'
                        

                        """
                        
    sql ['buffer'] = """
                    SELECT b.PID, b.OWNER_TYPE, b.PARCEL_CLASS, 
                           SDO_UTIL.TO_WKTGEOMETRY(b.SHAPE) SHAPE
                    
                    FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a, 
                         WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_FA_SVW b
                    
                    WHERE a.CROWN_LANDS_FILE = '1404764'
                        AND a.DISPOSITION_TRANSACTION_SID = 937294
                        AND a.INTRID_SID = 970611
                        AND b.OWNER_TYPE = 'Private'
                    
                        AND SDO_WITHIN_DISTANCE (b.SHAPE, a.SHAPE,'distance = 500') = 'TRUE'
                        AND SDO_GEOM.SDO_DISTANCE(a.SHAPE, b.SHAPE, 0.005) > 0
                         
                    """ 
                    
    return sql


print ('Connecting to BCGW.')
hostname = 'bcgw.bcgov/idwprod1.bcgov'
bcgw_user = os.getenv('bcgw_user')
bcgw_pwd = os.getenv('bcgw_pwd')
connection, cursor = connect_to_DB (bcgw_user,bcgw_pwd,hostname)


print ('Running queries.')
sql = load_queries ()

df_aoi = read_query(connection, cursor, sql['aoi'])
df_intr = read_query(connection, cursor, sql['intersect'])
df_buff = read_query(connection, cursor, sql['buffer'])

df_intr['QUERY'] = "INTERSECT"
df_buff['QUERY'] = "WITHIN 500m"


df_results = pd.concat([df_intr,df_buff])
#df_results.drop_duplicates(subset=['PID'],inplace=True)


print ('Create a Geodataframe of results.')
gdf_aoi = df_2_gdf (df_aoi, 3005)
gdf_rsl = df_2_gdf (df_results, 3005)

print ('Make an interactive map.')
workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\TOOLS\SCRIPTS\STATUSING\demo'
make_status_map (gdf_aoi, gdf_rsl, 'PID', 'Private Parcels within 500m', workspace)
