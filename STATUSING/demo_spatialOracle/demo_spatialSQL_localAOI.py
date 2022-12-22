import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkt, wkb
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



def read_query(connection,cursor,query,bvars):
    "Returns a df containing SQL Query results"
    cursor.execute(query,bvars)
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



def get_wkb_srid (gdf):
    """Returns SRID and WKB objects from gdf"""
    # if multipart, dissove to singlepart
    if gdf.shape[0] > 1:
        gdf['dissolvefield'] = 1
        gdf = gdf.dissolve(by='dissolvefield')
        gdf.reset_index(inplace=True)


    srid = gdf.crs.to_epsg()
    
    geom = gdf['geometry'].iloc[0]

    wkb_aoi = geom.to_wkb()
    
    # if geometry has Z values, flatten geometry
    if geom.has_z:
        wkb_aoi = wkb.dumps(geom, output_dimension=2)
        
    
    return wkb_aoi, srid



def make_status_map (gdf_aoi, gdf_intr, col_lbl, item, workspace):
    """ Generates HTML Interactive maps of AOI and intersection geodataframes"""
    
    m = folium.Map(tiles='openstreetmap')
    x1,y1,x2,y2 = gdf_aoi.to_crs(4326)['geometry'].total_bounds
    m.fit_bounds([[y1, x1], [y2, x2]])

    gdf_aoi.explore(
         m=m,
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
   
    sql ['intersect'] = """
                    SELECT b.PID, b.OWNER_TYPE, b.PARCEL_CLASS,
                           ROUND((SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(b.SHAPE, SDO_GEOMETRY(:wkb_aoi, :srid), 0.005), 0.005, 'unit=HECTARE')), 2) OVERLAP_HECTARE,
                           SDO_UTIL.TO_WKTGEOMETRY(b.SHAPE) SHAPE
                    
                    FROM WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_FA_SVW b
                    
                    WHERE b.OWNER_TYPE = 'Private'
                        
                        AND SDO_RELATE (b.SHAPE, SDO_GEOMETRY(:wkb_aoi, :srid),'mask=ANYINTERACT') = 'TRUE'
                        

                        """
                        
    sql ['buffer'] = """
                    SELECT b.PID, b.OWNER_TYPE, b.PARCEL_CLASS, 
                           ROUND(SDO_GEOM.SDO_DISTANCE(b.SHAPE, SDO_GEOMETRY(:wkb_aoi, :srid), 0.005),2) DISTANCE_METER,
                           SDO_UTIL.TO_WKTGEOMETRY(b.SHAPE) SHAPE
                    
                    FROM WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_FA_SVW b
                    
                    WHERE b.OWNER_TYPE = 'Private'
                    
                        AND SDO_WITHIN_DISTANCE (b.SHAPE, SDO_GEOMETRY(:wkb_aoi, :srid),'distance = 500') = 'TRUE'
                        AND SDO_GEOM.SDO_DISTANCE(b.SHAPE, SDO_GEOMETRY(:wkb_aoi, :srid), 0.005) > 0
                         
                    """ 
                    
    return sql

workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\TOOLS\SCRIPTS\STATUSING\demo'

print ('Connecting to BCGW.')
hostname = 'bcgw.bcgov/idwprod1.bcgov'
bcgw_user = os.getenv('bcgw_user')
bcgw_pwd = os.getenv('bcgw_pwd')
connection, cursor = connect_to_DB (bcgw_user,bcgw_pwd,hostname)

print ('Read input AOI')
aoi = os.path.join(workspace,'test_aoi.shp')
gdf_aoi = esri_to_gdf (aoi)
wkb_aoi, srid = get_wkb_srid (gdf_aoi)


print ('Running queries.')
sql = load_queries ()

cursor.setinputsizes(wkb_aoi=cx_Oracle.BLOB) 
bvars = {'wkb_aoi':wkb_aoi,'srid':srid}
df_intr = read_query(connection, cursor, sql['intersect'],bvars)
cursor.setinputsizes(wkb_aoi=cx_Oracle.BLOB) 
df_buff = read_query(connection, cursor, sql['buffer'],bvars)

df_intr['QUERY'] = "INTERSECT"
df_buff['QUERY'] = "WITHIN 500m"

df_results = pd.concat([df_intr,df_buff])
#df_results.drop_duplicates(subset=['PID'],inplace=True)


print ('Create a Geodataframe of results.')

gdf_rsl = df_2_gdf (df_results, 3005)

print ('Make an interactive map.')

make_status_map (gdf_aoi, gdf_rsl, 'PID', 'Private Parcels within 500m', workspace)
