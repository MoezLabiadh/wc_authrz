"""
This script creates summary statistics for the 
new SE Coal stewardship project polygons.
"""

import warnings
warnings.simplefilter(action='ignore')

import os
import json
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkb


def get_db_cnxinfo (dbname='BCGW'):
    """ Retrieves db connection params from the config file"""
    
    with open(r'H:\config\db_config.json', 'r') as file:
        data = json.load(file)
        
    if dbname in data:
        cnxinfo = data[dbname]
        return cnxinfo
    
    raise KeyError(f"Database '{dbname}' not found.")
    

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


def multipart_to_singlepart(gdf):
    """Converts a multipart gdf to singlepart gdf """
    gdf['dissolvefield'] = 1
    gdf = gdf.dissolve(by='dissolvefield')
    gdf.reset_index(inplace=True)
    gdf = gdf[['geometry']] #remove all columns
         
    return gdf


def get_wkb_srid(gdf):
    """Returns SRID and WKB objects from gdf"""
    srid = gdf.crs.to_epsg()
    geom = gdf['geometry'].iloc[0]

    wkb_aoi = wkb.dumps(geom, output_dimension=2)
        
    return wkb_aoi, srid


def load_queries ():
    sql = {}

    sql ['geomCol'] = """
                    SELECT column_name GEOM_NAME
                    
                    FROM  ALL_SDO_GEOM_METADATA
                    
                    WHERE owner = :owner
                        AND table_name = :tab_name
                    """                   
                                         
    sql ['intersect_wkb'] = """
                    SELECT {cols}, 
                        ROUND(SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(b.{geom_col},
                              SDO_GEOMETRY(:wkb_aoi, :srid), 0.005), 0.005, 'unit=HECTARE'), 5) AREA_HA
                    
                    FROM {tab} b
                    
                    WHERE SDO_RELATE (b.{geom_col}, 
                                      SDO_GEOMETRY(:wkb_aoi, :srid),'mask=ANYINTERACT') = 'TRUE'
                        {def_query}  
                        """

    return sql


def get_geom_colname (connection,cursor,table,geomQuery):
    """ Returns the geometry column of BCGW table: SHAPE or GEOMETRY"""
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

        worksheet.set_column(0, dataframe.shape[1], 25)

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
    cnxinfo= get_db_cnxinfo(dbname='BCGW')
    hostname = cnxinfo['hostname']
    username= cnxinfo['username']
    password= cnxinfo['password']
    connection, cursor = connect_to_DB (username,password,hostname)
    
    print ('Reading tool inputs.')
    workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240109_SE_Coal_Stewardship'
    rule_xls = os.path.join(workspace,'scripts','summary_rules.xlsx')
    df_stat = pd.read_excel(rule_xls, 'rules')
    df_stat.fillna(value='nan',inplace=True)
    polys = os.path.join(workspace,'data','data.gdb','protection_polygons')
    gdf_polys = esri_to_gdf (polys)
    gdf_polys['POLYGON_AREA_HA']= round(gdf_polys['geometry'].area/ 10000,2)
    
    
    
    #####FOR TESTING#####
    #test_ploys=['Todhunter Creek','Chauncey Creek']
    #gdf_polys= gdf_polys.loc[gdf_polys['POLYGON_NAME'].isin(test_ploys)]
    
    
    
    sql = load_queries ()
    
    poly_list = gdf_polys['POLYGON_NAME'].tolist()
    
    print ('Running Analysis.')
    results = {} 
    c_names = 1
    for index, row in df_stat.iterrows(): 
        name = row['Name']
        table = row['Dataset']
        cols = row['Columns']
        print ("\n...overlapping {} of {}: {}".format(c_names,df_stat.shape[0],name))
    
        if row['Where'] != 'nan':
            def_query = 'AND ' + row['Where']
        else:
            def_query = ' '
        
        c_names += 1
         
        c_ha = 1
        dfs = []
        
        for poly in poly_list:
            print (".....working on Polygon {} of {}: {}".format (c_ha, str(len(poly_list)), poly))
            gdf_poly= gdf_polys.loc[gdf_polys['POLYGON_NAME'] == poly]
            
            if gdf_poly.shape[0] > 1:
                gdf_poly =  multipart_to_singlepart(gdf_poly) 
                
            wkb_aoi,srid = get_wkb_srid (gdf_poly)
            
            if table.startswith('WHSE'):
                geomQuery = sql ['geomCol']
                geom_col = get_geom_colname (connection,cursor,table,geomQuery)
                
                query = sql ['intersect_wkb'].format(cols=cols,tab=table,
                                                     def_query=def_query, geom_col=geom_col)
                cursor.setinputsizes(wkb_aoi=cx_Oracle.BLOB)
                bvars = {'wkb_aoi':wkb_aoi,'srid':srid}
                df = read_query(connection,cursor,query,bvars)
           
            else:
                gdf_trg = esri_to_gdf (table)
                if not gdf_trg.crs.to_epsg() == 3005:
                        gdf_trg = gdf_trg.to_crs({'init': 'epsg:3005'})
                        
                gdf_intr = gpd.overlay(gdf_poly, gdf_trg, how='intersection')
                gdf_intr['AREA_HA'] = gdf_intr['geometry'].area/ 10000
                df = pd.DataFrame(gdf_intr)
                
                df['TYPE']= name
                
                cols_d = []
                
                cols_lst= cols.split(",")
                for col in cols_lst:
                    cols_d.append(col)
                    
                cols_d.append('AREA_HA')
                df = df[cols_d]
                
                if name=='THLB Area':
                    df['AREA_HA']= df['AREA_HA'] * df['thlb_fact']
    
            df ['POLYGON_NAME'] = poly
            
            if name== 'Land Ownership':
                df['TYPE']= df['TYPE'] + " - " + df['OWNER_TYPE']
                df.drop(columns=['OWNER_TYPE'], inplace= True)
                
            #summarize data
            sum_cols= ['POLYGON_NAME', 'TYPE']
            df_sum = df.groupby(sum_cols)['AREA_HA'].sum().reset_index()
            df_sum['AREA_HA'] = round(df_sum['AREA_HA'], 2)
            
            dfs.append (df_sum)
            
            c_ha += 1
        
        df_res = pd.concat(dfs).reset_index(drop=True) 
        cols_res = [col for col in df_res.columns if col != 'POLYGON_NAME']
        cols_res.insert(0,'POLYGON_NAME')
        df_res = df_res[cols_res]
        
        df_res = df_res.loc[df_res['AREA_HA'] > 0]
        df_res = df_res.sort_values('POLYGON_NAME')
        
        if df_res.shape [0] < 1:
            df_res = df_res.append({'POLYGON_NAME' : 'NO OVERLAPS FOUND!'}, ignore_index=True)
        
        results[name] =  df_res 
    
    df_sum_lst = list(results.values())    
    df_sum_all= pd.concat(df_sum_lst).reset_index(drop=True) 
    
    df_hect= gdf_polys[['POLYGON_NAME', 'POLYGON_AREA_HA']]
    
    df_sum_all= pd.merge(df_hect, 
                         df_sum_all, 
                         how='left', 
                         on='POLYGON_NAME')
    
    df_pivot= pd.pivot_table(df_sum_all, 
                             index=['POLYGON_NAME', 'POLYGON_AREA_HA'],
                             columns='TYPE',
                             values= 'AREA_HA').reset_index()
    
    print ('\nGenerating the Summary Report.')    
    filename = 'SE_Coal_stewardship_summaryStats'
    df_list=[df_sum_all, df_pivot]
    sheet_list = ['Raw', 'Pivot table']
    outloc= os.path.join(workspace, 'output')
    generate_report (outloc, df_list, sheet_list ,filename)
    

run_analysis ()