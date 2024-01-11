#-------------------------------------------------------------------------------
# Name:        Aqua Wild Plants - Statusing Tool
#
# Purpose:     This script generates a statusing report based on Aqua Plants Harvest Areas
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     28-11-2022
# Updated:
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkt, wkb


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


def get_wkb_srid (gdf):
    """Returns SRID and WKB objects from gdf"""

    srid = gdf.crs.to_epsg()
    
    geom = gdf['geometry'].iloc[0]

    wkb_aoi = geom.to_wkb()
    
    # if geometry has Z values, flatten geometry
    if geom.has_z:
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
                              SDO_GEOMETRY(:wkb_aoi, :srid), 0.005), 0.005, 'unit=HECTARE'), 5) OVERLAP_AREA_HA
                    
                    FROM {tab} b
                    
                    WHERE SDO_RELATE (b.{geom_col}, 
                                      SDO_GEOMETRY(:wkb_aoi, :srid),'mask=ANYINTERACT') = 'TRUE'
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

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'sum'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()
    
    
def run_staus ():
    """ Runs statusing"""
    print ('Connecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection, cursor = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ('Reading tool inputs.')
    workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20240110_aquaWildPlants_2024_aplics_status'
    rule_xls = os.path.join(workspace,'statusing','statusing_rules.xlsx')
    df_stat = pd.read_excel(rule_xls, 'rules')
    df_stat.fillna(value='nan',inplace=True)
    hareas = os.path.join(workspace,'data.gdb','new_harvest_areas_2023_v2')
    gdf_hareas = esri_to_gdf (hareas)
    
    sql = load_queries ()
    
    hareas = gdf_hareas['harvest_area'].tolist()
    
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
        for harea in hareas:
            print (".....working on Harvest Area {} of {}: {}".format (c_ha, str(len(hareas)), harea))
            gdf_ha = gdf_hareas.loc[gdf_hareas['harvest_area'] == harea]
            
            if gdf_ha.shape[0] > 1:
                gdf_ha =  multipart_to_singlepart(gdf_ha) 
                
            wkb_aoi,srid = get_wkb_srid (gdf_ha)
            
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
                        
                gdf_intr = gpd.overlay(gdf_ha, gdf_trg, how='intersection')
                gdf_intr['OVERLAP_AREA_HA'] = gdf_intr['geometry'].area/ 10**6
                df = pd.DataFrame(gdf_intr)
                cols_d = []
                cols_d.append(cols)
                cols_d.append('OVERLAP_AREA_HA')
                df = df[cols_d]
    
            df ['HARVEST_AREA'] = harea
            dfs.append (df)
            
            c_ha += 1
        
        df_res = pd.concat(dfs).reset_index(drop=True) 
        cols_res = [col for col in df_res.columns if col != 'HARVEST_AREA']
        cols_res.insert(0,'HARVEST_AREA')
        df_res = df_res[cols_res]
        
        df_res = df_res.loc[df_res['OVERLAP_AREA_HA'] > 0]
        df_res = df_res.sort_values('HARVEST_AREA')
        
        if name == 'FN PIP Consultation Areas':
            df_res = df_res.groupby(['HARVEST_AREA','CNSLTN_AREA_NAME','CONTACT_ORGANIZATION_NAME'], 
                                    as_index=False)['OVERLAP_AREA_HA'].agg('sum')
        if df_res.shape [0] < 1:
            df_res = df_res.append({'HARVEST_AREA' : 'NO OVERLAPS FOUND!'}, ignore_index=True)
        
        results[name] =  df_res  
        
    
    print ('\nGenerating the statusing Report.')    
    filename = 'aquaPlants_wild_2023_newApplics_statusing'
    df_list = list(results.values())
    sheet_list = list(results.keys())
    generate_report (workspace, df_list, sheet_list,filename)
    

run_staus ()

            
