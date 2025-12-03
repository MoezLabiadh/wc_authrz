#-------------------------------------------------------------------------------
# Name:        Aqua Wild Plants - Statusing Tool
#
# Purpose:     This script generates a statusing report based on Aqua Plants Harvest Areas
#
# Author:      Moez Labiadh - GeoBC
#
# Created:     2022-11-28
# Updated:     2025-12-03
#-------------------------------------------------------------------------------

import warnings
warnings.simplefilter(action='ignore')

import os
import oracledb
import pandas as pd
import geopandas as gpd
from shapely import wkb
import zipfile
import tempfile
from pathlib import Path
from datetime import datetime


def connect_to_DB (username,password,hostname):
    """ Returns a connection and cursor to Oracle database"""
    try:
        connection = oracledb.connect(user=username, password=password, dsn=hostname)
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


def combine_kmz_kml(folder_path):
    """
    Combines all KMZ and KML files in a folder and return a geodataframe
    """
    gdfs = []
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Get all KMZ and KML files in the folder
        kmz_files = list(Path(folder_path).glob('*.kmz'))
        kml_files = list(Path(folder_path).glob('*.kml'))
        
        all_files = kmz_files + kml_files
        
        if not all_files:
            raise Exception(f"No KMZ or KML files found in {folder_path}")
        
        print(f"..Found {len(all_files)} KMZ/KML files to process")
        
        for file_path in all_files:
            print(f"...Processing: {file_path.name}")
            
            if file_path.suffix.lower() == '.kmz':
                # Extract KMZ to temporary directory
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                
                # Find KML file in extracted contents
                kml_path = None
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        if file.endswith('.kml'):
                            kml_path = os.path.join(root, file)
                            break
                    if kml_path:
                        break
                
                if kml_path:
                    gdf = gpd.read_file(kml_path, driver='KML')
                else:
                    print(f"Warning: No KML found in {file_path.name}, skipping")
                    continue
            else:
                # Read KML directly
                gdf = gpd.read_file(file_path, driver='KML')
            
            # Add source file name as harvest_ar
            gdf['harvest_ar'] = file_path.stem
            
            # Reproject to BC Albers (EPSG:3005) if needed
            if gdf.crs is None:
                print(f"Warning: No CRS found for {file_path.name}, assuming WGS84")
                gdf.set_crs(epsg=4326, inplace=True)
            
            if gdf.crs.to_epsg() != 3005:
                gdf = gdf.to_crs(epsg=3005)
            
            gdfs.append(gdf)
        
        if not gdfs:
            raise Exception("No valid geodata could be read from the files")
        
        # Combine all geodataframes
        combined_gdf = pd.concat(gdfs, ignore_index=True)
        
        # Keep only geometry and harvest_ar columns
        cols_to_keep = ['geometry', 'harvest_ar']
        combined_gdf = combined_gdf[cols_to_keep]
        
        print(f"..Successfully combined {len(gdfs)} files")
        
        # Dissolve polygons by harvest_ar
        dissolved_gdf = combined_gdf.dissolve(by='harvest_ar', as_index=False)
        
        return dissolved_gdf
        
    finally:
        # Clean up temporary directory
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


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
    wkb_aoi = wkb.dumps(geom)
    
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
    # Add timestamp to filename
    timestamp = datetime.now().strftime('%Y%m%d_')
    outfile= os.path.join(workspace, timestamp + filename + '.xlsx')

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

    #writer.save()
    writer.close()
    
    
if __name__ == "__main__":
    """ Runs statusing"""
    print ('Connecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection, cursor = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ('Reading tool inputs.')
    workspace = r'W:\srm\gss\sandbox\mlabiadh\workspace\20251202_aquaculture_wild_harvest_statusing'
    rule_xls = os.path.join(workspace,'statusing_rules.xlsx')
    df_stat = pd.read_excel(rule_xls, 'rules')
    df_stat.fillna(value='nan',inplace=True)


   # Check if KMZ folder should be used. if not, use a shapefile
    kmz_folder = r'W:\srm\gss\authorizations\wildlife\2025\wild_harvest_statusing\Harvest+area+maps\Harvest area maps'

    use_kmz = True  # Set to False to use a shapefile instead
    
    if use_kmz and os.path.exists(kmz_folder):
        print(f'\nProcessing KMZ/KML files from folder...')
        gdf_hareas = combine_kmz_kml(kmz_folder)
    else:
        print('\nUsing existing shapefile...')
        hareas = os.path.join(workspace,'20240919_1313_status_shapes_2025.shp')
        gdf_hareas = esri_to_gdf(hareas)
    
    id_col = 'harvest_ar'
    hareas = gdf_hareas[id_col].tolist()
    
    print ('\nRunning Analysis.')
    sql = load_queries ()

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
            gdf_ha = gdf_hareas.loc[gdf_hareas[id_col] == harea]
            
            if gdf_ha.shape[0] > 1:
                gdf_ha =  multipart_to_singlepart(gdf_ha) 
                
            wkb_aoi,srid = get_wkb_srid (gdf_ha)
            
            if table.startswith('WHSE'):
                geomQuery = sql ['geomCol']
                geom_col = get_geom_colname (connection,cursor,table,geomQuery)
                
                query = sql ['intersect_wkb'].format(cols=cols,tab=table,
                                                     def_query=def_query, geom_col=geom_col)
                cursor.setinputsizes(wkb_aoi=oracledb.DB_TYPE_BLOB)
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
            new_row = pd.DataFrame({'HARVEST_AREA': ['NO OVERLAPS FOUND!']})
            df_res = pd.concat([df_res, new_row], ignore_index=True)
        
        results[name] =  df_res  
        
    
    print ('\nGenerating the statusing Report.')    
    filename = 'aquaPlants_wild_2025_Applics_statusing'
    df_list = list(results.values())
    sheet_list = list(results.keys())
    generate_report (workspace, df_list, sheet_list,filename)