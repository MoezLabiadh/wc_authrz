"""
Name:        Automatic Status Tool - LITE version! DRAFT
Purpose:     This script checks for overlaps between an AOI and datasets
             specified in the AST datasets spreadsheets (common and region specific). 
             
Notes        The script supports AOIs in TANTALIS Crown Tenure spatial view 
             and User defined AOIs (shp, featureclass).
               
             The script generates a spreadhseet of conflicts and 
             Interactive HTML maps showing the AOI and ovelappng features
                             
Arguments:   - Output location (workspace)
             - BCGW username
             - BCGW password
             - Region (west coast, skeena...)
             - AOI: - ESRI shp or featureclass OR
                    - File number
                    - Disposition ID
                    - Parcel ID
                
Author:      Moez Labiadh
Created:     2023-01-12
"""



import warnings
warnings.simplefilter(action='ignore')

import os
import re
import timeit
import cx_Oracle
import pandas as pd
import folium
import geopandas as gpd
from shapely import wkt, wkb
#from datetime import datetime



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



def read_input_spreadsheets (wksp_xls,region):
    """Returns input spreadhseets"""
    common_xls = os.path.join(wksp_xls, 'one_status_common_datasets.xlsx')
    region_xls = os.path.join(wksp_xls, 'one_status_{}_specific.xlsx'.format(region.lower()))
    
    df_stat_c = pd.read_excel(common_xls)
    df_stat_r = pd.read_excel(region_xls)
    
    df_stat = pd.concat([df_stat_c, df_stat_r])
    df_stat.dropna(how='all', inplace=True)
    
    df_stat = df_stat.reset_index(drop=True)
    
    return df_stat
    
    

def get_table_cols (item_index,df_stat):
    """Returns table and field names from the AST datasets spreadsheet"""
    #df_stat = df_stat.loc[df_stat['Featureclass_Name(valid characters only)'] == item]
    df_stat_item = df_stat.loc[[item_index]]
    df_stat_item.fillna(value='nan',inplace=True)

    table = df_stat_item['Datasource'].iloc[0].strip()
    
    fields = []
    fields.append(str(df_stat_item['Fields_to_Summarize'].iloc[0].strip()))

    for f in range (2,7):
        for i in df_stat_item['Fields_to_Summarize' + str(f)].tolist():
            if i != 'nan':
                fields.append(str(i.strip()))

    col_lbl = df_stat_item['map_label_field'].iloc[0].strip()
    
    if col_lbl != 'nan' and col_lbl not in fields:
        fields.append(col_lbl)
    
    if table.startswith('WHSE') or table.startswith('REG'):       
        cols = ','.join('b.' + x for x in fields)

        # TEMPORARY FIX:  for empty column names in the COMMON AST input spreadsheet
        if cols == 'b.nan':
            cols = 'b.OBJECTID'
    else:
        cols = fields
        # TEMPORARY FIX:  for empty column names in the REGION AST input spreadsheet
        if cols[0] == 'nan':
            cols = []
    
    return table, cols, col_lbl

          

def get_def_query (item_index,df_stat):
    """Returns an ORacle SQL formatted def query (if any) from the AST datasets spreadsheet"""
    #df_stat = df_stat.loc[df_stat['Featureclass_Name(valid characters only)'] == item]
    df_stat_item = df_stat.loc[[item_index]]
    df_stat_item.fillna(value='nan',inplace=True)

    def_query = df_stat_item['Definition_Query'].iloc[0].strip()

    def_query = def_query.strip()
    
    if def_query == 'nan':
        def_query = " "
        
    else:
       def_query = def_query.replace('"', '')
       def_query = re.sub(r'(\bAND\b)', r'\1 b.', def_query)
       def_query = re.sub(r'(\bOR\b)', r'\1 b.', def_query)
       
       if def_query[0] == "(":
           def_query = def_query.replace ("(", "(b.") 
           def_query = "(" + def_query + ")"
       else:
           def_query = "b." + def_query
    
       def_query = 'AND (' + def_query + ')'
    
    
    return def_query



def get_radius (item_index, df_stat):
    """Returns the buffer distance (if any) from the AST common datasets spreadsheet"""
    #df_stat = df_stat.loc[df_stat['Featureclass_Name(valid characters only)'] == item]
    df_stat_item = df_stat.loc[[item_index]]
    df_stat_item.fillna(value=0,inplace=True)
    df_stat_item['Buffer_Distance'] = df_stat_item['Buffer_Distance'].astype(int)
    radius = df_stat_item['Buffer_Distance'].iloc[0]
    
    return radius



def load_queries ():
    sql = {}

    sql ['aoi'] = """
                    SELECT SDO_UTIL.TO_WKTGEOMETRY(a.SHAPE) SHAPE
                    
                    FROM  WHSE_TANTALIS.TA_CROWN_TENURES_SVW a
                    
                    WHERE a.CROWN_LANDS_FILE = :file_nbr
                        AND a.DISPOSITION_TRANSACTION_SID = :disp_id
                        AND a.INTRID_SID = :parcel_id
                  """
                        
    sql ['geomCol'] = """
                    SELECT column_name GEOM_NAME
                    
                    FROM  ALL_SDO_GEOM_METADATA
                    
                    WHERE owner = :owner
                        AND table_name = :tab_name
                        
          
                    """    
                    
    sql ['srid'] = """
                    SELECT s.{geom_col}.sdo_srid SP_REF
                    FROM {tab} s
                    WHERE rownum = 1
                   """
                           
    sql ['overlay'] = """
                    SELECT {cols},
                    
                           CASE WHEN SDO_GEOM.SDO_DISTANCE(b.{geom_col}, a.SHAPE, 0.5) = 0 
                            THEN 'INTERSECT' 
                             ELSE 'Within ' || TO_CHAR({radius}) || ' m'
                              END AS RESULT,
                              
                           SDO_UTIL.TO_WKTGEOMETRY(b.{geom_col}) SHAPE
                    
                    FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a, {tab} b
                    
                    WHERE a.CROWN_LANDS_FILE = :file_nbr
                        AND a.DISPOSITION_TRANSACTION_SID = :disp_id
                        AND a.INTRID_SID = :parcel_id
                        
                        AND SDO_WITHIN_DISTANCE (b.{geom_col}, a.SHAPE,'distance = {radius}') = 'TRUE'
                        
                        {def_query}  
                    """ 
                                       
    sql ['overlay_wkb'] = """
                    SELECT {cols},
                    
                           CASE WHEN SDO_GEOM.SDO_DISTANCE(b.{geom_col}, SDO_GEOMETRY(:wkb_aoi, :srid_t), 0.5) = 0 
                            THEN 'INTERSECT' 
                             ELSE 'Within ' || TO_CHAR({radius}) || ' m'
                              END AS RESULT,
                              
                           SDO_UTIL.TO_WKTGEOMETRY(b.{geom_col}) SHAPE
                    
                    FROM {tab} b
                    
                    WHERE SDO_WITHIN_DISTANCE (b.{geom_col}, 
                                               SDO_GEOMETRY(:wkb_aoi, :srid),'distance = {radius}') = 'TRUE'
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



def get_geom_srid (connection,cursor,table,geom_col,sridQuery):
    """ Returns the SRID of the BCGW table"""

    sridQuery = sridQuery.format(tab=table,geom_col=geom_col)
    df_s = read_query(connection,cursor,sridQuery,{})
    
    srid_t = df_s['SP_REF'].iloc[0]

    return srid_t



def make_status_map (gdf_aoi, gdf_intr, col_lbl, item, workspace):
    """ Generates HTML Interactive maps of AOI and intersection geodataframes"""
    
    m = folium.Map(tiles='openstreetmap')
    xmin,ymin,xmax,ymax = gdf_aoi.to_crs(4326)['geometry'].total_bounds
    m.fit_bounds([[ymin, xmin], [ymax, xmax]])

    gdf_aoi.explore(
         m=m,
         tooltip= False,
         style_kwds=dict(fill= False, color="red", weight=3),
         name="AOI")

    gdf_intr.explore(
         m=m,
         column= col_lbl, 
         tooltip= col_lbl, 
         popup=True, 
         cmap="Dark2",  
         style_kwds=dict(color="gray"),
         name=item)
	
    folium.TileLayer('stamenterrain', control=True).add_to(m)
    folium.LayerControl().add_to(m)
    
    maps_dir = os.path.join(workspace,'maps')
    if not os.path.exists(maps_dir):
        os.makedirs(maps_dir)
        
    out_html = os.path.join(maps_dir, item +'.html')
    m.save(out_html)
 


def write_xlsx (results,df_stat,workspace):
    """Writes results to a spreadsheet"""
    df_res= df_stat[['Category', 'Featureclass_Name(valid characters only)']]   
    df_res.rename(columns={'Featureclass_Name(valid characters only)': 'item'}, inplace=True)
    df_res['List of conflicts'] = ""
    df_res['Map'] = ""
    
    for index, row in df_res.iterrows():
        for k, v in  results.items():
            if row['item'] == k and v.shape[0]>0:
                v = v.drop('RESULT', axis=1)
                v['Result'] = v[v.columns].apply(lambda row: ','.join(row.values.astype(str)), axis=1)
                res_all = " ; ".join (str(x) for x in v['Result'].to_list()) 
                df_res.loc[index, 'List of conflicts'] = res_all
                df_res.loc[index, 'Map'] = '=HYPERLINK("{}", "View Map")'.format(os.path.join(workspace,'maps',k+'.html'))

    filename = os.path.join(workspace, 'AST_lite_TAB3.xlsx')
    sheetname = 'Conflicts & Constraints'
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')        
    df_res.to_excel(writer, sheet_name=sheetname, index=False, startrow=0 , startcol=0)
    
    workbook=writer.book
    worksheet = writer.sheets[sheetname]
    
    txt_format = workbook.add_format({'text_wrap': True})
    lnk_format = workbook.add_format({'underline': True, 'font_color': 'blue'})
    worksheet.set_column(0, 0, 30)
    worksheet.set_column(1, 1, 60)
    worksheet.set_column(2, 2, 80, txt_format)
    worksheet.set_column(3, 3, 20)
    
    worksheet.conditional_format('D2:D{}'.format (df_res.shape[0]+1), 
                                 {'type': 'cell',
                                  'criteria' : 'equal to', 
                                  'value' : '"View Map"',
                                  'format' : lnk_format})
    
    col_names = [{'header': col_name} for col_name in df_res.columns]
    worksheet.add_table(0, 0, df_res.shape[0]+1, df_res.shape[1]-1,{'columns': col_names})
    
    writer.save()
    writer.close()


    
def execute_status ():
    """Executes the AST light process """
    start_t = timeit.default_timer() #start time
    
    #user inputs
    workspace = r"\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\TOOLS\SCRIPTS\STATUSING\results_demo"
    aoi = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\TOOLS\SCRIPTS\STATUSING\test_data\aoi_test.shp'
    input_src = 'AOI' # Possible values are "TANTALIS" and AOI
    
    
    print ('Connecting to BCGW.')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    #bcgw_user = 'XXXX'
    bcgw_pwd = os.getenv('bcgw_pwd')
    #bcgw_pwd = 'XXXX'
    connection, cursor = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ('\nLoading SQL queries')
    sql = load_queries ()
    
    
    print ('\nReading User inputs: AOI.')
    
    if input_src == 'AOI':
        print('....Reading the AOI file')
        gdf_aoi = esri_to_gdf (aoi)
    
        if gdf_aoi.shape[0] > 1:
            gdf_aoi =  multipart_to_singlepart(gdf_aoi)
        
        wkb_aoi, srid = get_wkb_srid (gdf_aoi)
        
        
    elif input_src == 'TANTALIS':
        in_fileNbr = '1413583'
        in_dispID = 892661
        in_prclID = 911845
        print ('....input File Number: {}'.format(in_fileNbr))
        print ('....input Disposition ID: {}'.format(in_dispID))
        print ('....input Parcel ID: {}'.format(in_prclID))
        
        bvars_aoi = {'file_nbr': in_fileNbr,
                     'disp_id': in_dispID, 'parcel_id': in_prclID}
        
        df_aoi= read_query(connection,cursor,sql ['aoi'],bvars_aoi) 
        
        if df_aoi.shape[0] < 1:
            raise Exception('Parcel not in TANTALIS. Please check inputs!')
            
        else:
            gdf_aoi = df_2_gdf (df_aoi, 3005)
    
                
    else:
        raise Exception('Possible input sources are TANTALIS and AOI!')
    
    print ('\nReading the AST datasets spreadsheet.')
    wksp_xls = r'\\GISWHSE.ENV.GOV.BC.CA\whse_np\corp\script_whse\python\Utility_Misc\Ready\statusing_tools_arcpro\statusing_input_spreadsheets'
    region = 'west_coast' #**************USER INPUT: REGION*************
    print ('....Region is {}'.format (region))
    df_stat = read_input_spreadsheets (wksp_xls,region)
    
    
    
    print ('\nRunning the analysis.')
    results = {} # this dictionnary will hold the overlay results
    
    item_count = df_stat.shape[0]
    counter = 1
    for index, row in df_stat.iterrows():
        item = row['Featureclass_Name(valid characters only)']
        item_index = index
        
        print ('\n****working on item {} of {}: {}***'.format(counter,item_count,item))
        
        print ('.....getting table and column names')
        table, cols, col_lbl = get_table_cols (item_index,df_stat)
        
        print ('.....getting definition query (if any)')
        def_query = get_def_query (item_index,df_stat)
    
        print ('.....getting buffer distance (if any)')
        radius = get_radius (item_index, df_stat)  
         
        print ('.....running Overlay Analysis.')
        
        if table.startswith('WHSE') or table.startswith('REG'): 
            geomQuery = sql ['geomCol']
            sridQuery = sql ['srid']
            geom_col = get_geom_colname (connection,cursor,table,geomQuery)
            
            try:
                srid_t = get_geom_srid (connection,cursor,table,geom_col,sridQuery) 
            except:
                srid_t = 3005
            
            if input_src == 'TANTALIS':
                query= sql ['overlay'].format (cols=cols,tab=table,radius=radius,
                                                 geom_col=geom_col,def_query=def_query)
                bvars_intr = {'file_nbr':in_fileNbr,
                              'disp_id':in_dispID,'parcel_id': in_prclID}
            else:
                query= sql ['overlay_wkb'].format (cols=cols,tab=table,radius=radius,
                                                     geom_col=geom_col,def_query=def_query)
                cursor.setinputsizes(wkb_aoi=cx_Oracle.BLOB) # set the WKB as oracle BLOB
                bvars_intr = {'wkb_aoi':wkb_aoi,'srid':srid,'srid_t':str(srid_t)}
                
            df_all= read_query(connection,cursor,query,bvars_intr) 
            
                
        else:
            try:
                gdf_trg = esri_to_gdf (table)
                
                if not gdf_trg.crs.to_epsg() == 3005:
                    gdf_trg = gdf_trg.to_crs({'init': 'epsg:3005'})
                    
                gdf_intr = gpd.overlay(gdf_aoi, gdf_trg, how='intersection')
                
                
                # TEMPORARY FIX:  for Empty/Wrong column names in the REGION AST input spreadsheet
                gdf_cols = [col for col in gdf_trg.columns]  
                diffs = list(set(cols).difference(gdf_cols))
                for diff in diffs:
                    cols.remove(diff)
                if len(cols) ==0:
                    cols.append(gdf_trg.columns[0])
                 
                df_intr = pd.DataFrame(gdf_intr)
                df_intr ['RESULT'] = 'INTERSECT'
                
                if radius > 0:
                    aoi_buf = gdf_aoi.buffer(radius)
                    gdf_aoi_buf = gpd.GeoDataFrame(gpd.GeoSeries(aoi_buf))
                    gdf_aoi_buf = gdf_aoi_buf.rename(columns={0:'geometry'}).set_geometry('geometry')
                    gdf_aoi_buf_ext = gpd.overlay(gdf_aoi, gdf_aoi_buf, how='symmetric_difference')  
                    gdf_buf= gpd.overlay(gdf_aoi_buf_ext, gdf_trg, how='intersection')
                    
                    df_buf = pd.DataFrame(gdf_buf)
                    df_buf ['RESULT'] = 'WITHIN {} m'.format(str(radius))   
                    
                    df_all =  pd.concat([df_intr, df_buf])
                    
                else:
                    df_all = df_intr
                
                df_all.rename(columns={'geometry':'SHAPE'},inplace=True)
                
            except:
                print ('.......ERROR: the Source Dataset does NOT exist!')
                df_all = pd.DataFrame([])
        
        
        if isinstance(cols, str) == True:
            l = cols.split(",")
            cols = [x[2:] for x in l]
    
        cols.append('RESULT')
        
        df_all_res = df_all[cols]  
        
        
        ov_nbr = df_all_res.shape[0]
        print ('.....number of overlaps: {}'.format(ov_nbr))
        
        # add the dataframe to the resuls dictionnary
        results[item] =  df_all_res
    
    
        if ov_nbr > 0:
            print ('.....generating a map.')
            gdf_intr = df_2_gdf (df_all, 3005)
            
            # FIX FOR MISSING LABEL COLUMN NAME
            if col_lbl == 'nan': 
                col_lbl = cols[0]
                gdf_intr [col_lbl] = gdf_intr [col_lbl].astype(str)
            
            # datetime columns are causing errors when plotting in Folium. Converting them to str
            for col in gdf_intr.columns:
                if gdf_intr[col].dtype == 'datetime64[ns]':
                    gdf_intr[col] = gdf_intr[col].astype(str)
            
            gdf_intr[col_lbl] = gdf_intr[col_lbl].astype(str) 
            
            make_status_map (gdf_aoi, gdf_intr, col_lbl, item, workspace)
    
        
        counter += 1
    
    print ('\nWriting Results to spreadsheet')
    write_xlsx (results,df_stat,workspace)
    
    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print ('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))
        
    return results
              

results = execute_status()
