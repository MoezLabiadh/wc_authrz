import os
import cx_Oracle
import pandas as pd
import geopandas as gpd


#Hide pandas warning
pd.set_option('mode.chained_assignment', None)

def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


def filter_TITAN (titan_report):
    """Returns a df of filtered TITAN report"""
    #Read TITAN report into df
    df = pd.read_excel(titan_report, 'TITAN_RPT012',
                       converters={'FILE #':str})

    # Convert expiry date column to datetime format
    df['STATUS CHANGED DATE'] =  pd.to_datetime(df['STATUS CHANGED DATE'],
                                    infer_datetime_format=True,
                                    errors = 'coerce').dt.date
    
    df_dig = df.loc[(df['STAGE'] == 'TENURE') &
                    (df['STATUS'] == 'DISPOSITION IN GOOD STANDING')]
    
    df_app= df.loc[(df['STAGE'] == 'APPLICATION') &
                   (df['STATUS'] == 'ACCEPTED')]
    
    excld = df_dig['FILE #'].tolist() + df_app['FILE #'].tolist()


    #df['CANCELLED YEAR'] = df['STATUS CHANGED DATE'].dt.year
    #df['CANCELLED YEAR'] =pd.DatetimeIndex(df['STATUS CHANGED DATE']).year

    df_canc = df.loc[(df['STAGE'] == 'TENURE') &
                     (df['STATUS'] == 'CANCELLED') &
                     (~df['FILE #'].isin(excld))]

    df_canc['CANCELLED YEAR'] =pd.DatetimeIndex(df_canc['STATUS CHANGED DATE']).year
    df_canc.sort_values(by = ['CANCELLED YEAR'], inplace = True)
    df_canc.drop_duplicates(subset=['FILE #'], keep="last", inplace=True)


    return df_canc


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
        raise Exception ('Format not recognized. Please provide a shp or featureclass (gdb)')
    
    return gdf


def get_wkt_srid (gdf):
    """Returns the SRID and WKT string of each feature in a gdf"""
    
    #gdf['wkt'] = gdf.apply(lambda row:row['geometry'].wkt, axis=1)
    
    srid = gdf.crs.to_epsg()
    if srid != 3005:
        raise Exception ('Shape must be in BC Albers Projection!')
    
    # Generate WKT strings. 
    #If WKT string is larger then 4000 characters (ORACLE VARCHAR2 limit), 
     # OPTION A: algorithm will simplify the geometry until limit is reached.
    
    wkt_dict = {}
    for index, row in gdf.iterrows():
        f = 'feature '+ str(index) # Replace index with another ID column (name ?)
        wkt = row['geometry'].wkt
    
        if len(wkt) < 4000:
            print ('{} - FULL WKT returned: within Oracle VARCHAR limit'.format(f)) 
            wkt_dict [f] = wkt
            
        else:
            print ('Geometry will be Simplified for {} - beyond Oracle VARCHAR limit'.format (f))
            s = 50
            wkt_sim = row['geometry'].simplify(s).wkt

            while len(wkt_sim) > 4000:
                s += 10
                wkt_sim = row['geometry'].simplify(s).wkt

            print ('Geometry Simplified with Tolerance {} m'.format (s))            
            wkt_dict [f] = wkt_sim 
                
            #Option B: just generate an Envelope Geometry
            #wkt_env = row['geometry'].envelope.wkt
            #wkt_dict [f] = wkt_env

    return wkt_dict, srid


def read_query(connection,query):
    "Returns a df containing SQL Query results"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        names = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=names)
    
    finally:
        if cursor is not None:
            cursor.close()
            

def generate_report (workspace, df_list, sheet_list, filename):
    """ Exports dataframes to multi-tab excel spreasheet"""
    out_file = os.path.join(workspace, str(filename) + '.xlsx')

    writer = pd.ExcelWriter(out_file,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'count'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()


def main():
    """Runs the program"""

    workspace = r'\\...\WORKSPACE\20220713_aqua_cancelled_westCoast'
    
    print ('Connecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    
    print ("\nFiltering TITAN report...")
    titan_report = os.path.join(workspace, 'TITAN_RPT012.xlsx')
    df_canc = filter_TITAN (titan_report)
    
    print ('\nReading the input file...')
    aoi = os.path.join(workspace, 'data.gdb', 'sooke_to_capeScott')
    gdf = esri_to_gdf (aoi)   
    
    print ('\nGetting WKT and SRID...')
    wkt_dict, srid = get_wkt_srid (gdf)
    
    sql =  """
        SELECT*
        FROM WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES t
        WHERE t.INTRID_SID in ({p})
        AND SDO_RELATE (t.SHAPE, SDO_GEOMETRY('{w}', {s}),
                        'mask=ANYINTERACT') = 'TRUE'
        """
    
    print ('\nRunning SQL...')
    
    l = df_canc ['INTEREST PARCEL ID'].astype(int).tolist()
    prcls = ",".join(str(x) for x in l)

    query = sql.format(p= prcls, w= list(wkt_dict.values())[0],  s= srid)
    df_q = read_query(connection,query)
    
    wc_p = df_q['INTRID_SID'].tolist()
    
    df_wc_canc = df_canc.loc[df_canc['INTEREST PARCEL ID'].isin(wc_p)]
    
    cols = ['FILE #', 'DTID', 'STAGE', 'STATUS', 'STATUS CHANGED DATE', 'CANCELLED YEAR', 'APPLICATION TYPE',
            'TYPE', 'SUBTYPE', 'PURPOSE', 'SUBPURPOSE', 'COMMENCEMENT DATE', 'EXPIRY DATE','LOCATION', 'CLIENT NAME',
            'ADDRESS LINE 1', 'ADDRESS LINE 2','ADDRESS LINE 3','CITY', 'PROVINCE', 'POSTAL CODE','COUNTRY','STATE','ZIP CODE']
    
    df_wc_canc = df_wc_canc[cols]
    
     
    print ('\nExporting Query Results...')
    filename = 'aqua_cancelled_tenures_westCoastVI_20220714'
    generate_report (workspace, [df_wc_canc], ['List'],filename)
    

    print ('Processing completed!')


if __name__ == "__main__":
    main()


