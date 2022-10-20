import warnings
warnings.simplefilter(action='ignore')

import os
import cx_Oracle
import numpy as np
import pandas as pd
import datetime as dt
import geopandas as gpd


def get_titan_report_date (rpt009):
    """ Returns the date of the input TITAN report"""
    df = pd.read_excel(rpt009,'Info')
    titan_date = df.columns[1].strftime("%Y%m%d")
   
    return titan_date


def load_reports (rpt009, rpt011):
    """Loads TITAN Reports"""
    df_rpt11 = pd.read_excel (rpt011,'TITAN_RPT011',
                                  converters={'FILE #':str})

    df_rpt09 = pd.read_excel (rpt009,'TITAN_RPT009',
                                  converters={'FILE NUMBER':str})
    
    df_rpt11.rename(columns={'FILE #':'FILE NUMBER'}, inplace=True)
    
    return df_rpt11, df_rpt09 


def get_expired_tenures (df_rpt11, df_rpt09):
    """Retireve Expired tenures and information"""
    df_dig = df_rpt11.loc[(df_rpt11['STATUS'] == 'DISPOSITION IN GOOD STANDING')]
        
    
    df_rep = df_rpt09.loc[(df_rpt09['TASK DESCRIPTION'] == 'REPLACEMENT APPLICATION') &
                          (df_rpt09['STATUS'] == 'ACCEPTED') &
                          (df_rpt09['COMPLETED DATE'].isnull())]
    
    df_rep.drop('EXPIRY DATE', axis=1, inplace=True)
    
    df_exp = df_rpt11.loc[(df_rpt11['STAGE'] == 'TENURE') &
                          (df_rpt11['STATUS'] == 'EXPIRED') &
                          (~df_rpt11['FILE NUMBER'].isin(df_dig['FILE NUMBER'].tolist()))]
    
    df_exp.sort_values(by='EXPIRY DATE', ascending=False, inplace= True)
    df_exp.drop_duplicates(subset=['FILE NUMBER'], keep='first', inplace= True )
    df_exp.drop('RECEIVED DATE', axis=1, inplace=True)
    df_exp = df_exp[['FILE NUMBER','EXPIRY DATE', 'RENT','INTEREST PARCEL ID']]
    
    df_exp = df_exp.loc[(df_exp['FILE NUMBER'].isin(df_rep['FILE NUMBER'].tolist()))]
    
    df = pd.merge(df_exp, df_rep, how='left', on= 'FILE NUMBER')
    
    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
    df.loc[df['PURPOSE'] == 'AQUACULTURE', 'DISTRICT OFFICE'] = 'AQUA'
    
    df.rename(columns={'FDISTRICT':'GEOGRAPHIC LOCATION',
                             'RENT': 'MOST RECENT RENTAL AMOUNT'}, inplace=True)

    return df

def add_cols (df):
    """Adding informations forr the report"""
    df['QUEUE'] = 'YES'
    
    df['RECEIVED DATE'] =  pd.to_datetime(df['RECEIVED DATE'],
                                     infer_datetime_format=True,
                                     errors = 'coerce').dt.date
    df['LAND STATUS DATE'] =  pd.to_datetime(df['LAND STATUS DATE'],
                                     infer_datetime_format=True,
                                     errors = 'coerce').dt.date
    
    df ['TODAY']  = dt.datetime.combine(dt.date.today(), dt.datetime.min.time())
    df ['DAYS SINCE EXPIRY'] = df ['TODAY'] - df['EXPIRY DATE']
    
    df ['YRS SINCE EXPIRY'] = df ['DAYS SINCE EXPIRY']/ np.timedelta64(1,'Y')
    df ['YRS SINCE EXPIRY']= df ['YRS SINCE EXPIRY'].apply(np.floor)
    df ['UNBILLED USE OF CROWN LAND'] = df ['YRS SINCE EXPIRY'] * df ['MOST RECENT RENTAL AMOUNT'] 
    
    df ['DAYS SINCE EXPIRY'] = (df ['DAYS SINCE EXPIRY'] / np.timedelta64(1, 'D')).astype(int)
    
    
    df ['PERIOD OF EXPIRY'] = 'N/A'
    
    for i, row in df.iterrows():
        d = row['DAYS SINCE EXPIRY']
        if 1 < d <= 30:
            df.at[i,'PERIOD OF EXPIRY'] = '1-30 days past Expiry'       
        elif 1 < d < 30:
            df.at[i,'PERIOD OF EXPIRY'] = '1-30 days past Expiry'            
        elif 30 < d <= 120:
            df.at[i,'PERIOD OF EXPIRY'] = '30-120 days past Expiry'
        elif 120 < d < 365:
            df.at[i,'PERIOD OF EXPIRY'] = '120-365 days past Expiry'        
        elif 365 < d <= 730:
            df.at[i,'PERIOD OF EXPIRY'] = '1-2 Yrs past Expiry'  
        elif 730 < d <= 1095:
            df.at[i,'PERIOD OF EXPIRY'] = '2-3 Yrs past Expiry'  
        elif d > 1095:
            df.at[i,'PERIOD OF EXPIRY'] = '>3 Yrs past Expiry' 
            
    #cleanup columns
    df['EXPIRY DATE'] =  pd.to_datetime(df['EXPIRY DATE'],
                               infer_datetime_format=True,
                               errors = 'coerce').dt.date
    return df


def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


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
        f = str(row['Name']) # Replace index with another ID column (name ?)
        wkt = row['geometry'].wkt
       # print ('Original WKT size is {}'.format(len(wkt)))
    
        if len(wkt) < 4000:
            #print ('{} - FULL WKT returned: within Oracle VARCHAR limit'.format(f)) 
            wkt_dict [f] = wkt
            
        else:
            #print ('Geometry will be Simplified for {} - beyond Oracle VARCHAR limit'.format (f))
            s = 50
            wkt_sim = row['geometry'].simplify(s).wkt

            while len(wkt_sim) > 4000:
                s += 10
                wkt_sim = row['geometry'].simplify(s).wkt

            #print ('Geometry Simplified with Tolerance {} m'.format (s))            
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


def proximity_model(df, wkt_dict, srid,connection,sql):
    """ Adds assignement office to the report based on proximity analysis"""
    
    df['PROXIMITY_OFFICE'] = 'N/A'
    
    for i, row in df.iterrows():
        p = row['INTEREST PARCEL ID']
        o = row['DISTRICT OFFICE']
        
        dist_dict = {}
        for k, v in wkt_dict.items():
            query = sql.format(wkt= v,  srid= srid, prcl=p)
            df_q = read_query(connection,query)
            dist_dict[k] = df_q['PROXIMITY_METERS'].iloc[0]
            
        prox_off = min(dist_dict, key=dist_dict.get)
        
        if o != 'AQUA':
            df.at[i,'PROXIMITY OFFICE'] = prox_off
        else:
            df.at[i,'PROXIMITY OFFICE'] = 'AQUA'
        
    return df
        

def generate_report (workspace, df_list, sheet_list,filename):
    """ Exports dataframes to multi-tab excel spreasheet"""
    file_name = os.path.join(workspace, filename+'.xlsx')

    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')

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
    workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20221017_expiredTenures_report_Shawn'
    aoi = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\DATASETS\local_data.gdb\Admin\district_regional_areas'
    rpt011 = os.path.join(workspace, 'TITAN_RPT011.xlsx')
    rpt009 = os.path.join(workspace, 'TITAN_RPT009.xlsx')
    
    print ('Loading Titan Reports...')
    titan_date = get_titan_report_date (rpt009)
    df_rpt11, df_rpt09 = load_reports (rpt009, rpt011)
    
    print ('Retrieving Expired Tenures...')
    df = get_expired_tenures (df_rpt11, df_rpt09 )
    
    print ('Adding information...')
    df = add_cols (df)

    print ('Connecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
    
    print ('Reading the input zoning file...')
    gdf = esri_to_gdf (aoi)
    
    print ('Getting WKT and SRID...')
    wkt_dict, srid = get_wkt_srid (gdf)
    
    sql = """
            SELECT SDO_GEOM.SDO_DISTANCE(ten.SHAPE, 
                                         SDO_GEOMETRY('{wkt}', {srid}), 
                                         0.005, 'unit=meter') PROXIMITY_METERS 
            FROM WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES ten
            WHERE ten.INTRID_SID = {prcl}
          """
    
    print ('Running the Proximity analysis..')
    df= proximity_model(df, wkt_dict, srid,connection,sql)
    
    cols = ['FILE NUMBER', 'GEOGRAPHIC LOCATION', 'DISTRICT OFFICE', 'PROXIMITY OFFICE',
            'USERID ASSIGNED WORK UNIT', 'CLIENT NAME', 'LOCATION',
            'TYPE', 'SUBTYPE', 'PURPOSE', 'SUBPURPOSE', 'RECEIVED DATE',
            'PRIORITY CODE', 'QUEUE','COMMENTS', 'LAND STATUS DATE',
            'EXPIRY DATE', 'YRS SINCE EXPIRY', 'PERIOD OF EXPIRY', 
            'MOST RECENT RENTAL AMOUNT', 'UNBILLED USE OF CROWN LAND']

    df = df[cols]
    
    print ('Generating the report...')
    filename = 'replacement_report_{}'.format(titan_date)
    generate_report (workspace, [df], ['REPLACEMETS'],filename)
    
    print ('Processing Completed!')
    
main()
