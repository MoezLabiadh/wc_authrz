import warnings
warnings.simplefilter(action='ignore')

import os
import json
import cx_Oracle
import pandas as pd
import geopandas as gpd
from shapely import wkb
from datetime import datetime


class OracleConnector:
    def __init__(self, dbname='BCGW'):
        self.dbname = dbname
        self.cnxinfo = self.get_db_cnxinfo()

    def get_db_cnxinfo(self):
        """ Retrieves db connection params from the config file"""
        with open(r'H:\config\db_config.json', 'r') as file:
            data = json.load(file)
        
        if self.dbname in data:
            return data[self.dbname]
        
        raise KeyError(f"Database '{self.dbname}' not found.")
    
    def connect_to_db(self):
        """ Connects to Oracle DB and create a cursor"""
        try:
            self.connection = cx_Oracle.connect(self.cnxinfo['username'], 
                                                self.cnxinfo['password'], 
                                                self.cnxinfo['hostname'], 
                                                encoding="UTF-8")
            self.cursor = self.connection.cursor()
            print  ("..Successffuly connected to the database")
        except Exception as e:
            raise Exception(f'..Connection failed: {e}')

    def disconnect_db(self):
        """Close the Oracle connection and cursor"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()
            print("....Disconnected from the database")
            

def read_query(connection,cursor,query,bvars):
    "Returns a df containing SQL Query results"
    cursor.execute(query, bvars)
    names = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=names)
    
    return df  


def get_input_shapes(in_folder):
    """Returns wkb dictionary of shapes"""
    wkb_dict = {}
    for root, dirs, files in os.walk(in_folder):
        for filename in files:
            if filename.endswith(".shp"):
                print(f'..processing {filename}')
                lyr = os.path.splitext(filename)[0]
                if 'Mar5' in lyr:
                    n= lyr[:-5]
                else :
                    n= lyr[:-6]
                
                filepath = os.path.join(root, filename)
                gdf = gpd.read_file(filepath)
                gdf['diss'] = 1
                gdf = gdf.dissolve(by='diss')
                gdf.reset_index(inplace=True)
                

                geom = gdf['geometry'].iloc[0]
                wkb_aoi = wkb.dumps(geom, output_dimension=2)

                wkb_dict[n] = wkb_aoi

    return wkb_dict


def load_Orc_sql():
    orSql= {}
          
    orSql['aqua']="""
        SELECT
            CROWN_LANDS_FILE,
            DISPOSITION_TRANSACTION_SID,
            INTRID_SID,
            TENURE_STAGE,
            TENURE_STATUS,
            TENURE_TYPE,
            TENURE_SUBTYPE,
            TENURE_PURPOSE,
            TENURE_SUBPURPOSE,
            
            ROUND(SDO_GEOM.SDO_AREA(
                SHAPE, 0.005, 'unit=HECTARE'), 6) AS TENURE_AREA_HA,
        
            ROUND(SDO_GEOM.SDO_AREA(
                SDO_GEOMETRY(:wkb_aoi, 3005), 0.005, 'unit=HECTARE'), 6) AS CNSRV_AREA_HA,
        
            ROUND(SDO_GEOM.SDO_AREA(
                SDO_GEOM.SDO_INTERSECTION(
                    SHAPE, SDO_GEOMETRY(:wkb_aoi, 3005), 0.005), 0.005, 'unit=HECTARE'), 6) OVERLAP_AREA_HA,
        
            ROUND((SDO_GEOM.SDO_AREA(
                SDO_GEOM.SDO_INTERSECTION(
                  SHAPE, SDO_GEOMETRY(:wkb_aoi, 3005), 0.005), 0.005,'unit=HECTARE') /
                     SDO_GEOM.SDO_AREA(
                       SHAPE, 0.005, 'unit=HECTARE')) * 100, 6) AS OVERLAP_PERCENTAGE
            
        FROM
            WHSE_TANTALIS.TA_CROWN_TENURES_SVW
        
        WHERE
            TENURE_PURPOSE= 'AQUACULTURE'
            AND SDO_RELATE (SHAPE, 
                          SDO_GEOMETRY(:wkb_aoi, 3005),'mask=ANYINTERACT') = 'TRUE'
        """     
        
    return orSql


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
    
    

if __name__ == "__main__":
    
    print ('Connecting to BCGW.')
    Oracle = OracleConnector()
    Oracle.connect_to_db()
    connection= Oracle.connection
    cursor= Oracle.cursor
    
    print ('Reading inputs')
    wks= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240410_aquaTenures_conservancies'
    inloc= r'W:\srm\nr\NEGSS\ENV\BCParks'
    wkb_dict= get_input_shapes (inloc)
    
    print ('Executing queries')
    orSql= load_Orc_sql()
    
    dfs= []
    for k, v in wkb_dict.items():
        print (f'...working on {k}')
        cursor.setinputsizes(wkb_aoi=cx_Oracle.BLOB)
        bvars = {'wkb_aoi':v}
        df = read_query(connection,cursor,orSql['aqua'],bvars)
        
        df.insert(0, 'CNSRV_NAME', k)
        
        if df.shape[0] >0:
            dfs.append(df)
        
    df_all= pd.concat(dfs)
    #df_all.reset_index(drop=True, inplace=True)
    
    print ('Exporting Results')
    today = datetime.today().strftime('%Y%m%d')
    filename= today + '_aquaTenures_propConservancies_overlaps'
    generate_report (wks, [df_all], ['results'],filename)
    
    
    