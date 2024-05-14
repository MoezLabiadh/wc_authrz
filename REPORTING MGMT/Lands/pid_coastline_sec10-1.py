import warnings
warnings.simplefilter(action='ignore')

import os
import json
import cx_Oracle
import pandas as pd



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
    
    print ('Connect to BCGW')    
    # Connect to the Oracle database
    Oracle = OracleConnector()
    Oracle.connect_to_db()
    orcCnx= Oracle.connection
    
    try:
        print('\nRun query')
        sql= """
        SELECT
            pfct.pid,
            pfct.PARCEL_STATUS,
            pfct.PARCEL_CLASS,
            pfct.OWNER_TYPE,
            ROUND(SDO_GEOM.SDO_DISTANCE(cst.GEOMETRY, pfct.SHAPE, 0.005)) DISTANCE_TO_COASTLINE_METER
            
        FROM 
            WHSE_BASEMAPPING.FWA_COASTLINES_SP cst
            JOIN (SELECT
                        pf.pid,
                        pf.PARCEL_STATUS,
                        pf.PARCEL_CLASS,
                        pf.OWNER_TYPE,
                        pf.SHAPE
                  FROM WHSE_CADASTRE.PMBC_PARCEL_FABRIC_POLY_SVW pf
                    JOIN WHSE_TANTALIS.TA_CROWN_TENURES_SVW ten
                        ON SDO_RELATE (pf.SHAPE, ten.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                            AND ten.CROWN_LANDS_FILE = '1415239' 
                    ) pfct
                    
                ON SDO_WITHIN_DISTANCE (cst.GEOMETRY, pfct.SHAPE, 'distance=30 unit=m') = 'TRUE'
                AND pfct.OWNER_TYPE= 'Private'
            """
        
        df= pd.read_sql(sql, orcCnx)
        df.sort_values(by=['DISTANCE_TO_COASTLINE_METER'], inplace=True)
        df.drop_duplicates(subset=['PID'], keep='first', inplace=True)
        

    except Exception as e:
        raise Exception(f"Error occurred: {e}")  

    finally: 
        Oracle.disconnect_db()
        
        

    outloc= r'W:\lwbc\visr\Workarea\moez_labiadh\WORKSPACE_2024\20240514_privateLands_Coastline_sec101'
    filename= 'PIDs_coastline_sec10-1'
    generate_report (outloc, [df], ['list'], filename)

    