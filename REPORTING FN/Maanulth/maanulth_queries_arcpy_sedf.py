'''
This script performs the following workflow:
1) Connect to BCGW
2) Execute the SQL queries
3) Export the query results to Feature Classes
'''

import warnings
warnings.simplefilter(action='ignore')

import os
import arcpy
import pyodbc
import pandas as pd

from arcgis.features import GeoAccessor, GeoSeriesAccessor

from queries import load_queries


def connect_to_DB (driver,server,port,dbq, username,password):
    """ Returns a connection to Oracle database"""
    try:
        connectString ="""
                    DRIVER={driver};
                    SERVER={server}:{port};
                    DBQ={dbq};
                    Uid={uid};
                    Pwd={pwd}
                       """.format(driver=driver,server=server, port=port,
                                  dbq=dbq,uid=username,pwd=password)

        connection = pyodbc.connect(connectString)
        print  ("...Successffuly connected to the database")
    except:
        raise Exception('...Connection failed! Please check your connection parameters')

    return connection



def df_to_sedf_to_fc(df, output_fc_path):
    # Convert the DataFrame to a Spatially Enabled DataFrame
    sedf = pd.DataFrame.spatial.from_df(df, sr= 3005, geometry_column='SHAPE')

    # Set the spatial ref system (BCalbers)
    #sedf.spatial.sr = arcpy.SpatialReference(3005)

    # Export the SEDF to a feature class
    sedf.spatial.to_featureclass(location=output_fc_path)


def main():
    print ('Connect to BCGW.')
    # connection parmaters to BCGW
    driver = 'Oracle in OraClient12Home1'
    server = 'bcgw.bcgov'
    port= '1521'
    dbq= 'idwprod1'
    hostname = 'bcgw.bcgov/idwprod1.bcgov'

    username= 'XXXX' #BCGW Username ################## CHANGE THIS################
    password= 'XXXX' #BCGW Password ################## CHANGE THIS################

    connection= connect_to_DB (driver,server,port,dbq,username,password)

    # reporting year
    year = 2023

    print ("\nLoad the SQL queries...")
    sql = load_queries ()

    print ("\nRun the process")
    workspace= r'\\...\arcpy_tests' # ################## CHANGE THIS################
    output_gdb = os.path.join(workspace, 'maanulth_proj.gdb')  # ################## CHANGE THIS################
    
    # iterate through the sqls, run queries and export result to featureclass
    counter= 1
    for k, v in sql.items():
        # add Year and Year-1 parameters to the sql
        print ("..working on SQL {} of {}: {}".format(counter, len(sql),k))
        query = v.format(y= year, prvy=year-1)

        # read the query into a dataframe
        print ("....executing the query")
        df= pd.read_sql(query,connection)

        nbr_rows= df.shape[0] 
        if nbr_rows == 0:
            print ('......query returned 0 results, no featureclass will be exported')
        else:
            print ("....exporting {} results to a feature class".format(nbr_rows))
            arcpy.env.overwriteOutput = True

            # set the output gdb and the feature class name
            out_fc_name = k
            out_fc= os.path.join(output_gdb, out_fc_name)
            # convert the df to feature class
            df_to_sedf_to_fc(df, out_fc)


        counter+= 1

    print ("\n Processign Completed!")



main()
