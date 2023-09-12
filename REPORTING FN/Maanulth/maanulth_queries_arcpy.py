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



def df_to_featureclass(output_gdb, geometry_type, feature_class_name, data_frame):
    """Create a feature class based on pandas dataframe (with geometry info)."""
    
    #Create an empty feature class in the output gdb
    spatial_reference = arcpy.SpatialReference(3005) # BCalbers
    arcpy.CreateFeatureclass_management(output_gdb, feature_class_name, 
                                        geometry_type, spatial_reference=spatial_reference)

    # Add fields from the df to the feature class
    feature_class= os.path.join(output_gdb, feature_class_name)
    fields = data_frame.columns.tolist()
    fields.remove('SHAPE') #remove the geometry field from the list of attributes

    for field_name in fields:
        arcpy.AddField_management(feature_class, field_name, "TEXT")

    # Populate data (geometry+attributes) from df to feature class
    with arcpy.da.InsertCursor(feature_class, ['SHAPE@WKT'] + fields) as cursor:
        for index, row in data_frame.iterrows():
            feature = row['SHAPE']
            values = [row[field] for field in fields]
            rowdata= [feature] + values
            cursor.insertRow(rowdata)    



def main():
    print ('Connect to BCGW.')
    # connection parmaters to BCGW
    driver = 'Oracle in OraClient12Home1'
    server = 'bcgw.bcgov'
    port= '1521'
    dbq= 'idwprod1'
    hostname = 'bcgw.bcgov/idwprod1.bcgov'

    username= 'XXXXXXXXXX' #BCGW Username ################## CHANGE THIS################
    password= 'XXXXXXXXXX' #BCGW Password ################## CHANGE THIS################

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
            feature_class_name = k

            # convert the df to feature class
              ## determine the geometry type.
            wkt_str = df.loc[0, 'SHAPE']

            shape_value = wkt_str.split(" ")[0]
            if shape_value in ['POLYGON', 'MULTIPOLYGON']:
              geometry_type= "POLYGON"
            elif shape_value in ['LINESTRING','MULTILINESTRING']:
                geometry_type= "POLYLINE"
            elif shape_value in ['POINT ','MULTIPOINT']:
                geometry_type= "POINT"

            df_to_featureclass(output_gdb, geometry_type, feature_class_name, df)

        counter+= 1

    print ("\n Processign Completed!")



main()
