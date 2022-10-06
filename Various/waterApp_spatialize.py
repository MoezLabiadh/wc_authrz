import os
import arcpy
import pandas as pd
import numpy as np
from datetime import datetime


def prep_df (f):
    """Returns a df with formatted column names based on the Water apps ledger"""
    df = pd.read_excel (f, 'Active Applications', index_col=False,
                        converters= {'File\nNumber':str})
    df.rename(columns={'File\nNumber':'File Number'}, inplace=True)
    df.columns = df.columns.str.replace(' ', '_')
    df['Decision_Due_Date'] = df['Decision_Due_Date'].astype(str)
    df['Application_Date'] = df['Application_Date'].astype(str)

    return df

def df2gdb (df):
    """Converts a pandas df to a gbd table"""
    #Turn dataframe into a simple np series
    arr = np.array(np.rec.fromrecords(df.values))

    #Create a list of field names from the dataframe
    colnames = [name.encode('UTF8') for name in df.dtypes.index.tolist()]

    #Update column names in structured array
    arr.dtype.names = tuple(colnames)
    #print (arr.dtype)
    #Create the GDB table
    table = 'in_memory\df_table'
    arcpy.da.NumPyArrayToTable(arr, table)

    return table

def create_point_lyr (out_loc, table,todayDate):
    """Converts an xy table to a Shapefile"""
    outLayer = 'in_memory\_xy_layer'
    spRef = arcpy.SpatialReference(4326)
    outShp = os.path.join(out_loc, 'waterApps_asof{}.shp'.format(todayDate))
    arcpy.MakeXYEventLayer_management(table, 'Longitude', 'Latitude', outLayer, spRef)
    arcpy.CopyFeatures_management(outLayer, outShp)

    return outShp

def update_map (mxd_loc,out_loc,outShp,todayDate):
    """Generates a new map of Water Applications"""
    mxd = arcpy.mapping.MapDocument(mxd_loc)
    frames = arcpy.mapping.ListDataFrames(mxd)

    for frame in frames:
        lyrs = arcpy.mapping.ListLayers(mxd, "", frame)

        for lyr in lyrs:
            if lyr.name == 'Water Applications':
                lyr.replaceDataSource (out_loc, 'SHAPEFILE_WORKSPACE',
                                       os.path.basename(outShp)[:-4])

    for elm in arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT"):
        if elm.name == 'date':
            dateText = datetime.today().strftime("%Y-%m-%d")
            elm.text = str(dateText)

    #mxd.save()

    outPDF = os.path.join(out_loc, 'waterApps_asof{}'.format(todayDate))
    arcpy.mapping.ExportToPDF(mxd, outPDF, resolution=150,
                              image_quality= 'BETTER', georef_info=False, jpeg_compression_quality=100)



arcpy.env.overwriteOutput = True
workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20220916_waterLicencing_support\TESTS'
f = os.path.join(workspace,'Water Application Ledger_tests.xlsx')

todayDate = datetime.today().strftime("%Y%m%d")

print ('Formatting the Work Ledger Spreadsheet...')
df= prep_df (f)

print ('Converting to xy table...')
table= df2gdb (df)

print ('Creating Spatial Files...')
out_loc = os.path.join(workspace, 'OUT')
outShp = create_point_lyr (out_loc, table, todayDate)

print ('Updating the Water Applications map...')
mxd_loc = os.path.join(workspace,'proj_test.mxd')
update_map (mxd_loc,out_loc,outShp,todayDate)

arcpy.Delete_management('in_memory')

print('Finished Processing!')
