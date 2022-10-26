import os
import arcpy
import pandas as pd
import numpy as np
from datetime import datetime


def prep_df (f):
    """Returns a df with formatted column names based on the Water apps ledger"""
    df = pd.read_excel (f, 'Existing Use Applications', index_col=False,
                        converters= {'File\nNumber':str})
    df.rename(columns={'File\nNumber':'File Number'}, inplace=True)
    df.columns = df.columns.str.replace(' ', '_')
    df['DATE_RECEIVED'] = df['DATE_RECEIVED'].astype(str)

    return df

def df2gdb (df):
    """Converts a pandas df to a gbd table"""
    #Turn dataframe into a simple np series
    arr = np.array(np.rec.fromrecords(df.values))

    #Create a list of field names from the dataframe
    colnames = [name.encode('UTF8') for name in df.dtypes.index.tolist()]

    #Update column names in structured array
    arr.dtype.names = tuple(colnames)
    #arcpy.AddMessage (arr.dtype)
    #Create the GDB table
    #table = 'in_memory\df_table'
    arcpy.da.NumPyArrayToTable(arr, table)

    return table

def create_point_lyr (out_loc, table,todayDate):
    """Converts an xy table to a Shapefile"""
    outLayer = 'in_memory\_xy_layer'
    spRef = arcpy.SpatialReference(4326)
    outShp = os.path.join(out_loc, 'existingUse_gwApps_asof{}.shp'.format(todayDate))
    outKml = os.path.join(out_loc, 'existingUse_gwApps_asof{}.kml'.format(todayDate))
    arcpy.MakeXYEventLayer_management(table, 'LONGITUDE', 'LATITUDE', outLayer, spRef)
    arcpy.CopyFeatures_management(outLayer, outShp)
    arcpy.LayerToKML_conversion (outLayer, out_kmz_file)

    return outShp


def update_map (mxd_loc,out_loc,outShp,todayDate):
    """Generates a new map of Water Applications"""
    mxd = arcpy.mapping.MapDocument(mxd_loc)
    frames = arcpy.mapping.ListDataFrames(mxd)

    for frame in frames:
        lyrs = arcpy.mapping.ListLayers(mxd, "", frame)

        for lyr in lyrs:
            if lyr.name == 'Exisiting Use Applications':
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


def main():
    arcpy.env.overwriteOutput = True
    #workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20220916_waterLicencing_support\TESTS'
    #f = os.path.join(workspace,'Water Application Ledger_tests.xlsx')
    f =  arcpy.GetParameterAsText(0)
    out_loc = arcpy.GetParameterAsText(1)

    todayDate = datetime.today().strftime("%Y%m%d")

    #print ('Formatting the Work Ledger Spreadsheet...')
    arcpy.AddMessage ('Formatting the Work Ledger Spreadsheet...')
    #df= prep_df (f)

    print ('Converting to xy table...')
    arcpy.AddMessage ('Converting to xy table...')

    # DEBUG THIS: works in staandalone. Doesent work in toolbox.
    #table= df2gdb (df)



    table= os.path.join(out_loc, 'table_gw.dbf')
    arcpy.ExcelToTable_conversion (f, table, 'Existing Use Applications')





    print ('Creating Spatial Files...')
    arcpy.AddMessage ('Creating Spatial Files...')
    outShp = create_point_lyr (out_loc, table, todayDate)
    arcpy.management.Delete(table)

    print ('Updating the Water Applications map...')
    arcpy.AddMessage ('Creating the Water Applications map (THIS MIGHT TAKE A WHILE!)')
    mxd_loc = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\TEMPLATES\Water_Licencing\template_waterApps_map_existingUse.mxd'
    update_map (mxd_loc,out_loc,outShp,todayDate)

    arcpy.Delete_management('in_memory')

    print('Finished Processing!')
    arcpy.AddMessage('Finished Processing!')

main()
