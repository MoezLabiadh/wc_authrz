#-------------------------------------------------------------------------------
# Name:        Tenure Document map generator
# Purpose:     This script generates a Tenure document map (LDS map) - BATCH mode.
#
# Author:      MLABIADH
#
# Created:     20-01-2022
#-------------------------------------------------------------------------------

import os
import math
import arcpy
import pandas as pd


def set_defQuery (mxd,df,file_nbr,dt_id,parcel_id):
    """Locate the Teure Layer and set a Definition Query"""
    layersList = arcpy.mapping.ListLayers(mxd,"",df)

    for lyr in layersList:
        if lyr.name == 'SELECTED TENURE':
            tenure_layer = lyr

        elif lyr.name  == 'BCGS_SHEETS':
           bcgs_sheet = lyr

        else:
            pass

    defQuery = """CROWN_LANDS_FILE = '{}'
              AND DISPOSITION_TRANSACTION_SID = {}
              AND INTRID_SID = {} """.format (file_nbr,dt_id,parcel_id)

    tenure_layer.definitionQuery = defQuery

    result = arcpy.GetCount_management(tenure_layer)
    count = int(result.getOutput(0))
    if count < 1:
        raise Exception ('ERROR: Parcel not found! Check user entries.')
    else:
        pass

    return tenure_layer, bcgs_sheet


def set_scale (df, df_oview, tenure_layer):
    """ Set the map scale """
    ext = tenure_layer.getExtent()

    df.extent = ext

    # Use user entry if any otherwise calculate scale based on tenure layer extent
    scale = int (df.scale)
    scale_buf = scale*4

    thousands = scale_buf // 1000
    hundreds = (scale_buf % 1000) // 100

    if hundreds >= 6:
        rounded_scale = int(math.ceil(scale_buf / 1000.0)) * 1000
    else:
        r = 500 - (scale_buf - (thousands*1000))
        rounded_scale = scale_buf + r

    if rounded_scale > 1500:
        df.scale = rounded_scale
    else:
        df.scale = 1500

    '''
    # set scale of overview map
    df_oview.extent = ext

    if  df.scale >= 3500:
        df_oview.scale = 600000
    else:
        df_oview.scale = 500000
    '''

def populate_info (mxd,tenure_layer):
    """ Populates map text elements"""

    fields = [
                  'CROWN_LANDS_FILE', #0
                  'TENURE_AREA_IN_HECTARES', #1
                  'TENURE_TYPE', #2
                  'TENURE_SUBTYPE', #3
                  'TENURE_PURPOSE', #4
                  'TENURE_SUBPURPOSE', #5
                  'TENURE_LEGAL_DESCRIPTION' #6
                  ]

    values = []

    with arcpy.da.SearchCursor(tenure_layer, fields) as cursor:
        for row in cursor:
                values.extend([row[0],row[1],row[2],row[3],row[4],row[5],row[6]])

    val_dict = dict(zip(fields, values))

    for elm in arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT"):
        for k, v in val_dict.items():
            if elm.name == k:
                if k == 'TENURE_LEGAL_DESCRIPTION':
                    elm.text = str(v)

                elif k == 'TENURE_AREA_IN_HECTARES':
                    elm.text = round(float(v),3)



def export_Map(workspace,mxd,file_nbr,dt_id,parcel_id):
    """ Exports the map to PDF"""
    output = os.path.join(workspace, 'LDS_tenure{0}_dtid{1}_sid{2}.pdf'.format(file_nbr, str(dt_id),str(parcel_id)))
    arcpy.mapping.ExportToPDF(mxd, output)



def main ():
    input_file = sys.argv[1]

    workspace = sys.argv[2]

    mxd = arcpy.mapping.MapDocument("CURRENT")
    df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
    df_oview = arcpy.mapping.ListDataFrames(mxd, "Overview")[0]

    brknMXD = arcpy.mapping.ListBrokenDataSources(mxd)
    if len(brknMXD) >= 1:
        raise Exception ('Layers are broken!')
    else:
        pass


    data = pd.read_excel(input_file,converters={'FILE #':str})
    nbr_files = data.shape[0]
    counter = 1

    for index, row in data.iterrows():
        file_nbr = row['FILE #']
        dt_id = row['DTID']
        parcel_id = row['INTEREST PARCEL ID']

        arcpy.AddWarning ('Processing file {} of {}: {}'.format(counter,nbr_files,file_nbr))

        arcpy.AddMessage ('...Updating the Def Query')
        tenure_layer, bcgs_sheet = set_defQuery (mxd,df,file_nbr,dt_id,parcel_id)


        arcpy.AddMessage ('...Setting Scale')
        set_scale (df, df_oview, tenure_layer)

        arcpy.AddMessage ('...Populating Map text info...')
        populate_info (mxd,tenure_layer)

        arcpy.AddMessage ('...Exporting Map\n')
        export_Map(workspace,mxd,file_nbr,dt_id,parcel_id)
        counter += 1

    arcpy.AddMessage  ('Completed! Check the output folder for results')



if __name__ == "__main__":
    main()

