#-------------------------------------------------------------------------------
# Name:        Tenure Document map generator
# Purpose:     This script generates a Tenure document map (LDS map).
#
# Author:      MLABIADH
#
# Created:     13-09-2021
#-------------------------------------------------------------------------------

import os
import math
import arcpy

def set_defQuery (mxd,df,file_nbr,dt_id,parcel_id):
    """Locate the Teure Layer and set a Definition Query"""
    layersList = arcpy.mapping.ListLayers(mxd,"",df)
    tenure_layer = layersList[1]

    defQuery = """CROWN_LANDS_FILE = '{}'
              AND DISPOSITION_TRANSACTION_SID = {}
              AND INTRID_SID = {} """.format (file_nbr,dt_id,parcel_id)

    tenure_layer.definitionQuery = defQuery

    # check if the water licence has parcels associated with it.
    result = arcpy.GetCount_management(tenure_layer)
    count = int(result.getOutput(0))
    if count < 1:
        raise Exception ('ERROR: Parcel not found! Check user entries.')
    else:
        pass

    return tenure_layer


def set_scale (df, df_oview, tenure_layer,in_scale):
    """ Set the map scale """
    ext = tenure_layer.getExtent()

    df.extent = ext

    # Use user entry if any otherwise calculate scale based on tenure layer extent
    scale = int (df.scale)
    scale_buf = scale*4
    if in_scale == '#':
        thousands = scale_buf // 1000
        hundreds = (scale_buf % 1000) // 100
        if hundreds >= 6:
            rounded_scale = int(math.ceil(scale_buf / 1000.0)) * 1000
        else:
            r = 500 - (scale_buf - (thousands*1000))
            rounded_scale = scale_buf + r

        df.scale = rounded_scale

    else:
        df.scale = int(in_scale)

    # set scale of overview map
    df_oview.extent = ext

    if  df.scale >= 3500:
        df_oview.scale = 600000
    else:
        df_oview.scale = 500000


def populate_info (mxd,tenure_layer,legal_txt):
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
                if k == 'TENURE_LEGAL_DESCRIPTION' and legal_txt != '#':
                    elm.text = legal_txt

                elif k == 'TENURE_AREA_IN_HECTARES':
                    elm.text = round(float(v),3)

                else:
                    elm.text = str(v)



def export_Map(workspace,mxd,file_nbr,dt_id,parcel_id):
    """ Exports the map to PDF"""
    output = os.path.join(workspace, 'LDS_tenure{0}_dtid{1}_sid{2}.pdf'.format(file_nbr, str(dt_id),str(parcel_id)))
    arcpy.mapping.ExportToPDF(mxd, output)

    '''
    #mxd.dataDrivenPages.refresh ()
    #arcpy.RefreshActiveView()
    #arcpy.RefreshTOC()
    #mxd.save()

    for pageNum in range(1, mxd.dataDrivenPages.pageCount + 1):
        mxd.dataDrivenPages.currentPageID = pageNum
        arcpy.AddMessage( "..Exporting page {0} of {1}".format(str(mxd.dataDrivenPages.currentPageID), str(mxd.dataDrivenPages.pageCount)))
        output = os.path.join(workspace, 'LDS_tenure{0}_dtid{1}_sid{2}_scale{3}_{4}.pdf'.format(file_nbr, str(dt_id),str(parcel_id),str(scale), str(pageNum)))
        arcpy.mapping.ExportToPDF(mxd,output)
    '''




def main ():
    file_nbr = sys.argv[1]
    dt_id = sys.argv[2]
    parcel_id = sys.argv[3]
    legal_txt = sys.argv[4]
    in_scale = sys.argv[5]
    workspace = sys.argv[6]

    mxd = arcpy.mapping.MapDocument("CURRENT")
    df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
    df_oview = arcpy.mapping.ListDataFrames(mxd, "Overview")[0]

    brknMXD = arcpy.mapping.ListBrokenDataSources(mxd)
    if len(brknMXD) >= 1:
        raise Exception ('Layers are broken!')
    else:
        pass


    arcpy.AddMessage ('Updating the Def Query...')
    tenure_layer = set_defQuery (mxd,df,file_nbr,dt_id,parcel_id)


    arcpy.AddMessage ('Setting Scale')
    set_scale (df, df_oview, tenure_layer,in_scale)

    arcpy.AddMessage ('Populating Map text info...')
    populate_info (mxd,tenure_layer,legal_txt)

    arcpy.AddMessage ('Exporting Map...')
    export_Map(workspace,mxd,file_nbr,dt_id,parcel_id)

    arcpy.AddMessage  ('Completed! Check the output folder for results')



if __name__ == "__main__":
    main()

