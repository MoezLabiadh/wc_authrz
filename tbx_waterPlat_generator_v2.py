#-------------------------------------------------------------------------------
# Name:        Water plat generator
# Purpose:     This script generates a water plat (pdf map)
#              based on a water licence number.
#
# Author:      MLABIADH
#
# Created:     23-11-2021
#-------------------------------------------------------------------------------

import os
import math
import arcpy


def get_layers (mxd,df):
    """Returns the list of layers required for the rest of processing"""
    layersList = arcpy.mapping.ListLayers(mxd,"",df)
    arcpy.AddMessage ('Getting the required layers...')
    for lyr in layersList:
        if lyr.name == 'SELECTED PARCEL':
            lic_parcel = lyr

        elif lyr.name  == 'Water Management Districts':
            wtr_disct = lyr

        elif lyr.name  == 'Water Management Precincts':
            wtr_prsct = lyr

        elif lyr.name  == 'Land Districts':
            lnd_disct = lyr

        elif lyr.name  == 'BCGS_SHEETS_5k':
            bcgs_sheet = lyr

        else:
            pass

    return lic_parcel, wtr_disct, wtr_prsct, lnd_disct, bcgs_sheet


def update_defQuery (pid,lic_parcel):
    """Updates the definition Query of Water Licence parcel Layer"""
    defQuery = """PID = '{}' """.format (str(pid))

    lic_parcel.definitionQuery = defQuery

    # check if the water licence has parcels associated with it.
    result = arcpy.GetCount_management(lic_parcel)
    count = int(result.getOutput(0))
    if count < 1:
        raise Exception ('ERROR:Parcel Identifier (PID) {} not found!'.format(str(pid)))
    else:
        pass


def set_scale (df, df_oview, lic_parcel,in_scale):
    """ Set the map scale """
    ext = lic_parcel.getExtent()

    df.extent = ext

    # Use user entry if any otherwise calculate scale based on licence layer extent
    scale = int (df.scale)
    if in_scale == '#':
        thousands = scale // 1000
        hundreds = (scale % 1000) // 100
        if hundreds >= 6:
            rounded_scale = int(math.ceil(scale / 1000.0)) * 1000
        else:
            r = 500 - (scale - (thousands*1000))
            rounded_scale = scale + r

        df.scale = rounded_scale

    else:
        df.scale = int(in_scale)

    # set scale of overview map
    df_oview.extent = ext

    if  df.scale >= 3500:
        df_oview.scale = 650000
    else:
        df_oview.scale = 550000


def populate_info (mxd,lic_nbr, file_nbr,lic_parcel, wtr_disct, wtr_prsct, lnd_disct, bcgs_sheet):
    """ Populates map text elements"""

    field_dict = {
                  wtr_disct: ['DISTRICT_NAME'],
                  wtr_prsct: ['PRECINCT_NAME'],
                  lnd_disct: ['LAND_DISTRICT_NAME'],
                  bcgs_sheet: ['MAP_TILE_DISPLAY_NAME']
                  }

    for k, v in field_dict.items():

        if arcpy.Exists("tempo_lyr"):
            arcpy.Delete_management("tempo_lyr")

        arcpy.MakeFeatureLayer_management(k, "tempo_lyr")
        arcpy.SelectLayerByLocation_management ("tempo_lyr", "INTERSECT", lic_parcel)

        with arcpy.da.SearchCursor("tempo_lyr", v[0]) as cursor:
            for row in cursor:
                v.append(str(row[0]))
                break

        arcpy.AddMessage ('...{} | {}: {}'.format(k, v[0], v[1]))

    for elm in arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT"):
        if elm.name == "lic_nbr":
            elm.text = str(lic_nbr)

        elif elm.name == "water_dis":
            elm.text = field_dict.get(wtr_disct)[1].upper()

        elif elm.name == "precinct":
            elm.text = field_dict.get(wtr_prsct)[1].upper()

        elif elm.name == "land_dis":
            elm.text = field_dict.get(lnd_disct)[1].replace('DISTRICT', '').replace('DIST', '')

        elif elm.name == "file_nbr":
            elm.text = str(file_nbr)

        elif elm.name == "map_sheet":
            elm.text = field_dict.get(bcgs_sheet)[1]

        else:
            pass


def export_Map(workspace,mxd,lic_nbr):
    """ Exports the map to PDF"""
    output = os.path.join(workspace, 'Water_Plat_{}.pdf'.format(lic_nbr))
    arcpy.mapping.ExportToPDF(mxd, output)

    '''
    #mxd.dataDrivenPages.refresh ()
    for pageNum in range(1, mxd.dataDrivenPages.pageCount + 1):
        mxd.dataDrivenPages.currentPageID = pageNum
        arcpy.AddMessage( "..Exporting page {0} of {1}".format(str(mxd.dataDrivenPages.currentPageID), str(mxd.dataDrivenPages.pageCount)))
        output = os.path.join(workspace, 'Water_Plat_{}.pdf'.format(lic_nbr))
        arcpy.mapping.ExportToPDF(mxd,output)

    '''


def main():
    """ Executes the program"""
    #user inputs
    lic_nbr = sys.argv[1]
    file_nbr = sys.argv[2]
    pid = sys.argv[3]
    in_scale = sys.argv[4]
    workspace = sys.argv[5]

    mxd = arcpy.mapping.MapDocument("CURRENT")
    df = arcpy.mapping.ListDataFrames(mxd, "Layers")[0]
    df_oview = arcpy.mapping.ListDataFrames(mxd, "Overview")[0]

    brknMXD = arcpy.mapping.ListBrokenDataSources(mxd)
    if len(brknMXD) >= 1:
        raise Exception ('Layers are broken!')
    else:
        pass

    arcpy.AddMessage  ('Getting layers...')
    lic_parcel, wtr_disct, wtr_prsct, lnd_disct, bcgs_sheet = get_layers(mxd,df)

    arcpy.AddMessage ('Updating the Def Query...')
    update_defQuery (pid,lic_parcel)

    arcpy.AddMessage ('Setting Scale')
    set_scale (df, df_oview, lic_parcel,in_scale)

    arcpy.AddMessage ('Populating Map text info...')
    populate_info (mxd, lic_nbr, file_nbr, lic_parcel, wtr_disct, wtr_prsct, lnd_disct, bcgs_sheet)

    if workspace != '#':
        arcpy.AddMessage ('Exporting Map...')
        export_Map(workspace,mxd,lic_nbr)

    else:
        pass

    arcpy.AddMessage  ('Completed!')



if __name__ == "__main__":
    main()


