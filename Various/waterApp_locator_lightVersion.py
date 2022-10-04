import arcpy
#import pandas as pd


def get_wtshd_info(lat, long, wtsh_fc):
    """Returns Watershed and Region info for the Water Apps ledger"""
    arcpy.AddWarning ('Entered Latitude: {}'. format(lat))
    arcpy.AddWarning ('Entered Longitude: {}'. format(long))

    if (lat is None) or (long is None):
        raise Exception("Latitude or/and Longitude values are not specified!")

    elif not min(54.465, 48.2) < lat < max(54.5, 48.134):
        raise Exception("Latitude value is out of range!")


    elif not min(-122.634, -133.257) < long < max(-122.7, -133.3):
            raise Exception("Longitude value is out of range!")

    else:
        pt = arcpy.Point(float(long),float(lat))
        sr = arcpy.SpatialReference("WGS 1984")
        ptGeo = arcpy.PointGeometry(pt,sr)

        pt_fc = 'in_memory\pt_fc'
        arcpy.management.CopyFeatures(ptGeo, pt_fc)

        intr = 'in_memory\_intersect'
        arcpy.Intersect_analysis([pt_fc,wtsh_fc], intr)

        with arcpy.da.UpdateCursor(intr,['WATER_LICENSING_WATERSHED_NAME', 'Region']) as cursor:
            for row in cursor:
                wtrsh_name = row[0]
                region = row[1]

        arcpy.Delete_management('in_memory')

    arcpy.AddWarning ('Watershed name: {}'.format(wtrsh_name))
    arcpy.AddWarning ('Subregion name: {}'.format(region))

def main():
    wtsh_fc = r'\\..\waterLicencing_watersheds'
    lat = float(arcpy.GetParameterAsText(0))
    long = float(arcpy.GetParameterAsText(1))

    arcpy.AddWarning ('************************************')
    get_wtshd_info(lat, long, wtsh_fc)
    arcpy.AddWarning ('************************************')

main()
