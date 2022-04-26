import os
import arcpy
import pandas as pd

wks = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20220324_aquaPlantWilld_spatialProj'
xlsx = os.path.join(wks, 'data', 'AquaticPlant-WildHarvest_2005-2021_tempo.xlsx')

df = pd.read_excel (xlsx, '2013-2021 Master')

df = df.loc[(df['Status'] == 'Active') &
            (df['Year'] > 2013)]

cols = ['Year','Appl_num','Harvest_Area_Num','Species_Group',
        'Quota_Requested_MT','Total_Quota_Approved',
        'Total_Quantity_harvested']

df = df[cols]
df = df.sort_values(by=['Appl_num'])

df['Species_Group'] = df['Species_Group'].astype(int).astype(str)
        
har_areas = df['Harvest_Area_Num'].unique()

mxd_path = os.path.join(wks, 'Aquaculture_HarvestArea_MapsPlots','Aquaculture_HarvestArea_MapsPlots.aprx')
mxd = arcpy.mp.ArcGISProject(mxd_path)
df = mxd.listMaps("Layers")[0]
lyt = mxd.listLayouts("2022_Aquaculture_HarvestArea_Maps_small")[0]

layersList = df.listLayers()
harAr_lyr = layersList[0]


#for lyr in layersList:
 #       if lyr.name == 'harvest_areas_unique':
  #          harAr_lyr = lyr

for ha in har_areas:
    print ('Working on {}'.format (str(ha)))
    defQuery = """harvest_area = '{}' """.format (str(ha))
    harAr_lyr.definitionQuery = defQuery

    #desc = arcpy.Describe (harAr_lyr)
    descDS = arcpy.Describe (harAr_lyr.dataSource)
    df.extent = descDS.extent
    df.referenceScale = df.referenceScale * 1.1

    for elem in lyt.listElements():
        if elem.name == 'Plot':
            picPath = os.path.join(wks,'outputs','plots','by_harvest_area', 
                                   'graph_harvArea_{}.png'.format(str(ha)))

            elem.sourceImage = picPath 

        elif elm.name == "harvArea":
            elm.text = str(ha)

        output = os.path.join(wks, 'outputs', 'maps', 'Map_harvest_area_{}.pdf'.format(str(ha)))
        lyt.exportToPDF(output)
