import os
import arcpy
import pandas as pd

wks = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20220324_aquaPlantWilld_spatialProj'
xlsx = os.path.join(wks, 'data', 'AquaticPlant-WildHarvest_2005-2021_tempo.xlsx')

df = pd.read_excel (xlsx, '2013-2021 Master')

df = df.loc[(df['Status'] == 'Active') &
            (df['Year'] > 2013)]

cols = ['Year','Appl_num','Harvest_Area_Num','DFO_Area', 'Species_Group',
        'Quota_Requested_MT','Total_Quota_Approved',
        'Total_Quantity_harvested']

df = df[cols]

df['DFO_Area'] = df['DFO_Area'].astype(str)
df['Harvest_Area_Num'] = df['Harvest_Area_Num'].astype(str)

df['harv_is_missing'] = 'NO'
df.loc[df['Total_Quantity_harvested'].isnull(), 'harv_is_missing'] = "YES"
        
dfos = sorted(df['DFO_Area'].unique())
#dfos= ['7', '123']


mxd_path = os.path.join(wks, 'Aquaculture_HarvestArea_MapsPlots','Aquaculture_HarvestArea_MapsPlots.aprx')
mxd = arcpy.mp.ArcGISProject(mxd_path)
mp = mxd.listMaps("Main Map")[0]
lyt = mxd.listLayouts("2022_Aquaculture_HarvestArea_Maps_small")[0]

layersList = mp.listLayers()
harAr_lyr = layersList[0]

#for lyr in layersList:
 #       if lyr.name == 'harvest_areas_unique':
  #          harAr_lyr = lyr

for dfo in dfos:
    print ('Working on DFO {}'.format (str(dfo)))
    df_dfo = df.loc[df['DFO_Area'] == str(dfo)]
    harvArs = df_dfo['Harvest_Area_Num'].unique()
    harvArs_str = ",".join ("'" + str(x).strip() + "'" for x in harvArs)
    print (harvArs)

    defQuery = """harvest_area IN ({}) """.format (harvArs_str)
    harAr_lyr.definitionQuery = defQuery

    mf = lyt.listElements("mapframe_element", "Main Map")[0]
    ext = mf.getLayerExtent(harAr_lyr, False, True)
    mf.camera.setExtent(ext)
    mf.camera.scale = mf.camera.scale * 1.1

    for elem in lyt.listElements():
        if elem.name == 'Plot':
            picPath = os.path.join(wks,'outputs','plots','by_dfo', 
                                   'graph_dfo_{}.png'.format(str(dfo)))

            elem.sourceImage = picPath 

        elif elem.name == "dfo_num":
            elem.text = str(dfo)

        output = os.path.join(wks, 'outputs', 'maps', 'Map_DFO_{}.pdf'.format(str(dfo)))
        lyt.exportToPDF(output)
