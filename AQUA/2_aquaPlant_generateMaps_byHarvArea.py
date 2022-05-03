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

#df['DFO_Area'] = df['DFO_Area'].astype(str)
df['Harvest_Area_Num'] = df['Harvest_Area_Num'].astype(str)

df['harv_is_missing'] = 'NO'
df.loc[df['Total_Quantity_harvested'].isnull(), 'harv_is_missing'] = "YES"
        
harvs = sorted(df['Harvest_Area_Num'].unique())
#harvs= ['5107', '5000']

mxd_path = os.path.join(wks, 'Aquaculture_HarvestArea_MapsPlots','Aquaculture_HarvestArea_MapsPlots_harvArea.aprx')
mxd = arcpy.mp.ArcGISProject(mxd_path)
mp = mxd.listMaps("Main Map")[0]

layersList = mp.listLayers()
harAr_lyr = layersList[0]

for harv in harvs:
    print ('\n Working on Harvest Area {}'.format (str(harv)))
    df_harv = df.loc[df['Harvest_Area_Num'] == str(harv)]
    #harvArs = df_dfo['Harvest_Area_Num'].unique()
    
    #harvArs_str = ",".join ("'" + str(x).strip() + "'" for x in harvArs)

    defQuery = """harvest_area = '{}' """.format (str(harv))
    harAr_lyr.definitionQuery = defQuery
    print (defQuery)

    sp_gr = df_harv['Species_Group'].unique()

    if len(sp_gr) == 1:
        lyt = mxd.listLayouts("2022_Aquaculture_HarvestArea_Maps_1spcs")[0]
        posY = 6.1648
    elif len(sp_gr) == 2:
        lyt = mxd.listLayouts("2022_Aquaculture_HarvestArea_Maps_2spcs")[0]
        posY = 7.6339
    else:
        lyt = mxd.listLayouts("2022_Aquaculture_HarvestArea_Maps_more2")[0]
        posY = 9.68
    
    mf = lyt.listElements("mapframe_element", "Main Map")[0]
    ext = mf.getLayerExtent(harAr_lyr, False, True)
    mf.camera.setExtent(ext)
    mf.camera.scale = mf.camera.scale * 1.1

    for elem in lyt.listElements():
        if elem.name == 'Plot':
            picPath = os.path.join(wks,'outputs','plots','by_harvest_area', 
                                   'graph_harvArea_{}.png'.format(str(harv)))

            elem.sourceImage = picPath 
            elem.elementPositionX = 27.8998
            elem.elementPositionY = posY

        elif elem.name == "harvAr_num":
            elem.text = str(harv)

    output = os.path.join(wks, 'outputs', 'maps', 'by_harvArea', 'Map_harvestArea_{}.pdf'.format(str(harv)))
    lyt.exportToPDF(output, resolution =150)
