import os
import arcpy
import pandas as pd

wks = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20220324_aquaPlantWilld_spatialProj'
xlsx = os.path.join(wks, 'data', 'AquaticPlant-WildHarvest_2005-2021_tempo.xlsx')

df = pd.read_excel (xlsx, '2013-2021 Master')

df = df.loc[(df['Status'] == 'Active') &
            (df['Year'] > 2013)]

cols = ['Year','Appl_num','Harvest_Area_Num','DFO_Area', 'Species_Group', 'Geographic_Region-ID',
        'Quota_Requested_MT','Total_Quota_Approved',
        'Total_Quantity_harvested']

df = df[cols]
df = df.sort_values(by=['Appl_num'])

df['Species_Group'] = df['Species_Group'].astype(int).astype(str)
df['Appl_num'] = df['Appl_num'].astype(str)
df['DFO_Area'] = df['DFO_Area'].astype(str)

df['harv_is_missing'] = 'NO'
df.loc[df['Total_Quantity_harvested'].isnull(), 'harv_is_missing'] = "YES"
        
        

locs= sorted(df['Geographic_Region-ID'].unique())
#dfos= ['10', '14', '15']

mxd_path = os.path.join(wks, 'Aquaculture_HarvestArea_MapsPlots','Aquaculture_HarvestArea_MapsPlots_Geoloc.aprx')
mxd = arcpy.mp.ArcGISProject(mxd_path)
mp = mxd.listMaps("Main Map")[0]

layersList = mp.listLayers()
harAr_lyr = layersList[0]

for loc in locs:
    print ('\nWorking on {}'.format (str(loc)))
    df_loc = df.loc[df['Geographic_Region-ID'] == str(loc)]
    harvArs = df_loc['Harvest_Area_Num'].unique()
    
    harvArs_str = ",".join ("'" + str(x).strip() + "'" for x in harvArs)

    defQuery = """harvest_area IN ({}) """.format (harvArs_str)
    harAr_lyr.definitionQuery = defQuery
    print (defQuery)

    sp_gr = df_loc['Species_Group'].unique()

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
            picPath = os.path.join(wks,'outputs','plots','by_geoloc', 
                                   'graph_geoloc_{}.png'.format(str(loc)))

            elem.sourceImage = picPath 
            elem.elementPositionX = 27.8998
            elem.elementPositionY = posY

        elif elem.name == "geoloc":
            elem.text = str(loc)

    output = os.path.join(wks, 'outputs', 'maps', 'by_geoloc', 'Map_geoloc_{}.pdf'.format(str(loc)))
    lyt.exportToPDF(output, resolution =150)
