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
df['Appl_num'] = df['Appl_num'].astype(str)


df['harv_is_missing'] = 'NO'
df.loc[df['Total_Quantity_harvested'].isnull(), 'harv_is_missing'] = "YES"


# Add MaPP column to master df
xlsx_mapp = os.path.join(wks, 'data', 'lookup_harArea_MaPP.xlsx')
df_mapp = pd.read_excel (xlsx_mapp)

mapp_dict = {}
for index, row in df_mapp.iterrows():
    mapp_dict[str(row['harvest_area'])] = str(row['MaPP_name'])

for index, row in df.iterrows():
    for k, v in mapp_dict.items():
        if str(row['Harvest_Area_Num']) == str(k):
            df.at[index, 'MaPP_name'] =  str(v) 


df = df.loc[df['MaPP_name'].notnull()]


mapps = df['MaPP_name'].unique()
#dfos= ['10', '14', '15']

mxd_path = os.path.join(wks, 'Aquaculture_HarvestArea_MapsPlots','Aquaculture_HarvestArea_MapsPlots_MAPP.aprx')
mxd = arcpy.mp.ArcGISProject(mxd_path)
mp = mxd.listMaps("Main Map")[0]

layersList = mp.listLayers()
harAr_lyr = layersList[0]

for mapp in mapps:
    print ('\nWorking {}'.format (str(mapp)))
    df_mapp = df.loc[df['MaPP_name'] == str(mapp)]
    harvArs = df_mapp['Harvest_Area_Num'].unique()
    
    harvArs_str = ",".join ("'" + str(x).strip() + "'" for x in harvArs)

    defQuery = """harvest_area IN ({}) """.format (harvArs_str)
    harAr_lyr.definitionQuery = defQuery
    print (defQuery)

    sp_gr = df_mapp['Species_Group'].unique()

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
            picPath = os.path.join(wks,'outputs','plots','by_maPP', 
                                   'graph_MaPP_{}.png'.format(str(mapp)))

            elem.sourceImage = picPath 
            elem.elementPositionX = 27.8998
            elem.elementPositionY = posY

        elif elem.name == "mapp":
            l = mapp.split(' ')[:-2]
            st_mapp = ' '.join(x for x in l)
            elem.text = str(st_mapp)

    output = os.path.join(wks, 'outputs', 'maps', 'by_mapp', 'Map_MaPP_{}.pdf'.format(str(st_mapp)))
    lyt.exportToPDF(output, resolution =150)
