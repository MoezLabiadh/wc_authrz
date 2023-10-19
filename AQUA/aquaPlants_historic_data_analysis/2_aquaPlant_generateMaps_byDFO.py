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
        
mxd_path = os.path.join(wks, 'Aquaculture_HarvestArea_MapsPlots','Aquaculture_HarvestArea_MapsPlots.aprx')
mxd = arcpy.mp.ArcGISProject(mxd_path)

homeFol = mxd.homeFolder
name = 'BCGW'
database_platform = 'ORACLE'
account_authorization  = 'DATABASE_AUTH'
instance = 'bcgw.bcgov/idwprod1.bcgov'
username = 'XXX'
password = 'XXX'
bcgw_conn_path = os.path.join(homeFol,'BCGW.sde')
if arcpy.Exists(bcgw_conn_path):
    arcpy.Delete_management(bcgw_conn_path)
    arcpy.CreateDatabaseConnection_management (homeFol,name, database_platform,
                                               instance,account_authorization,
                                               username ,password, 'DO_NOT_SAVE_USERNAME')

mp = mxd.listMaps("Main Map")[0]
layersList = mp.listLayers()
dfo_ar_lyr = layersList[0]
dfo_subar_lyr = layersList[1]

#dfos = sorted(df['DFO_Area'].unique())
dfos= ['29']

for dfo in dfos:
    print ('\n Working on DFO {}'.format (str(dfo)))
    df_dfo = df.loc[df['DFO_Area'] == str(dfo)]
    harvArs = df_dfo['Harvest_Area_Num'].unique()
    
    #harvArs_str = ",".join ("'" + str(x).strip() + "'" for x in harvArs)

    defQuery = """ MANAGEMENT_AREA = {} """.format (dfo)
    dfo_ar_lyr.definitionQuery = defQuery
    dfo_subar_lyr.definitionQuery = defQuery

    sp_gr = df_dfo['Species_Group'].unique()

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
    ext = mf.getLayerExtent(dfo_ar_lyr, False, True)
    mf.camera.setExtent(ext)
    mf.camera.scale = mf.camera.scale * 1.1

    for elem in lyt.listElements():
        if elem.name == 'Plot':
            picPath = os.path.join(wks,'outputs','plots','by_dfo', 
                                       'graph_dfo_{}.png'.format(str(dfo)))

            elem.sourceImage = picPath 
            elem.elementPositionX = 27.8998
            elem.elementPositionY = posY


        elif elem.name == "dfo_num":
            elem.text = str(dfo)

        elif elem.name == "harvAreaList":
            harvArs = df_dfo['Harvest_Area_Num'].astype(str).unique()
            harvArs = sorted(list(set(harvArs)))
            harvArs_str = ", ".join (str(x).strip() for x in harvArs)
            elem.text = harvArs_str

    output = os.path.join(wks, 'outputs', 'maps', 'by_dfo', 'Map_DFO_{}.pdf'.format(str(dfo)))
    lyt.exportToPDF(output, resolution =200)
