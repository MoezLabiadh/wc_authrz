import os
#import fiona
#import geopandas as gpd
import pandas as pd
import numpy as np

def get_apps_list (app_xls):
    df = pd.read_excel(app_xls, '2013-2021 Master')
    df = df.loc[df['Status'] == 'Active']
    df['Harvest_Area_Num'].fillna('MISSING', inplace=True)
    df = df.sort_values(by=['Appl_num', 'Harvest_Area_Num'], ascending=False)
    
    
    df = df[['Appl_num', 'Harvest_Area_Num']]
    
    return df

def get_harvAreas (hav_xls):
    df = pd.read_excel(hav_xls)
    df = df.sort_values(by=['Year', 'harvest_area'], ascending=False)  
    
    return df

wrks = r'\\spatialfiles.bcgov\...\20220324_aquaPlantWilld_spatialProj' 
app_xls = os.path.join (wrks, 'data', 'AquaticPlant-WildHarvest_2005-2021_tempo.xlsx')
hav_xls = os.path.join (wrks, 'data', 'harvest_areas_allYears_iteration0.xlsx')

print ('Get applications list\n')
df_appl = get_apps_list(app_xls)

print ('Get Harvest Area list\n')
df_hav = get_harvAreas (hav_xls)

print ('Create Dictionnary\n')

cols = ['Appl_num', 'Harvest_area', '2022', '2021', '2020', '2019', '2018']
data = {}

print ('Add Hectares for each Year \n')

for index, row in df_appl.iterrows():
    row_id = 'row_{}'.format(str(index))
    data[row_id] = []
    data[row_id].append (row['Appl_num']) 
    data[row_id].append (row['Harvest_Area_Num'])
    
    for year in df_hav['Year'].unique():
        if str(row['Harvest_Area_Num']) in df_hav.loc[df_hav['Year'] == year]['harvest_area'].tolist():
            df_area = df_hav.loc[(df_hav['harvest_area'] == str(row['Harvest_Area_Num'])) & (df_hav['Year'] == year)]
            hactare = round(df_area ['area_ha'].iloc[0],2)
            data[row_id].append (hactare)
        else:
             data[row_id].append (np.nan)
             
df = pd.DataFrame.from_dict(data,orient='index', columns = cols)


print ('Calculate the Percent Change \n')

df = df.set_index(['Appl_num','Harvest_area'])

df_ch = df.pct_change(axis='columns')
df_ch['pctChange_mean'] = df_ch.mean(axis=1)


#out = os.path.join (wrks, 'harvest_areas_check_v2.xlsx')
#df.to_excel (out)
