import os
#import fiona
#import geopandas as gpd
import pandas as pd

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

print ('Check Harvest Areas \n')

for index, row in df_appl.iterrows():
    row_id = 'row_{}'.format(str(index))
    data[row_id] = []
    data[row_id].append (row['Appl_num']) 
    data[row_id].append (row['Harvest_Area_Num'])
    
    for year in df_hav['Year'].unique():
        if str(row['Harvest_Area_Num']) in df_hav.loc[df_hav['Year'] == year]['harvest_area'].tolist():
            data[row_id].append ('Y')
        else:
             data[row_id].append ('N')
             
df = pd.DataFrame.from_dict(data,orient='index', columns = cols)


def sp_func(s_2018,s_2019,s_2020,s_2021,s_2022):
    
    if (s_2018 == 'N' and s_2019 == 'N' and s_2020 == 'N' and s_2021 == 'N' and s_2022 == 'N'):
        rslt = 'N'
    else:
        rslt = 'Y'
        
    return rslt


df['HAS_SPATIAL'] = df.apply(lambda x: sp_func(x['2018'], x['2019'], x['2020'], x['2021'], x['2022']), axis=1)

df.loc[(df['Harvest_area'] == '5017/5109'),'HAS_SPATIAL']='Y'

df_noSp = df.loc [df['HAS_SPATIAL'] == 'N']

#out = os.path.join (wrks, 'harvest_areas_check_v2.xlsx')
#df.to_excel (out)             
