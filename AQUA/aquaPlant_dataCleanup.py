import os
import time
import pandas as pd
import numpy as np


wks = r'\\spatialfiles.bcgovXX'

start_time = time.time()

f = open(os.path.join(wks, 'change_log.txt'), 'w')
f.write("Start\n\n")

xls = os.path.join (wks, 'AquaticPlant-WildHarvest_2005-2021_tempo.xlsx')
df = pd.read_excel (xls, '2013-2021 Master', converters = {'Scientific_Names': str})

df['Scientific_Names'] = df['Scientific_Names'].fillna('tempo_NAN')


for index, row in df.iterrows():
    if  row['Scientific_Names'] == 'tempo_NAN':
        f.write('Row {}. Appl# {} has NO Species. NO ACTION\n'.format (index+2, row['Appl_num']))
        print ('Row {}. Appl# {} has NO Species. NO ACTION'.format (index+2, row['Appl_num']))
        
    else:
        l = row['Scientific_Names'].split(",")
        sp_nbr = len(l)
        
        if sp_nbr == 1:
            df.loc[index, 'Species_1'] = l[0]
            #print (index, df['Total_Quota_Approved'])
            
            df.loc[index, 'Species_1_Quota_Approved'] = row['Total_Quota_Approved']
            df.loc[index, 'Species_1_Quantity_Harvest'] = row['Total_Quantity_harvested']
            
            f.write ('Row {}. Appl# {} has {} Species. SPECIES 1 INFO UPDATED\n'.format (index+2, row['Appl_num'], sp_nbr))
            print ('Row {}. Appl# {} has {} Species. SPECIES 1 INFO UPDATED'.format (index+2, row['Appl_num'], sp_nbr))
            
        else:
    
            f.write ('Row {}. Appl# {} has {} Species. NO ACTION\n'.format (index+2, row['Appl_num'], sp_nbr))
            print ('Row {}. Appl# {} has {} Species. NO ACTION'.format (index+2, row['Appl_num'], sp_nbr))
            


df = df.replace('tempo_NAN', np.nan)

date_cols = ['Licence_start_date', 'Licence_end_date', 'Harvest_Record_Date_submitted']
for date_col in date_cols:
    df[date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d %H:%M:%S', errors='coerce').dt.date
      

f.write ("Completed --- %s seconds ---" % (time.time() - start_time))
print("Completed --- %s seconds ---" % (time.time() - start_time))

df.to_excel(os.path.join(wks, 'mods.xlsx'))

