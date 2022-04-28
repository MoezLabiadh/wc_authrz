import os
import time
import pandas as pd
import numpy as np


wks = r'\\spatialfiles.bcgov\...\data'

start_time = time.time()

f = open(os.path.join(wks, 'change_log.txt'), 'w')
f.write("Start\n\n")

xls = os.path.join (wks, 'AquaticPlant-WildHarvest_2005-2021_tempo.xlsx')
df = pd.read_excel (xls, '2013-2021 Master', converters = {'Scientific_Names': str})

xls_lkup = os.path.join (wks, 'Species-grp_Species-num_lookup.xlsx')
df_sp = pd.read_excel(xls_lkup)

df['Scientific_Names'] = df['Scientific_Names'].fillna('tempo_NAN')


sp_dict = {}
for index, row in df_sp.iterrows():
    sp_dict [str(row[0])] = [row[x] for x in range(1,6)]
    
    
for index, row in df.iterrows():
    if  (row['Scientific_Names'] == 'tempo_NAN') or (row['Appl_num'] == '2021-044'):
        
        f.write('Row {}. Appl# {} has NO Species. NO ACTION\n'.format (index+2, row['Appl_num']))
        print ('Row {}. Appl# {} has NO Species. NO ACTION\n'.format (index+2, row['Appl_num']))
        
    else:
        l = row['Scientific_Names'].split(",")
        sp_nbr = len(l)
        
        if sp_nbr == 1:
            species = l[0]
            speciesGr = str(int(row['Species_Group']))
            spPos = sp_dict[speciesGr].index(species)
            sp_id = spPos + 1
            
            spName_col = "Species_{}".format(sp_id) 
            spQuota_col = "Species_{}_Quota_Approved".format(sp_id) 
            spHarv_col = "Species_{}_Quantity_Harvest".format(sp_id) 
            
            df.loc[index, spName_col] = species
            df.loc[index, spQuota_col] = row['Total_Quota_Approved']
            df.loc[index, spHarv_col] = row['Total_Quantity_harvested']
            
            f.write ('Row {}. Appl# {} has {} Species. {} INFO UPDATED\n'.format (index+2, row['Appl_num'], sp_nbr,spName_col))
            print ('Row {}. Appl# {} has {} Species. {} INFO UPDATED\n'.format (index+2, row['Appl_num'], sp_nbr,spName_col))
            
            
        else:
    
            f.write ('Row {}. Appl# {} has {} Species. NO ACTION\n'.format (index+2, row['Appl_num'], sp_nbr))
            #print ('Row {}. Appl# {} has {} Species. NO ACTION'.format (index+2, row['Appl_num'], sp_nbr))
            


df = df.replace('tempo_NAN', np.nan)

date_cols = ['Licence_start_date', 'Licence_end_date', 'Harvest_Record_Date_submitted']
for date_col in date_cols:
    df[date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d %H:%M:%S', errors='coerce').dt.date
      

f.write ("\n Completed --- %s seconds ---" % (time.time() - start_time))
#print("Completed --- %s seconds ---" % (time.time() - start_time))

#df.to_excel(os.path.join(wks, 'mods.xlsx'))

