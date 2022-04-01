import os
import time
import pandas as pd
#import numpy as np


wks = r'\\spatialfiles.bcgov\...\data'

start_time = time.time()

f = open(os.path.join(wks, 'species_check.txt'), 'w')
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
        pass
    
    else:
        l = row['Scientific_Names'].strip().split(",")
        speciesGr = str(int(row['Species_Group']))
       
        for sps in l:
            if sps.strip() not in sp_dict[speciesGr]:
               f.write ('Row {} - Appl# {}: {} is not in Species Group {} \n'.format (index+2, row['Appl_num'],sps,speciesGr))
               print ('Row {} - Appl# {}: {} is not in Species Group {} \n'.format (index+2, row['Appl_num'],sps,speciesGr))
               
            else:
               pass

f.write ("\n Completed --- %s seconds ---" % (time.time() - start_time))
#print("Completed --- %s seconds ---" % (time.time() - start_time))

#df.to_excel(os.path.join(wks, 'mods.xlsx'))

