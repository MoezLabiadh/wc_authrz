import os
#import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


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
#har_areas = ['5107','5405', '5438']

for mapp in mapps:
    print ('working on {} '.format(mapp))
    df_mp = df.loc[df['MaPP_name'].astype(str) == str(mapp)] 
    
    df_mp = df_mp.rename(columns={'Quota_Requested_MT': 'Quota Requested', 
                                  'Total_Quota_Approved': 'Quota Approved'})
        
        
    df_gr = df_mp.groupby(['Year', 'Species_Group']).agg({'Quota Requested':'sum',
                                                          'Quota Approved':'sum',
                                                          'Total_Quantity_harvested':'sum'}).reset_index()
        
    df_hr = df_gr[['Year', 'Species_Group', 'Total_Quantity_harvested']]
    df_qt = df_gr.melt(id_vars=['Year', 'Species_Group'], 
                       value_vars=['Quota Requested', 'Quota Approved' ], ignore_index=True)
    
    
    sp_grps = sorted(list(df_mp['Species_Group'].unique()))
    
    # initiate the figure/plot

 
    if len(sp_grps) in (1,2):
        h_coef = 6
    elif len(sp_grps) == 3:
        h_coef = 5
    else:
        h_coef = 4

    fig, axes = plt.subplots(nrows=len(sp_grps), figsize=(16,len(sp_grps)*h_coef), constrained_layout=True)
    t_txt = 'Wild Aquatic Plant Harvest by Year ({})'.format (mapp)  
    fig.suptitle(t_txt, fontsize=20)
    #fig.tight_layout()
    
    #Create one subplot for each Species Group
    for i, sp_g in enumerate(sp_grps):
        filt_hr = df_hr.loc [df_hr['Species_Group'] == sp_g].reset_index()
        filt_qt= df_qt.loc [df_qt['Species_Group'] == sp_g]
   
        if len(sp_grps) == 1:
            ax = axes
        else:
            ax = axes[i]
            
        sns.barplot(data = filt_qt, x='Year', y='value', alpha=0.7,
                    palette = ['tab:orange', 'tab:blue'], hue = 'variable', ax=ax)
        
        sns.lineplot(data =filt_hr['Total_Quantity_harvested'], linewidth = 1.5, markers=True,sort = False,
                     marker="o",markersize=10, color='darkred', label='Harvested Quantity', ax=ax)   
        
        
        #Set labels
        ax.set_title('Species Group: {}'.format(sp_g), size=15)
        ax.set_ylabel('Quantity (tonne)', fontsize=14)
        
        if sp_g == sp_grps[-1]:
            ax.set_xlabel('Harvest Year', fontsize=14)
        else:
            ax.set_xlabel(xlabel = None)
            
        #add annotation for missing Harvest Quantity values
        df_mp = df_mp.sort_values(by=['harv_is_missing'], ascending=False)
        for ind in range (0,filt_hr.shape[0]):
            miss = df_mp.loc[(df_mp['Year'] == filt_hr['Year'].iloc[ind]) & 
                             (df_mp['Species_Group'] == sp_g)]['harv_is_missing'].iloc[0]



            if miss == 'YES':
               x = filt_hr.index[filt_hr['Year']==filt_hr['Year'].iloc[ind]].tolist()[0]
               y = filt_hr['Total_Quantity_harvested'].iloc[ind]
               ax.text(x,y+0.008, '**', weight="bold")
               
               nan_txt = "**: Harvest Quantity for one (or more) application(s) is missing"
               plt.gcf().text(0, 0.007, nan_txt, fontsize=12)
               #fig.subplots_adjust(bottom=0.05)
            
  
        #label bars
        #for container in ax1.containers:
            #ax1.bar_label(container)
        
        #plt.legend(fontsize=22)
        
        
        #Remove legend
        #ax1.legend(title='Legend')
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles=handles, labels=labels)
        
    filename = 'graph_MaPP_{}.png'.format (mapp)
    fig.savefig(os.path.join(wks, 'outputs', 'plots', 'by_maPP', filename))
