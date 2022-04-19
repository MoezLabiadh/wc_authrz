import os
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

#har_areas = df['Harvest_Area_Num'].unique()
har_areas = ['5107']

for ha in har_areas:
    print ('working on Harvest Area {} '.format(ha))
    df_ha = df.loc[df['Harvest_Area_Num'].astype(str) == str(ha)] 
    
    df_ha = df_ha.rename(columns={'Quota_Requested_MT': 'Quota Requested', 
                                  'Total_Quota_Approved': 'Quota Approved'})
        
        
    df_gr = df_ha.groupby(['Year', 'Species_Group']).agg({'Quota Requested':'sum',
                                                          'Quota Approved':'sum',
                                                          'Total_Quantity_harvested':'sum'}).reset_index()
        
    df_hr = df_gr[['Year', 'Species_Group', 'Total_Quantity_harvested']]
    df_qt = df_gr.melt(id_vars=['Year', 'Species_Group'], 
                       value_vars=['Quota Requested', 'Quota Approved' ], ignore_index=True)
    
    
    
    sp_grps = list(df_ha['Species_Group'].unique())
    
    # initiate the figure/plot
    fig, ax = plt.subplots(nrows=len(sp_grps), figsize=(20,len(sp_grps)*6))
    t_txt = 'Wild Aquatic Plant Harvest by Year\n Harvest Area # {}'.format (ha)  
    fig.suptitle(t_txt, fontsize=30)
    #plt.tight_layout()
    
    #Create one subplot for each Species Group
    for i, sp_g in enumerate(sp_grps):
        filt_hr = df_hr.loc [df_hr['Species_Group'] == sp_g].reset_index()
        filt_qt= df_qt.loc [df_qt['Species_Group'] == sp_g]
   
        
        sns.barplot(data = filt_qt, x='Year', y='value', alpha=0.7,
                    palette = ['tab:orange', 'tab:blue'], hue = 'variable', ax=ax[i])
        
        sns.lineplot(data =filt_hr['Total_Quantity_harvested'], linewidth = 1.5, markers=True,sort = False,
                     marker="o",markersize=10, color='darkred', label='Harvested Quantity', ax=ax[i])   
        
        
        #Set labels
        ax[i].set_title('Species Group: {}'.format(sp_g), size=25)
        ax[i].set_ylabel('Quantity (tonne)', fontsize=14)
        
        if sp_g == sp_grps[-1]:
            ax[i].set_xlabel('Harvest Year', fontsize=14)
        else:
            ax[i].set_xlabel(xlabel = None)
            

        
        #label bars
        #for container in ax1.containers:
            #ax1.bar_label(container)
        
        #plt.legend(fontsize=22)
        
        
        #Remove legend
        #ax1.legend(title='Legend')
        handles, labels = ax[i].get_legend_handles_labels()
        ax[i].legend(handles=handles, labels=labels)
        
    filename = 'graph_harvArea{}.png'.format (ha)
    fig.savefig(os.path.join(wks, 'outputs', 'plots', filename))
