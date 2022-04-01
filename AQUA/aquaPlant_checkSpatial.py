import os
import numpy as np
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


def check_spatial (df_appl, df_hav, cols):
    data = {}
    
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
      
    return df

 
def sp_func(s_2018,s_2019,s_2020,s_2021,s_2022):
    
    if (s_2018 == 'N' and s_2019 == 'N' and s_2020 == 'N' and s_2021 == 'N' and s_2022 == 'N'):
        rslt = 'N'
    else:
        rslt = 'Y'
        
    return rslt
      

def compare_hectare (df_appl,df_hav,cols):
    data_hec = {}
    
    
    for index, row in df_appl.iterrows():
        row_id = 'row_{}'.format(str(index))
        data_hec[row_id] = []
        data_hec[row_id].append (row['Appl_num']) 
        data_hec[row_id].append (row['Harvest_Area_Num'])
        
        for year in df_hav['Year'].unique():
            if str(row['Harvest_Area_Num']) in df_hav.loc[df_hav['Year'] == year]['harvest_area'].tolist():
                df_area = df_hav.loc[(df_hav['harvest_area'] == str(row['Harvest_Area_Num'])) & (df_hav['Year'] == year)]
                hactare = round(df_area ['area_ha'].iloc[0],2)
                data_hec[row_id].append (hactare)
            else:
                 data_hec[row_id].append (np.nan)
                 
    df = pd.DataFrame.from_dict(data_hec,orient='index', columns = cols)
    
    return df


def caluclate_pctChange (df):
    
    df = df.set_index(['Appl_num','Harvest_area'])
    
    df_ch = df.pct_change(axis='columns').abs()
    df_ch['pctChange_max'] = df_ch.max(axis=1)
    
    df_ch.reset_index(inplace=True)   
    df_ch.reset_index(inplace=True) 
    
    return df_ch
    

def generate_report (workspace, df_list, sheet_list, filename):
    """ Exports dataframes to multi-tab excel spreasheet"""
    out_file = os.path.join(workspace, str(filename) + '.xlsx')

    writer = pd.ExcelWriter(out_file,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'count'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
        
        
def main ():

    wrks = r'\\spatialfiles.bcgov\..' 
    app_xls = os.path.join (wrks, 'data', 'AquaticPlant-WildHarvest_2005-2021_tempo.xlsx')
    hav_xls = os.path.join (wrks, 'data', 'harvest_areas_allYears_iteration0.xlsx')
    
    print ('Get applications list\n')
    df_appl = get_apps_list(app_xls)
    
    print ('Get Harvest Area list\n')
    df_hav = get_harvAreas (hav_xls)
    
    cols = ['Appl_num', 'Harvest_area', '2022', '2021', '2020', '2019', '2018']  
    
    print ('Check Harvest Areas \n')
    df_sp = check_spatial (df_appl, df_hav, cols)
    df_sp['HAS_SPATIAL'] = df_sp.apply(lambda x: sp_func(x['2018'], x['2019'], x['2020'], x['2021'], x['2022']), axis=1)
    df_sp.loc[(df_sp['Harvest_area'] == '5017/5109'),'HAS_SPATIAL']='Y'
    
    print ('Populate Hectare data \n')
    df_ar = compare_hectare (df_appl,df_hav,cols)
    
    print ('Calculate the Percent Change \n')
    df_ch = caluclate_pctChange (df_ar)
    
    print ("Generate the final report.")
    df_list = [df_sp,df_ar,df_ch]
    sheet_list = ['Shapes - check', 'Area- Hectares', 'Area - % Change']
    filename = 'harvestAreas_checkSpatial_v3'
    generate_report (wrks, df_list, sheet_list , filename)
    
    
    
main ()
    
