import os
import shutil
import pandas as pd


def prep_data (wks):
    xlsx = os.path.join(wks, 'data', 'AquaticPlant-WildHarvest_2005-2021_tempo.xlsx')

    df = pd.read_excel (xlsx, '2013-2021 Master')
    df = df.loc[(df['Status'] == 'Active') &
                (df['Year'] > 2013)]
    
    cols = ['Year','Harvest_Area_Num','DFO_Area']
    
    df = df[cols]
    
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
    
    df['DFO_Area'] = df['DFO_Area'].astype(str)
    
    return df

  
def create_package (by, wks, df):
    """ Creates map packages. Argument by takes on the following:
        'by_dfo', 'by_mapp' """
    if by == 'by_dfo':
        colname = 'DFO_Area'
        fpref = 'DFO_Area_'
        
    elif by == 'by_mapp':
        colname = 'MaPP_name'
        fpref = 'MaPP_'
    
    else:
        raise Exception('ERROR: Only by_dfo or b_map values are accepted!')

    in_dir = os.path.join (wks, 'outputs' , 'maps', by)
    out_dir = os.path.join (wks, 'outputs' , 'MAP_PACKAGES', by)
     
    for file in os.listdir(in_dir):
        if os.path.isfile(os.path.join(in_dir, file)):
            if file.endswith('.pdf'):
                l = file.split("_")
                item = l[-1][:-4]
                
                print ('Creating package for {}'. format (item))
                if by == 'by_dfo':
                    df_it = df.loc[df[colname] == str(item)]
                    
                elif by == 'by_mapp':
                    df_it = df.loc[df[colname] == str(item) + ' Marine Plan']
                    
                harvArs = df_it['Harvest_Area_Num'].astype(str).unique()
                
                item_dir = os.path.join (out_dir, fpref + str(item))
                if not os.path.exists(item_dir):
                    os.makedirs(item_dir)
                    
                shutil.copy(os.path.join(in_dir, file), 
                            os.path.join(out_dir, item_dir, file))
                
                for harv in harvArs:
                    harFname = 'Map_harvestArea_{}.pdf'.format (str(harv))
                    harDir = os.path.join (wks, 'outputs' , 'maps', 'by_harvArea')
                    shutil.copy(os.path.join(harDir, harFname), 
                            os.path.join(out_dir, item_dir, harFname))                   
     
    
def main ():
    wks = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20220324_aquaPlantWilld_spatialProj'
    df = prep_data (wks)
    
    for by in ('by_mapp', 'by_dfo'):
        print ('\nWorking on Map Packages {}'.format(by))
        create_package (by, wks, df)

main()
