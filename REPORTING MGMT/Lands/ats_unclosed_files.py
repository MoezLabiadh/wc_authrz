import warnings
warnings.simplefilter(action='ignore')

import pandas as pd


def import_titan (tnt_f):
    """Reads the Titan work ledger report into a df"""
    df = pd.read_excel(tnt_f,'TITAN_RPT009',
                       converters={'FILE NUMBER':str})
    
    tasks = ['NEW APPLICATION','REPLACEMENT APPLICATION','AMENDMENT','ASSIGNMENT']
    
    df = df.loc[df['TASK DESCRIPTION'].isin(tasks) &
                (df['COMPLETED DATE'].notnull()) &
                ((df['STATUS'] == 'DISPOSITION IN GOOD STANDING') | 
                 (df['STATUS'] == 'CANCELLED'))]
    
    df.loc[((df['STATUS'] == 'CANCELLED') & (df['OFFER ACCEPTED DATE'].isnull())), 'STATUS'] = 'CANCELLED APPLICATION'
    df.loc[df['STATUS'] == 'CANCELLED', 'STATUS'] = 'CANCELLED TENURE'
    
    df.rename(columns={'COMMENTS': 'TANTALIS COMMENTS'}, inplace=True)
 

    
    for col in df:
        if 'DATE' in col:
            df[col] =  pd.to_datetime(df[col],
                                   infer_datetime_format=True,
                                   errors = 'coerce').dt.date
        elif 'Unnamed' in col:
            df.drop(col, axis=1, inplace=True)
        
            
        else:
            pass
            
    df.loc[df['PURPOSE'] == 'AQUACULTURE', 'DISTRICT OFFICE'] = 'AQUACULTURE'
    df.loc[df['DISTRICT OFFICE'] == 'COURTENAY', 'DISTRICT OFFICE'] = 'AQUACULTURE'
    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
    
    return df



def import_ats_pt (ats_pt_f):
    """Reads the ATS Processing Time report into a df"""
    df = pd.read_csv(ats_pt_f, delimiter = "\t",encoding='cp1252',error_bad_lines=False)

    df.rename(columns={'Comments': 'ATS COMMENTS',
                       'Authorization Status': 'ATS STATUS'}, inplace=True)

    df= df.loc[df['ATS STATUS'].isin(['Active', 'On Hold']) &
               df['Accepted Date'].notnull() ]

    df['Decision-making Office Name'].fillna(df['Intake Office Name'], inplace=True)
    df.loc[df['Authorization Type'].str.contains('Aquaculture'), 
           'Decision-making Office Name'] = 'Aquaculture'
    
    df['Decision-making Office Name'] = df['Decision-making Office Name'].str.upper()
    
    for index,row in df.iterrows():
        z_nbr = 7 - len(str(row['File Number']))
        df.loc[index, 'File Number'] = z_nbr * '0' + str(row['File Number'])
     
    # fill na Onhold time with 0
    df['Total On Hold Time'].fillna(0, inplace=True)
    
    for col in df:
        if 'Date' in col:
            df[col] =  pd.to_datetime(df[col],
                               infer_datetime_format=True,
                               errors = 'coerce').dt.date
        elif 'Unnamed' in col:
            df.drop(col, axis=1, inplace=True)
    
    else:
        pass
            

    return df


def merge_tnt_ats(df_tnt, df_ats):
    """Join Tatntalis and ATS dfs"""
    
    df_tnt['CREATED DATE'] = pd.to_datetime(df_tnt['CREATED DATE'])
    df_ats['Accepted Date'] = pd.to_datetime(df_ats['Accepted Date'])
    

    df = pd.merge_asof(df_tnt.sort_values('CREATED DATE'), 
                       df_ats.sort_values('Accepted Date'), 
                       left_on='CREATED DATE', 
                       right_on='Accepted Date', 
                       left_by='FILE NUMBER',
                       right_by='File Number', 
                       direction='nearest')

    
    df = df.loc[df['Region Name'].notnull()]
    
    df['diff_acc']= abs((df['CREATED DATE'] - df['Accepted Date']).dt.days)
    
    df = df.loc[(df['diff_acc'] < 180)]
    
    #df = df.loc[df['STATUS'] != 'CANCELLED APPLICATION']
    
    df['TASK STATUS'] = 'COMPLETED'
    
    df['CREATED DATE'] = pd.to_datetime(df['CREATED DATE']).dt.date
    df['Accepted Date'] = pd.to_datetime(df['Accepted Date']).dt.date
    
    
    df.sort_values(by=['CREATED DATE'], ascending=False, inplace=True)
    df.reset_index(drop = True, inplace = True)
    
    return df


def generate_report (df_list, sheet_list,filename):
    """ Exports dataframes to multi-tab excel spreasheet"""

    writer = pd.ExcelWriter(filename,engine='xlsxwriter')

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



def main():
    tnt_f= 'TITAN_RPT009.xlsx'
    ats_pt_f= '20230901_ats_pt.xls'
    
    print ('Reading TITAN report')
    df_tnt= import_titan (tnt_f)
    
    print ('Reading ATS report')
    df_ats= import_ats_pt (ats_pt_f)
    
    print ('Merging TITAN and ATS data')
    df= merge_tnt_ats(df_tnt, df_ats)
    
    cols= ['DISTRICT OFFICE', 
           'FILE NUMBER',
           'STATUS',
           'USERID ASSIGNED TO', 
           'OTHER EMPLOYEES ASSIGNED TO',
           'TASK DESCRIPTION',
           'TASK STATUS',
           'COMPLETED DATE',
           'ATS STATUS',
           'TYPE', 
           'SUBTYPE',
           'PURPOSE', 
           'SUBPURPOSE',
           'RECEIVED DATE',
           'CREATED DATE',
           'EXPIRY DATE', 
           'REPORTED DATE', 
           'ADJUDICATED DATE', 
           'OFFERED DATE',
           'OFFER ACCEPTED DATE',
           'TANTALIS COMMENTS',
           'ATS COMMENTS'
          ]
    
    df= df[cols]
    
    print ('Exportng final report')
    generate_report ([df], ['ATS Unclosed Files'],'ATS_unclosed_files.xlsx')
    

main ()

