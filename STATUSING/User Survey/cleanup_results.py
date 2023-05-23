import os
import pandas as pd

# input csvs
wks = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20230419_status_survey\Unprocessed'

df01 = pd.read_csv(os.path.join(wks,'automated_status_tool_and_universal_overlap_tool_user_survey_submissions_v7.csv'))
df02 = pd.read_csv(os.path.join(wks,'automated_status_tool_and_universal_overlap_tool_user_survey_submissions_v8.csv'))
df03 = pd.read_csv(os.path.join(wks,'automated_status_tool_and_universal_overlap_tool_user_survey_submissions_v9.csv'))
df04 = pd.read_csv(os.path.join(wks,'automated_status_tool_and_universal_overlap_tool_user_survey_submissions_v10.csv'))

# read and concatinate csvs
df= pd.concat([df01,df02,df03,df04])
df = df.reset_index(drop= True)

# convert True/False to Yes/No to avoid confusion later with NaN
df = df.replace({True: 'Yes', False: 'No'})

# filter out 'no' and 'no_never' responses
#df = df.loc[df['use_tools_yn'].isin(['yes','user_outputs'])]


# cleanup user_works_for column
df.loc[df['user_works_for'] == 'other', 'user_works_for'] = df['user_works_for_other']
df.loc[df['user_works_for']=='Ministry of Tourism, Arts, Culture and Sport', 'user_works_for'] = 'TACS'
df.loc[df['user_works_for']=='Ministry of Citizens Services', 'user_works_for'] = 'CITZ'
df.loc[df['use_tools_yn'].isin(['no','no_never']),'user_works_for'] = 'not_user'
df.drop('user_works_for_other', axis=1, inplace=True)


# cleanup user_works_for_subgroup column
cols_subgr = [col for col in df.columns if col.startswith('user_works_for_')]
df['user_works_for_subgroup'] = df[cols_subgr].apply(lambda row: ''
                                              .join(filter(lambda x: x not in ['nan', 'other'], 
                                                           row.astype(str))), axis=1)
df.loc[df['user_works_for_subgroup'].str.strip() == '', 'user_works_for_subgroup'] = 'no_response'
df.loc[df['use_tools_yn'].isin(['no','no_never']),'user_works_for_subgroup'] = 'not_user'
df.loc[df['user_works_for_subgroup']=='Lands', 'user_works_for_subgroup'] = 'rOps'
df.loc[df['user_works_for_subgroup']=='Mines Competitiveness and Authorizations Division', 'user_works_for_subgroup'] = 'MCAD'
df.loc[df['user_works_for_subgroup']=='NaturalResourceInformationAndDigitalServices', 'user_works_for_subgroup'] = 'NRIDS'
df.loc[df['user_works_for_subgroup']=='NaturalResourceInformationAndDigitalServicesGeoBC', 'user_works_for_subgroup'] = 'NRIDS'
df.drop(cols_subgr, axis=1, inplace=True)
cols_w = ['user_works_for', 'user_works_for_subgroup']
new_idx = df.columns.get_loc('user_works_for') + 1
cols = list(df.columns)
cols.insert(new_idx, cols.pop(cols.index('user_works_for_subgroup')))
df = df[cols]

# cleanup the AST requestor columns
df['AST_use_frequency'].fillna('not_user', inplace=True)

df['AST_requestor'] = df.apply(lambda row: ', '.join([col.split('.')[1] 
                               for col in row.index if col.startswith('AST_requestor.') and row[col]=='Yes']), axis=1)
df.loc[df['AST_requestor']=='', 'AST_requestor'] = 'not_user'
df.loc[df['AST_requestor'] == 'other', 'AST_requestor'] = df['AST_requestor_other']
df.loc[df['AST_requestor']=='Ministry of Tourism, Arts, Culture and Sport', 'AST_requestor'] = 'TACS'
df.drop(columns=df.filter(regex='^AST_requestor[._]').columns, inplace=True)
new_idx = df.columns.get_loc('AST_use_frequency') + 1
cols = list(df.columns)
cols.insert(new_idx, cols.pop(cols.index('AST_requestor')))
df = df[cols]

# cleanup the AST requestor subgroup columns
f_cols = ['AST_MAg_subgroup','AST_EMLI_subgroup','AST_MoE_subgroup',
          'AST_MIRR_subgroup','AST_MoTI_subgroup','AST_MoTI_subgroup',
          'AST_other_subgroup','AST_FOR_subgroup_other','AST_WLRS_subgroup_other']

df['AST_requestor_subgroup1'] = df[f_cols].apply(lambda row: ''
                                         .join(filter(lambda x: x not in ['nan', 'other'], 
                                                       row.astype(str))), axis=1)


df['AST_requestor_subgroup2'] = df.apply(lambda row: ', '.join([col.split('.')[1] 
                                         for col in row.index if ('AST_FOR_subgroup.' in col or 'AST_WLRS_subgroup.' in col) 
                                         and row[col]=='Yes']), axis=1)

df.loc[df['AST_requestor_subgroup2']=='other', 'AST_requestor_subgroup2'] = ''


df['AST_requestor_subgroup'] = df.apply(lambda row: row['AST_requestor_subgroup2'] 
                                        if row['AST_requestor_subgroup1'] == '' 
                                        else row['AST_requestor_subgroup1'] + ', ' + row['AST_requestor_subgroup2'], axis=1)

df.loc[df['AST_requestor_subgroup']=='', 'AST_requestor_subgroup'] = 'not_user'

df['AST_requestor_subgroup'] = df['AST_requestor_subgroup'].str.strip(', ')

df.loc[df['AST_requestor_subgroup']=='Mines Competitiveness and Authorizations Division', 'AST_requestor_subgroup'] = 'MCAD'

df.drop(f_cols, axis=1, inplace=True)
df.drop(['AST_requestor_subgroup1','AST_requestor_subgroup2'], axis=1, inplace=True)

o_cols = [col for col in df.columns if ('AST_FOR_subgroup' in col or 'AST_WLRS_subgroup' in col)]
df.drop(o_cols, axis=1, inplace=True)

new_idx = df.columns.get_loc('AST_requestor') + 1
cols = list(df.columns)
cols.insert(new_idx, cols.pop(cols.index('AST_requestor_subgroup')))
df = df[cols]


# cleanup the UOT requestor columns
df['UOT_use_frequency'].fillna('not_user', inplace=True)

df.rename({'MIRR_MIRR_subgroup': 'UOT_MIRR_subgroup'}, axis=1, inplace=True)

df['UOT_requestor'] = df.apply(lambda row: ', '.join([col.split('.')[1] 
                               for col in row.index if col.startswith('UOT_requestor.') and row[col]=='Yes']), axis=1)
df.loc[df['UOT_requestor']=='', 'UOT_requestor'] = 'not_user'
df.loc[df['UOT_requestor'] == 'other', 'UOT_requestor'] = df['UOT_requestor_other']
df.loc[df['UOT_requestor']=='Ministry of Citizens Services', 'UOT_requestor'] = 'CITZ'
df.drop(columns=df.filter(regex='^UOT_requestor[._]').columns, inplace=True)
new_idx = df.columns.get_loc('UOT_use_frequency') + 1
cols = list(df.columns)
cols.insert(new_idx, cols.pop(cols.index('UOT_requestor')))
df = df[cols]

# cleanup the UOT requestor subgroup columns
f_cols = ['UOT_MAg_subgroup','UOT_EMLI_subgroup','UOT_MoE_subgroup',
          'UOT_MIRR_subgroup','UOT_MoTI_subgroup','UOT_MoTI_subgroup',
          'UOT_other_subgroup','UOT_FOR_subgroup_other','UOT_WLRS_subgroup_other']

df['UOT_requestor_subgroup1'] = df[f_cols].apply(lambda row: ''
                                         .join(filter(lambda x: x not in ['nan', 'other'], 
                                                       row.astype(str))), axis=1)


df['UOT_requestor_subgroup2'] = df.apply(lambda row: ', '.join([col.split('.')[1] 
                                         for col in row.index if ('UOT_FOR_subgroup.' in col or 'UOT_WLRS_subgroup.' in col) 
                                         and row[col]=='Yes']), axis=1)

df.loc[df['UOT_requestor_subgroup2']=='other', 'UOT_requestor_subgroup2'] = ''



df['UOT_requestor_subgroup'] = df.apply(lambda row: row['UOT_requestor_subgroup2'] 
                                        if row['UOT_requestor_subgroup1'] == '' 
                                        else row['UOT_requestor_subgroup1'] + ', ' + row['UOT_requestor_subgroup2'], axis=1)

df.loc[df['UOT_requestor_subgroup']=='', 'UOT_requestor_subgroup'] = 'not_user'


df['UOT_requestor_subgroup'] = df['UOT_requestor_subgroup'].str.strip(', ')

df.loc[df['UOT_requestor_subgroup']=='Mines Competitiveness and Authorizations Division', 'UOT_requestor_subgroup'] = 'MCAD'

new_idx = df.columns.get_loc('UOT_requestor_subgroup2') + 1
cols = list(df.columns)
cols.insert(new_idx, cols.pop(cols.index('UOT_requestor_subgroup')))
df = df[cols]

df.drop(f_cols, axis=1, inplace=True)
df.drop(['UOT_requestor_subgroup1','UOT_requestor_subgroup2'], axis=1, inplace=True)

o_cols = [col for col in df.columns if ('UOT_FOR_subgroup' in col or 'UOT_WLRS_subgroup' in col)]
df.drop(o_cols, axis=1, inplace=True)

new_idx = df.columns.get_loc('UOT_requestor') + 1
cols = list(df.columns)
cols.insert(new_idx, cols.pop(cols.index('UOT_requestor_subgroup')))
df = df[cols]


#make a second df for vizualisation purposes
df_viz = df.copy()

for col in ['AST_requestor','AST_requestor_subgroup','UOT_requestor','UOT_requestor_subgroup']:
    df_viz[col] = df_viz[col].apply(lambda x: 'Multiple' if ',' in str(x) else x)

for col in ['use_tools_yn']:
    df_viz.loc[df[col]=='no_never', col] = '1.no_never'
    df_viz.loc[df[col]=='no', col] = '2.no'
    df_viz.loc[df[col]=='user_outputs', col] = '3.user_outputs'
    df_viz.loc[df[col]=='yes', col] = '4.yes'

    
for col in ['AST_use_frequency','UOT_use_frequency']:
    df_viz.loc[df[col]=='never', col] = '1.never'
    df_viz.loc[df[col]=='rarely', col] = '2.rarely'
    df_viz.loc[df[col]=='sometimes', col] = '3.sometimes'
    df_viz.loc[df[col]=='often', col] = '4.often'
    df_viz.loc[df[col]=='frequently', col] = '5.frequently'
    
    
#export a cleaned up versions
df.to_csv ('survey_results_clean.csv')
df_viz.to_csv ('survey_results_clean_viz.csv')
