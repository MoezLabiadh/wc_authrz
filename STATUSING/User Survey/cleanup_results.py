import pandas as pd

# input csvs
f_v7= r'AST_UOT_user_survey_submissions_v7_20230427_1100.csv'
f_v9= r'AST_UOT_user_survey_submissions_v9_20230427_1100.csv'

# read and concatinate csvs
df_v7= pd.read_csv(f_v7)
df_v9= pd.read_csv(f_v9)
df= pd.concat([df_v7,df_v9])
df = df.reset_index(drop= True)

# convert True/False to Yes/No to avoid confusion later with NaN
df = df.replace({True: 'Yes', False: 'No'})

# filter out 'no' and 'no_never' responses
df = df.loc[df['use_tools_yn'].isin(['yes','user_outputs'])]


# cleanup user_works_for column
df.loc[df['user_works_for'] == 'other', 'user_works_for'] = df['user_works_for_other']
df.drop('user_works_for_other', axis=1, inplace=True)


# cleanup user_works_for_subgroup column
cols_subgr = [col for col in df.columns if col.startswith('user_works_for_')]
df['user_works_for_subgroup'] = df[cols_subgr].apply(lambda row: ''
                                              .join(filter(lambda x: x not in ['nan', 'other'], 
                                                           row.astype(str))), axis=1)
df.loc[df['user_works_for_subgroup'].str.strip() == '', 'user_works_for_subgroup'] = 'no_response'
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

#export a cleaned up version
df.to_csv ('survey_results_clean.csv')
