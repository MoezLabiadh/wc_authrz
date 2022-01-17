import os
import pandas as pd
import xlsxwriter


def filter_data (titan_report):
    """Returns filtered dataframes"""
    # read TITAN report into dataframe
    df = pd.read_excel (titan_report,'TITAN_RPT012',
                              converters={'FILE #':str})


    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
    df.loc[df['PURPOSE'] == 'AQUACULTURE', 'DISTRICT OFFICE'] = 'AQUA'

    df.sort_values(by='STATUS CHANGED DATE', ascending=False, inplace = True)

    df_dig = df.loc [(df['STATUS'] == 'DISPOSITION IN GOOD STANDING') &
                     (df['FILE #'] != '0000000')]

    df_exp = df.loc[(df['STAGE'] == 'APPLICATION') &
                    (df['STATUS'] == 'ACCEPTED') &
                    (df['APPLICATION TYPE'] == 'REP') &
                    (~df['FILE #'].isin(df_dig['FILE #'].tolist()))]


    return df_dig, df_exp


def generate_report (workspace, df_list, sheet_list):
    """ Exports dataframes to multi-tab excel spreasheet"""
    file_name = os.path.join(workspace, 'Crown_tenures_ALL_active_expired_asof20220117.xlsx')

    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]
        workbook = writer.book

        worksheet.set_column(0, dataframe.shape[1], 20)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'count'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()


def main():
    """Runs the program"""

    workspace = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20220117_ALLtenure_holders_expiredExisting'
    titan_report = os.path.join(workspace, 'TITAN_RPT012.xlsx')

    print ("Filtering TITAN report...")
    df_dig, df_exp = filter_data (titan_report)

    cols = ['DISTRICT OFFICE','FILE #', 'DTID',  'STAGE', 'STATUS','STATUS CHANGED DATE', 'APPLICATION TYPE',
          'TYPE', 'SUBTYPE', 'PURPOSE', 'SUBPURPOSE','COMMENCEMENT DATE',
          'EXPIRY DATE','LOCATION', 'LEGAL DESCRIPTION','CLIENT NAME','INTERESTED PARTY', 'ADDRESS LINE 1', 'ADDRESS LINE 2','ADDRESS LINE 3','CITY', 'PROVINCE', 'POSTAL CODE',
          'COUNTRY','STATE','ZIP CODE']

    df_dig = df_dig[cols]
    df_exp = df_exp[cols]

    print ("Create report...")
    sheet_list = ['Existing Tenures','Expired Tenures']
    generate_report (workspace, [df_dig,df_exp], sheet_list)

    print ('Processing completed! Check Output folder for results!')


if __name__ == "__main__":
    main()
