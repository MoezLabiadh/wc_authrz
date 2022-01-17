import os
import cx_Oracle
import pandas as pd
import xlsxwriter

def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database!")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


def get_phone_info (connection):
    """Returns a df with tenure holders' phone numbers"""
    query = 'SELECT* FROM WHSE_TANTALIS.TA_INTERESTED_PARTIES'
    df_ph = pd.read_sql(query, con=connection)

    df_ph ['WORK_AREA_CODE'].astype(str)
    df_ph ['WORK_PHONE_NUMBER'].astype(str)
    df_ph = df_ph[['INTERESTED_PARTY_SID', 'WORK_AREA_CODE', 'WORK_PHONE_NUMBER']]


    return df_ph



def filter_data (titan_report):
    """Returns filtered dataframes"""
    # read TITAN report into dataframe
    df = pd.read_excel (titan_report,'TITAN_RPT012',
                              converters={'FILE #':str})

    subpurposes = ['FLOATING COMMUNITY', 'FLOATING CABIN', 'COMMERCIAL WHARF', 'MARINA', 'HUNTING/FISHING CAMP', 'RESORT HUNT/FISH CAMPS & WHARVES']
    df = df.loc[df['SUBPURPOSE'].isin(subpurposes)]

    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
    df.rename(columns={'INTERESTED PARTY': 'INTERESTED_PARTY_SID'}, inplace=True)

    df.sort_values(by='STATUS CHANGED DATE', ascending=False, inplace = True)

    df_dig = df.loc [(df['STATUS'] == 'DISPOSITION IN GOOD STANDING') &
                     (df['FILE #'] != '0000000')]

    df_exp = df.loc[(df['STAGE'] == 'APPLICATION') &
                    (df['STATUS'] == 'ACCEPTED') &
                    (df['APPLICATION TYPE'] == 'REP') &
                    (~df['FILE #'].isin(df_dig['FILE #'].tolist()))]

    print (df_dig.shape[0])
    print (df_exp.shape[0])

    return df_dig, df_exp


def generate_report (workspace, df_list, sheet_list):
    """ Exports dataframes to multi-tab excel spreasheet"""
    file_name = os.path.join(workspace, 'CL_floatingStructures_exisitingExpired_asof20220117.xlsx')

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

    workspace = r'...'
    titan_report = os.path.join(workspace, 'TITAN_RPT012.xlsx')

    print ("Get phone numbers...")
    username = ''
    password = ''
    hostname = ''
    connection = connect_to_DB (username,password,hostname)
    df_ph = get_phone_info (connection)


    print ("Filtering TITAN report...")
    df_dig, df_exp = filter_data (titan_report)

    cols = ['DISTRICT OFFICE','FILE #', 'DTID',  'STAGE', 'STATUS','STATUS CHANGED DATE', 'APPLICATION TYPE',
          'TYPE', 'SUBTYPE', 'PURPOSE', 'SUBPURPOSE','COMMENCEMENT DATE',
          'EXPIRY DATE','LOCATION','CLIENT NAME', 'ADDRESS LINE 1', 'ADDRESS LINE 2','ADDRESS LINE 3','CITY', 'PROVINCE', 'POSTAL CODE',
          'COUNTRY','STATE','ZIP CODE', 'INTERESTED_PARTY_SID']

    df_dig = df_dig[cols]
    df_dig = pd.merge(df_dig, df_ph, how='left', on='INTERESTED_PARTY_SID')
    df_exp = df_exp[cols]
    df_exp = pd.merge(df_exp, df_ph, how='left', on='INTERESTED_PARTY_SID')

    print ("Create report...")
    sheet_list = ['Existing Tenures','Expired Tenures']
    generate_report (workspace, [df_dig,df_exp], sheet_list)

    print (df_dig.shape[0])
    print (df_exp.shape[0])

    print ('Processing completed! Check Output folder for results!')


if __name__ == "__main__":
    main()
