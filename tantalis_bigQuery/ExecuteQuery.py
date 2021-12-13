import os
import cx_Oracle
import pandas as pd
from bigQuery import get_bigQuery

def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database!")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


def execute_query (connection, query):
    """Executes SQL query and returns a df"""
    df = pd.read_sql(query, con=connection)

    return df


def filter_df (df):
    """Extracts needed information from df"""
    df_f = df.loc[(df['PURPOSE_NME'] == 'AQUACULTURE') &
                   (df['STAGE_NME'] == 'TENURE') &
                   (df['STATUS_NME'] == 'CANCELLED')]

    return df_f


def generate_report (workspace, df_list, sheet_list):
    """ Exports dataframes to multi-tab excel spreasheet"""
    file_name = os.path.join(workspace, 'output.xlsx')

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
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    username = input("Enter your BCGW username:")
    password = input("Enter your BCGW password:")
    connection = connect_to_DB (username,password,hostname)

    query = get_bigQuery(type = 'no_pid')

    print  ("Executing the bigQuery...")
    df = execute_query (connection,query)

    print  ("Extracting information...")
    df_f = filter_df (df)
    
    # do formatting stuff - convert date cols to datetime format
    df_f['DTS_EFFECTIVE_DAT'] =  pd.to_datetime(df_f['DTS_EFFECTIVE_DAT'],
                                    infer_datetime_format=True,
                                    errors = 'coerce').dt.date

    df_f['CANCELLED YEAR'] =pd.DatetimeIndex(df_f['DTS_EFFECTIVE_DAT']).year

    print  ("Exporting results...")
    df_list = [df_f]
    sheet_list = ["Master List"]
    workspace = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20211207_aqua_cancelled'
    generate_report (workspace, df_list, sheet_list)


    print ('Done!')

if __name__ == "__main__":
    main()
