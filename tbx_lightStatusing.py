import os
import sys
#import getpass
#import arcpy
import cx_Oracle
import pandas as pd
from datetime import date

def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        arcpy.AddMessage  ("Successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


def get_table_cols (item,status_xls):
    """Returns table and field names based on the Status tool common datasets spreadsheet"""
    excel = pd.ExcelFile(status_xls)
    df_stat = pd.read_excel(excel)
    df_stat = df_stat.loc[df_stat['Featureclass_Name(valid characters only)'] == item]
    df_stat.fillna(value='nan',inplace=True)

    table = df_stat['Datasource'].iloc[0]

    fields = []
    fields.append('b.' + str(df_stat['Fields_to_Summarize'].iloc[0].strip()))

    for f in range (2,7):
        for i in df_stat['Fields_to_Summarize' + str(f)].tolist():
            if i != 'nan':
                fields.append('b.' + str(i.strip()))

    cols = ','.join(x for x in fields)

    return table, cols

def get_def_query (item,status_xls):
    """Returns table and field names based on the Status tool common datasets spreadsheet"""
    excel = pd.ExcelFile(status_xls)
    df_stat = pd.read_excel(excel)
    df_stat = df_stat.loc[df_stat['Featureclass_Name(valid characters only)'] == item]
    df_stat.fillna(value='nan',inplace=True)

    elem = df_stat['Definition_Query'].iloc[0].strip()

    if elem == 'nan':
        def_query = " "
    else:
       elem = elem.replace('"', '')
       def_query = 'AND b.' + elem

    return def_query

def get_geom_colname (table, connection):
    """ Returns the geometry column name: can be either SHAPE or GEOMETRY"""
    el_list = table.split('.')

    geom_query = """
                    SELECT column_name GEOM_NAME
                    FROM  ALL_SDO_GEOM_METADATA
                    WHERE owner = '{owner}'
                    AND table_name = '{tab_name}'
                       """. format (owner =el_list[0].strip(),
                                    tab_name =el_list[1].strip())
    df_col = df = pd.read_sql(geom_query, con=connection)
    geom_col = df_col ['GEOM_NAME'].iloc[0]

    return geom_col


def generate_report (workspace, df_list, sheet_list):
    """ Exports dataframes to multi-tab excel spreasheet"""
    today = date.today().strftime("%Y%m%d")
    file_name = os.path.join(workspace, 'LightStatusing_' + today + '.xlsx')

    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')

    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe = dataframe.reset_index(drop=True)
        dataframe.index = dataframe.index + 1

        dataframe.to_excel(writer, sheet_name=sheet, index=False, startrow=0 , startcol=0)

        worksheet = writer.sheets[sheet]
        workbook = writer.book

        worksheet.set_column(0, 3, 20)
        worksheet.set_column(4, dataframe.shape[1], 30)

        col_names = [{'header': col_name} for col_name in dataframe.columns[1:-1]]
        col_names.insert(0,{'header' : dataframe.columns[0], 'total_string': 'Total'})
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'count'})

        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()


def main ():
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    #bcgw_user_name = ''

    #bcgw_user_name = getpass.getuser()
    #bcgw_password = getpass.getpass("Please enter BCGW password for User Name : %s" % bcgw_user_name)

    bcgw_user_name = sys.argv[1]
    demo_bcgw_password = sys.argv[2]
    bcgw_password = ''

    disp_list = sys.argv[3].replace(" ", "")
    radius = sys.argv[4]
    selected = sys.argv[5]
    items = selected.split(';')
    workspace = sys.argv[6]

    arcpy.AddMessage ('Connecting to BCGW ...')
    connection = cx_Oracle.connect(bcgw_user_name, bcgw_password, hostname, encoding="UTF-8")

    status_xls = r'\\GISWHSE.ENV.GOV.BC.CA\whse_np\corp\script_whse\python\Utility_Misc\Ready\statusing_tools\statusing_input_spreadsheets\one_status_common_datasets.xls'


    arcpy.AddMessage ('Executing Queries ...')
    df_list = []
    sheet_list = []
    counter = 1
    for item in items:
        item = item.replace("'", "")

        arcpy.AddWarning('..{} of {}: {}'.format(counter, len(items),item))
        table, cols = get_table_cols (item,status_xls)
        geom_col = get_geom_colname (table, connection)
        def_query = get_def_query (item,status_xls)

        query_interact = """
                       SELECT a.CROWN_LANDS_FILE, a.DISPOSITION_TRANSACTION_SID, a.INTRID_SID, {cols}
                       FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a,
                            {table} b
                       WHERE a.DISPOSITION_TRANSACTION_SID in ({disp_list})
                         {def_query}
                         AND SDO_ANYINTERACT (b.{geom_col}, a.SHAPE) = 'TRUE'

                    """. format(cols = cols ,
                                table = table,
                                disp_list = disp_list,
                                def_query = def_query,
                                geom_col = geom_col)

        query_buffer =  """
                       SELECT a.CROWN_LANDS_FILE, a.DISPOSITION_TRANSACTION_SID, a.INTRID_SID, {cols}
                       FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a,
                            {table} b
                       WHERE a.DISPOSITION_TRANSACTION_SID in ({disp_list})
                         {def_query}
                         AND SDO_WITHIN_DISTANCE (b.{geom_col}, a.SHAPE,'distance = {radius}') = 'TRUE'

                    """. format(cols = cols ,
                                table = table,
                                disp_list = disp_list,
                                def_query = def_query,
                                geom_col = geom_col,
                                radius = radius)

        df_interact = pd.read_sql(query_interact, con=connection)
        df_buffer = pd.read_sql(query_buffer, con=connection)

        df_interact ['SPATIAL OVERLAY'] = 'INTERSECT'
        df_buffer ['SPATIAL OVERLAY'] = 'WITHIN {} m'.format(str(radius))

        df_all = pd.concat([df_interact, df_buffer])
        df_all.sort_values(by='SPATIAL OVERLAY', ascending=True, inplace = True)
        cols = list(df_all.columns)
        cols.remove('SPATIAL OVERLAY')

        df_all.drop_duplicates(subset=cols, keep='first', inplace=True)

        df_all.sort_values(by= ['CROWN_LANDS_FILE','DISPOSITION_TRANSACTION_SID',
                                'INTRID_SID','SPATIAL OVERLAY'], inplace = True)
        arcpy.AddMessage ('....found {} hits!'.format(df_all.shape[0]))
        cols.insert(3,'SPATIAL OVERLAY')

        df_all = df_all[cols]
        df_all.rename(columns={'DISPOSITION_TRANSACTION_SID': 'DISPOSITION_ID',
                               'CROWN_LANDS_FILE': 'FILE_NBR'}, inplace=True)

        df_list.append(df_all)

        if len (item) > 31:
            sheet = item[0:31]
        else:
            sheet = item

        sheet_list.append(sheet)

        counter +=1

    generate_report (workspace, df_list, sheet_list)

    arcpy.AddMessage  ('Processing Completed. Please check the output folder for results!')

if __name__ == "__main__":
    main()
