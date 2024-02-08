import os
import cx_Oracle
import pandas as pd


def filter_TITAN (titan_report):
    """Returns a df of filtered TITAN report"""
    #Read TITAN report into df
    df = pd.read_excel(titan_report, 'TITAN_RPT012',
                       converters={'FILE #':str})

    df = df.loc[(df['STAGE'] == 'TENURE') &
               (df['STATUS'] == 'DISPOSITION IN GOOD STANDING')]

    return df


def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


def read_query(connection,query):
    "Returns a df containing results of SQL Query "
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        names = [ x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=names)
    
    finally:
        if cursor is not None:
            cursor.close()

def load_sql(params):
    """Return a dictionnary containing the SQL queries to run"""
    sqls = {}
    sqls['ip_pip'] = f"""
                      SELECT
                         ipr.INTRID_SID, pip.CNSLTN_AREA_NAME, pip.CONTACT_ORGANIZATION_NAME,
                         ROUND((SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(pip.SHAPE,ipr.SHAPE, 0.005), 0.005, 'unit=HECTARE')/ 
                         SDO_GEOM.SDO_AREA(ipr.SHAPE, 0.005, 'unit=HECTARE'))*100, 2) OVERLAP_PERCENT
                       
                    FROM
                        WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES ipr
                          INNER JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip 
                            ON SDO_RELATE (pip.SHAPE, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                     
                         WHERE pip.CNSLTN_AREA_NAME = q'[Hul'qumi'num Nations - Marine Territory]'
                           AND ipr.INTRID_SID IN ({params['parcel_list']})
                     """
    return sqls

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
    writer.close()

            
def main ():
    print ("Filtering TITAN report...\n")
    workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20211207_aqua_cancelled\TESTS'
    titan_report = os.path.join(workspace, 'TITAN_RPT012.xlsx')
    df_dtid = filter_TITAN (titan_report)

    print ("Connect to BCGW...\n")    
    bcgw_host = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,bcgw_host)
    
    print ("Execute SQL Queries...\n")
    params = {'parcel_list':",".join (str(x) for x in df_dtid['INTEREST PARCEL ID'].tolist())}
    sqls = load_sql(params)
    
    df_sql = read_query(connection, sqls['ip_pip'])
    
    print ("Create report(s)...\n")
    df = pd.merge(df_dtid, df_sql, left_on='INTEREST PARCEL ID', right_on= 'INTRID_SID')
    
    cols = ['ORG. UNIT', 'FILE #', 'DTID','CNSLTN_AREA_NAME', 'CONTACT_ORGANIZATION_NAME',
            'STAGE', 'STATUS', 'STATUS CHANGED DATE', 'APPLICATION TYPE','TYPE', 'SUBTYPE', 
            'PURPOSE', 'SUBPURPOSE', 'COMMENCEMENT DATE', 'EXPIRY DATE', 'TOTAL AREA', 'CLIENT NAME', 'LOCATION']
    
    df = df[cols]
    df.sort_values(by = ['STATUS CHANGED DATE'], inplace = True)
    
    print ("Generate Spreadsheet...\n")
    generate_report (workspace, [df], ['AQUA DIG FN'], 'test_report')
    
    print ("Processing Completed!")
    
main ()
