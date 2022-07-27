import os
import datetime
import cx_Oracle
import pandas as pd
import numpy as np


#Hide pandas warning
pd.set_option('mode.chained_assignment', None)


def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


def get_titan_report_date (titan_report):
    """ Returns the date of the input TITAN report"""
    df = pd.read_excel(titan_report,'Info')
    titan_date = df.columns[1]

    return titan_date


def filter_TITAN (titan_report, year):
    """Returns a df of filtered TITAN report"""
    #Read TITAN report into df
    df = pd.read_excel(titan_report, 'TITAN_RPT009',
                       converters={'FILE NUMBER':str,
                                   'OFFER ACCEPTED DATE':str})

    # Convert expiry date column to datetime format
    df['OFFERED DATE'] =  pd.to_datetime(df['OFFERED DATE'],
                                    infer_datetime_format=True,
                                    errors = 'coerce').dt.date

    df['EXPIRY DATE'] =  pd.to_datetime(df['EXPIRY DATE'],
                                    infer_datetime_format=True,
                                    errors = 'coerce').dt.date


    # Filter the needed data: tenures expiring during fiscal year
    df = df.loc [(df['OFFERED DATE'] >= datetime.date(year-1,9,1)) &
                 (df['OFFERED DATE'] <= datetime.date(year,8,31)) &
                 (~df['STATUS'].isin(['CANCELLED', 'EXPIRED']))]

    #Calculate Tenure Length
    df ['diff'] = ((df['EXPIRY DATE'] - df['OFFERED DATE'] )\
                                  / np.timedelta64(1,'Y'))
    df['TENURE LENGTH YEARS'] = df['diff'].fillna(0).round().astype(int)

    #Remove spaces from column names, remove special characters
    df.sort_values(by = ['OFFERED DATE'], inplace = True)
    #df['OFFERED DATE'] = df['OFFERED DATE'].astype(str)
    #df['EXPIRY DATE'] = df['EXPIRY DATE'].astype(str)
    df['DISTRICT OFFICE'] = df['DISTRICT OFFICE'].fillna(value='NANAIMO')
    #df.columns = df.columns.str.replace(' ', '_')

    return df


def load_queries ():
    """ Return the SQL queries that will be executed"""
    sql = {}
    sql['maan'] = """
                SELECT
               ipr.INTRID_SID, ipr.CROWN_LANDS_FILE, pip.CONTACT_ORGANIZATION_NAME,
               ROUND (ipr.TENURE_AREA_IN_HECTARES, 2) TENURE_HECTARE, 
               ROUND(SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(pip.SHAPE,ipr.SHAPE, 0.005), 0.005, 'unit=HECTARE'), 2) OVERLAP_HECTARE
            
            FROM
                WHSE_TANTALIS.TA_CROWN_TENURES_SVW ipr,
                WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
            
             WHERE pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
               AND ipr.TENURE_STAGE = 'TENURE' 
               AND ipr.CROWN_LANDS_FILE in ({t})
               AND SDO_RELATE (pip.SHAPE, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                 """
    
    sql['iha'] = """
                SELECT
               ipr.INTRID_SID, ipr.CROWN_LANDS_FILE, iha.AREA_TYPE, iha.TREATY_SIDE_AGREEMENT_ID,
               iha.TREATY_SIDE_AGREEMENT_AREA_ID,iha.STATUS IHA_STATUS, iha.START_DATE_TEXT, iha.END_DATE_TEXT,
               ROUND (ipr.TENURE_AREA_IN_HECTARES, 2) TENURE_HECTARE, 
               ROUND(SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(iha.GEOMETRY,ipr.SHAPE, 0.005), 0.005, 'unit=HECTARE'), 2) OVERLAP_HECTARE
            
            FROM
                WHSE_TANTALIS.TA_CROWN_TENURES_SVW ipr,
                WHSE_LEGAL_ADMIN_BOUNDARIES.FNT_TREATY_SIDE_AGREEMENTS_SP iha
            
             WHERE ipr.CROWN_LANDS_FILE in ({tm})
               AND ipr.TENURE_STAGE = 'TENURE' 
               AND iha.AREA_TYPE = 'Important Harvest Area'
               AND iha.STATUS = 'ACTIVE'
               AND SDO_RELATE (iha.GEOMETRY, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                 """    

    sql['lu'] = """
                SELECT
               ipr.INTRID_SID, ipr.CROWN_LANDS_FILE, ldu.LANDSCAPE_UNIT_NAME, 
               ROUND (ipr.TENURE_AREA_IN_HECTARES, 2) TENURE_HECTARE, 
               ROUND(SDO_GEOM.SDO_AREA(SDO_GEOM.SDO_INTERSECTION(ldu.GEOMETRY,ipr.SHAPE, 0.005), 0.005, 'unit=HECTARE'), 2) OVERLAP_HECTARE
            
            FROM
                WHSE_TANTALIS.TA_CROWN_TENURES_SVW ipr,
                WHSE_LAND_USE_PLANNING.RMP_LANDSCAPE_UNIT_SVW ldu
            
             WHERE ipr.CROWN_LANDS_FILE in ({tm})
               AND ipr.TENURE_STAGE = 'TENURE' 
               AND SDO_RELATE (ldu.GEOMETRY, ipr.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
                 """ 
                 
    return sql
         

def read_query(connection,query):
    "Returns a df containing SQL Query results"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        names = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=names)
    
    finally:
        if cursor is not None:
            cursor.close()
            

def generate_report (workspace, df_list, sheet_list,filename):
    """ Exports dataframes to multi-tab excel spreasheet"""
    file_name = os.path.join(workspace, filename+'.xlsx')

    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')

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
    """Runs the program"""
    
    workspace = r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20220719_maanulth_annual_report_2022'
    titan_report = os.path.join(workspace, 'TITAN_RPT009.xlsx')
    
    print ('Connecting to BCGW...')
    hostname = 'bcgw.bcgov/idwprod1.bcgov'
    bcgw_user = os.getenv('bcgw_user')
    bcgw_pwd = os.getenv('bcgw_pwd')
    connection = connect_to_DB (bcgw_user,bcgw_pwd,hostname)
        
    titan_date = get_titan_report_date (titan_report)
    print ('Titan report date/time is: {}'.format (titan_date))
    
    print ("TITAN filtering: Getting Offered Tenures...")
    year = 2022
    df_off = filter_TITAN (titan_report,year)
    s_off = ",".join("'" + str(x) + "'" for x in df_off['FILE NUMBER'].tolist())
    
    print ("SQL-1: Getting Tenures within Maanulth Territory...")
    sql = load_queries ()
    
    query = sql['maan'].format(t= s_off)
    df_maan = read_query(connection,query)
    
    df_maan = df_maan.groupby('CROWN_LANDS_FILE')[['TENURE_HECTARE', 'OVERLAP_HECTARE']].apply(sum).reset_index()
    
    df_maan = pd.merge(df_maan, df_off, how= 'left', left_on='CROWN_LANDS_FILE', right_on='FILE NUMBER')
    
    
    df_maan = df_maan[['FILE NUMBER', 'DISTRICT OFFICE', 'STATUS', 'TASK DESCRIPTION', 'OFFERED DATE', 
                       'OFFER ACCEPTED DATE','EXPIRY DATE', 'TENURE LENGTH YEARS','TYPE','SUBTYPE','PURPOSE','SUBPURPOSE',
                       'TENURE_HECTARE','OVERLAP_HECTARE']]
    
    df_maan.sort_values(by = ['FILE NUMBER'], inplace = True)
    
    s_maan= ",".join("'" + str(x) + "'" for x in df_maan['FILE NUMBER'].tolist())
    
    print ("SQL-2: Getting overlaps with Important Harvest Areas...")
    query = sql['iha'].format(tm= s_maan)
    df_iha = read_query(connection,query)
    
    df_iha.sort_values(by = ['CROWN_LANDS_FILE'], inplace = True)
    
    print ("SQL-3: Getting overlaps with Landscape Units...")
    query = sql['lu'].format(tm= s_maan)
    df_lu = read_query(connection,query)
    
    df_lu.sort_values(by = ['CROWN_LANDS_FILE'], inplace = True)
    
    df_lu_sum = df_lu.groupby('LANDSCAPE_UNIT_NAME')[['OVERLAP_HECTARE']].apply(sum).reset_index()
    
    print ('Generating the report...')
    df_list = [df_maan,df_iha,df_lu, df_lu_sum]
    sheet_list = ['Offered Tenures in Maanulth', 'Overlay - IHA','Overlay - LU', 'LU Area Summary']
    filename = 'Maanulth_annualReporting_{}_tables'.format(str(year))
    generate_report (workspace, df_list, sheet_list,filename)
    
main ()
