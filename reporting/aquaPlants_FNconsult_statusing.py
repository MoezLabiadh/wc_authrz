#-------------------------------------------------------------------------------
# Name:        Aqua Plants - Statusing Report
#
# Purpose:     This script generates a statusing report based on Aqua Plants Harvest Areas
#
# Author:      Moez Labiadh - FCBC, Nanaimo
#
# Created:     26-11-2021
# Updated:
#-------------------------------------------------------------------------------

import os
import arcpy
import pandas as pd
import numpy as np
import datetime
from ast import literal_eval

def get_stat_rules(workspace):
    """Return a Dictionnary of layers and fields required for statusing"""
    f = os.path.join(workspace, 'statusing_rules.xlsx')
    df = pd.read_excel(f, 'list_layers')
    keys = df['layer'].astype(str)
    values = df['fields'].apply(literal_eval)
    rules_dict = dict(zip(keys, values))

    return rules_dict


def create_bcgw_connection(workspace, bcgw_user_name,bcgw_password):
    """Returns a BCGW connnection file"""

    name = 'Temp_BCGW'
    database_platform = 'ORACLE'
    account_authorization  = 'DATABASE_AUTH'
    instance = 'bcgw.bcgov/idwprod1.bcgov'
    username = bcgw_user_name
    password = bcgw_password
    bcgw_conn_path = os.path.join(workspace,'Temp_BCGW.sde')
    if not arcpy.Exists(bcgw_conn_path):
        arcpy.CreateDatabaseConnection_management (workspace,name, database_platform,instance,account_authorization,
                                               username ,password, 'DO_NOT_SAVE_USERNAME')
    else:
        pass

    return bcgw_conn_path


def overlay_analysis (mxd,dframe,rules_dict,bcgw_conn_path):
    """Runs the Overlay Analysis and exports results to df"""
    layersList = arcpy.mapping.ListLayers(mxd,"",dframe)
    harv_areas = layersList[0]

    df_list = []
    sheet_list = []

    for lyr in layersList[1:]:
        for k, v in rules_dict.items():
            if lyr.name == k:
                print ('..Working on: {}'.format (k))

                print ('...Retrieving feature layer')
                if 'WHSE' in lyr.datasetName:
                    interLyr = os.path.join(bcgw_conn_path, lyr.datasetName)
                else:
                    interLyr = lyr.dataSource

                print ('...Intersecting features')
                if lyr.name == 'Marine Parks':
                    where_clause = """ PROTECTED_LANDS_NAME like '%MARINE%' """

                elif lyr.name == 'MaPP (PMZs) 1B and II only':
                    where_clause = """ NON_LEGAL_FEAT_OBJECTIVE in ('Protection Management Zone - II' ,
                                       'Protection Management Zone - lb') """

                elif lyr.name == 'FN Treaty Side Agreements':
                    where_clause = """ STATUS = 'ACTIVE' """

                else:
                    where_clause = None

                feat_lyr = 'feat_lyr'
                arcpy.MakeFeatureLayer_management (interLyr, feat_lyr,where_clause)

                if arcpy.Exists('in_memory\_intersect'):
                    arcpy.Delete_management('in_memory\_intersect')
                arcpy.Intersect_analysis ([harv_areas,feat_lyr], 'in_memory\_intersect')

                print ('...Adding Areas')
                exist_fields = [f.name for f in arcpy.ListFields('in_memory\_intersect')]
                area_field = 'OVERLAP_AREA_HA'
                if area_field not in exist_fields:
                     arcpy.AddField_management('in_memory\_intersect', area_field, 'DOUBLE')


                with arcpy.da.UpdateCursor('in_memory\_intersect', [area_field, 'SHAPE@AREA']) as cursor:
                    for row in cursor:
                        row[0] = round (float(row[1]/10000), 1)
                        cursor.updateRow(row)

                print ('...Exporting to pandas df')
                v.append(area_field)
                v.insert(0, 'Harvest_Area_Number')
                arr = arcpy.da.FeatureClassToNumPyArray(
                in_table='in_memory\_intersect',
                field_names=v,
                skip_nulls=False,
                null_value=-99999)

                df = pd.DataFrame (arr)
                df.sort_values(by=['Harvest_Area_Number'], inplace = True)

                if k == 'FN PIP Consultation Areas':
                    df = df.groupby(['Harvest_Area_Number','CNSLTN_AREA_NAME','CONTACT_ORGANIZATION_NAME'], as_index=False)[area_field]\
                           .agg('sum')

                    print (df.head())
                else:
                    pass


                if df.shape [0] < 1:
                    df = df.append({'Harvest_Area_Number' : 'NO OVERLAPS FOUND!'}, ignore_index=True)
                else:
                    pass

                df_list.append (df)
                sheet_list.append (k)


    return df_list, sheet_list


def generate_report (workspace, df_list, sheet_list):
    """ Exports dataframes to multi-tab excel spreasheet"""
    file_name = os.path.join(workspace, '2022_aquaPlants_Statusing.xlsx')

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
        col_names.append ({'header' : dataframe.columns[-1], 'total_function': 'sum'})


        worksheet.add_table(0, 0, dataframe.shape[0]+1, dataframe.shape[1]-1, {
            'total_row': True,
            'columns': col_names})

    writer.save()
    writer.close()


def main():
    arcpy.env.overwriteOutput = True
    arcpy.env.qualifiedFieldNames = False
    workspace = r'\\sfp.idir.bcgov\S164\S63087\Share\FrontCounterBC\Moez\WORKSPACE\20211125_aquaPlants_FN_consult'
    print ('Getting Statusing Rules\n')
    rules_dict = get_stat_rules(workspace)

    print ('Connecting to BCGW...PLease enter your credentials')
    bcgw_user_name = raw_input("Enter your BCGW username:")
    bcgw_password = raw_input("Enter your BCGW password:")
    bcgw_conn_path = create_bcgw_connection(workspace, bcgw_user_name,bcgw_password)

    print ('Running Overlay Analysis')
    mxd = arcpy.mapping.MapDocument(os.path.join(workspace,'proj_4.mxd'))
    dframe = arcpy.mapping.ListDataFrames(mxd, "Statusing_layers")[0]
    df_list, sheet_list = overlay_analysis (mxd,dframe,rules_dict,bcgw_conn_path)

    print ('\n Creating Report')
    generate_report (workspace, df_list, sheet_list)

    arcpy.Delete_management('in_memory')

    print ('Processing Completed!')


if __name__ == "__main__":
    main()

