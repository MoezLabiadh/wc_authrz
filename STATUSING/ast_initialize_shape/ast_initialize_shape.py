import os
import cx_Oracle
import pandas as pd
import geopandas as gpd
from datetime import date


def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("..successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection


def create_dir (path, dir):
    """ Creates new folder and returns path"""
    try:
      os.makedirs(os.path.join(path,dir))
      print('...folder {} created!'.format(dir))

    except OSError:
        print('...folder {} already exists!'.format(dir))
        pass

    return os.path.join(path,dir)

if __name__ == "__main__":
    in_tenure_nbr =   input('Enter File Number:')
    in_tenure_nbr= str(in_tenure_nbr).strip()
    
    print("\nCreating Statusing folders")
    stat_dir= r'\\spatialfiles.bcgov\Work\lwbc\visr\Workarea\moez_labiadh\WORKSPACE\20231108_landStatusing_folder_script\Lands_Statusing'
    tn_stat_dir= create_dir (stat_dir, in_tenure_nbr)
    this_month= date.today().strftime("%b%Y").lower()
    create_dir (tn_stat_dir, this_month)
    
    