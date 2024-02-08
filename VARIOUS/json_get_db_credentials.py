import os
import json
import cx_Oracle

def get_db_cnxinfo (dbname='BCGW'):
    """ Retrieves the db username and password from the config file"""
    
    with open(r'H:\config\db_config.json', 'r') as file:
        data = json.load(file)
        
    if dbname in data:
        cnxinfo = data[dbname]

        return cnxinfo
    
    
    raise KeyError(f"Database '{dbname}' not found.")



cnxinfo= get_db_cnxinfo(dbname='BCGW')

#test



def connect_to_DB (username,password,hostname):
    """ Returns a connection to Oracle database"""
    try:
        connection = cx_Oracle.connect(username, password, hostname, encoding="UTF-8")
        print  ("Successffuly connected to the database")
    except:
        raise Exception('Connection failed! Please verifiy your login parameters')

    return connection



connection= connect_to_DB (cnxinfo['username'],cnxinfo['password'],cnxinfo['hostname'])