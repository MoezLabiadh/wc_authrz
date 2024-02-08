import json

def get_db_cnxinfo (dbname='BCGW'):
    """ Retrieves the db username and password from the config file"""
    
    with open(r'H:\config\db_config.json', 'r') as file:
        data = json.load(file)
        
    if dbname in data:
        cnxinfo = data[dbname]

        return cnxinfo
    
    
    raise KeyError(f"Database '{dbname}' not found.")



cnxinfo= get_db_cnxinfo(dbname='BCGW')