import json

def get_db_credentials(dbname='BCGW'):
    """ Retrieves the db username and password from the config file"""
    
    with open(r'H:\credentials\db_config.json', 'r') as file:
        data = json.load(file)
        
    if dbname in data:
        credentials = data[dbname]
        username = credentials.get("username")
        password = credentials.get("password")
        
        return username, password
    
    
    raise KeyError(f"Database '{dbname}' not found.")

