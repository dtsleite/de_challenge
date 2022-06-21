
def get(name):
    
    config = {
        'TABLE_TRIP_RAW':'tbl_trip_raw',
        'DATABASE':'db_de_challenge', 
        'USER':'postgres',
        'PASSWORD':'postgres',
        'HOST':'localhost',
        'PORT':'5432',
        'FILE_PATH_RAW_TABLE':'data/trips.csv',
        'E_MAIL_RECEIVER':'dtsleite@gmail.com'
    }

    return config[name]

