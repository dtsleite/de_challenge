import psycopg2
import psycopg2.extras as extras
from sqlalchemy import create_engine  
from sqlalchemy.ext.declarative import declarative_base 
import config as p
import pandas as pd
import decimal, datetime
import json
import sendmail


def alchemyencoder(obj):
    """JSON encoder function for SQLAlchemy special classes."""
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)

def execute_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
    norm_tuples = [tuple(map(lambda i: str.replace(i, "'POINT ","POINT "), tuples)) for tuples in tuples]
  
    cols = ','.join(list(df.columns))
  
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)

    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()


def start_db():
    try:
        db = create_engine('postgresql://{}:{}@{}:{}/{}'.format(p.get('USER'),p.get('PASSWORD'),p.get('HOST'),p.get('PORT'),p.get('DATABASE')))
        db.autocommit = True  

        # test if table and database does not exists then create 
        db.execute("CREATE TABLE IF NOT EXISTS {} (region TEXT,origin_coord POINT,destination_coord POINT,datetime TIMESTAMP,datasource TEXT)".format(p.get('TABLE_TRIP_RAW')))  
        return 0    
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return 1


def load_raw():
    try:
        conn1 = psycopg2.connect(
            database=p.get('DATABASE'),
            user=p.get('USER'), 
            password=p.get('PASSWORD'), 
            host=p.get('HOST'), 
            port= p.get('PORT')
        )

        conn1.autocommit = True
        cursor = conn1.cursor()
        base = declarative_base()        

        data = pd.read_csv(p.get('FILE_PATH_RAW_TABLE'), encoding='utf-8', sep = ',', header=0)

        # transform some data for geometry data type insert compatibility    
        data['origin_coord'] = data['origin_coord'].str[7:-1].str.replace(' ',',')
        data['destination_coord'] = data['destination_coord'].str[7:-1].str.replace(' ',',')

        # remove duplicates for especified columns
        df_dedup = data.drop_duplicates(subset=['origin_coord','destination_coord','datetime'], keep=False)

        execute_values(conn1, df_dedup, p.get('TABLE_TRIP_RAW'))
        
        conn1.close()

        sendmail.send_email_alert('Ingestion for trips data was completed successfuly','Data Ingestion Successful')

        return 0

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        sendmail.send_email_alert('Ingestion for trips data encountered and error and exited', 'Data Ingestion Error')
        return 1

def get_avg_per_week():
    db = create_engine('postgresql://{}:{}@{}:{}/{}'.format(p.get('USER'),p.get('PASSWORD'),p.get('HOST'),p.get('PORT'),p.get('DATABASE')))
    result = db.execute('''
        with tmp as (
                SELECT region, date_trunc('week', datetime)::date AS week, COUNT(*) as total_per_week
                FROM tbl_trip_raw GROUP BY region,datetime
        )
        SELECT region, week, ROUND(AVG(total_per_week),2) as avg_per_week FROM tmp GROUP BY region,week ORDER BY 2
    ''')
    rows = result.fetchall()
    result.close()
    data = rows
    return json.dumps([dict(r) for r in rows], default=alchemyencoder)

    


    