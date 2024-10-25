import pandas as pd
import sqlalchemy 
from sqlalchemy import create_engine
from datetime import datetime , timedelta
import tempfile
import os
import psycopg2


def read_load_data():
    conn_params = {
    "host": "localhost",
    "database": "cropdb",
    "user": "postgres",
    "password": "mypassword",
    "port": "6000"  # Default is 5432
    }
    
    conn = psycopg2.connect(**conn_params)
    directory = "/home/kali/Documents/object_storage"
    current_time = datetime.now()
    oneminuteago = current_time - timedelta(minutes=5)
    query = ''' 
        SELECT * FROM crops   
        WHERE inserted_at >= %s AND inserted_at <= %s '''
    query1 = ''' SELECT * FROM crops '''

    df = pd.read_sql(query1 , conn)
    partitions = {   s_name : data  for s_name , data in df.groupby('state_name')    }
    print(partitions)

    with tempfile.TemporaryDirectory() as tmpdir:
        for s_name , data in partitions.items():
            name = s_name
            name = name.replace(" ","_").lower()
            name_timestamp = name + str(current_time)

            file_path = os.path.join(tmpdir , f"{name_timestamp}.parquet" )
            data.to_parquet(file_path , index = False)

            with open(file_path , 'rb') as main_file:

                path_to_store = directory+ f"/{name}/{name_timestamp}.parquet"
                path_of_directory = os.path.dirname(path_to_store)

                if not os.path.exists(path_of_directory):
                    os.makedirs(path_of_directory)

                    with open(path_to_store , 'wb') as fp:
                        fp.write(main_file.read())
                else:
                    with open(path_to_store , 'wb') as fp:
                        fp.write(main_file.read())


 



if __name__ == "__main__":
    data = read_load_data()



