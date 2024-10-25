import psycopg2
from kafka import KafkaConsumer
import json
from sqlalchemy import create_engine
import pandas as pd
import logging
import io


logging.basicConfig(filename = '/home/kali/Desktop/ETL/scripts/logs/load.log' ,level = logging.DEBUG , format = '%(asctime)s - %(levelname)s - %(message)s' )
logging = logging.getLogger()


def load_api_data():
                # Connect to PostgreSQL

    
    conn = psycopg2.connect("dbname=cropdb user=postgres password=mypassword host=localhost port=6000")
    cur = conn.cursor()

    try:
        transformed_data_consumer = KafkaConsumer(
            'transformed_csv_dict',
            bootstrap_servers = ['localhost:9092'],
            auto_offset_reset = 'latest',
            enable_auto_commit = 'True',
            group_id = None,
            value_deserializer = lambda x : json.loads(x.decode('utf-8') )     )
    except Exception as e:
        logging.error("error has occured in the creating the kafka_consumer_in load_data2",e)
    try:
        for message in transformed_data_consumer:
            data = message.value
            df = pd.DataFrame(data)
            
            df.rename(columns={
            'State_Name': 'state_name',
            'District_Name': 'district_name',
            'Crop_Year': 'crop_year',
            'Season': 'season',
            'Crop': 'crop',
            'Area': 'area',
            'Production': 'production'
            }           , inplace=True)

            df = df['production'].replace('', None, inplace=True)

            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            # Use COPY command to bulk-insert data
            cur.copy_from(buffer, 'crops', sep=',', columns=df.columns)

            logging.info("Data From CSV iS loaded into the Database")
    except Exception as e:
        logging.error("error has occured in loading the data in load_data1",e)
        

if __name__ == "__main__": 
    load_api_data()
