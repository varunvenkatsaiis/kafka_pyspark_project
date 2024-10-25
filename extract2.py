import json
import pandas as pd
import logging
from kafka import KafkaProducer

logging.basicConfig(filename = '/home/kali/Desktop/ETL/scripts/logs/extract.log' , level = logging.DEBUG , format = '%(asctime)s - %(levelname)s - %(message)s' )
logging = logging.getLogger()

def create_kafka_producer(server):
    try:
        producer = KafkaProducer(
            bootstrap_servers = server,  #bootstraps the server to the given port
            value_serializer = lambda v: json.dumps(v).encode('utf-8')   #    serializer it converts the message to a json format and then sends it through kafka server        
        )

        return producer
    except Exception as e:
        logging.error("error has occured while creating the kafka producer",e)



def data_from_csv(prod , file_path="/home/kali/Downloads/Crop_Production_data.csv" ):
    try:
        
      
        for chunk in pd.read_csv(file_path  , chunksize=100):
            dt1 = chunk.to_dict('records')
            prod.send('transformed_csv_dict',dt1)
            logging.info("Data From CSV is Pushed to the producer_csv")
            prod.flush()

    except Exception as e:
        print("An error has occured while pushing data to the transform component",e)
        logging.error(e)


if __name__ == "__main__":
    kserver = ['localhost:9092']
    producer_csv_data = create_kafka_producer(kserver)
    data_from_csv(producer_csv_data)
     
