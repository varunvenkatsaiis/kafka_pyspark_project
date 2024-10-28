END-TO-END Data Pipline using Kafka, Pyspark, Parquet, File Storage, PostgresDB, Grafana for efficient data ingestion, transformation, storage, and visualization.

Data Ingestion: Loaded csv data from filesystem to PostgresDB for incremental data loading 

Data Conversion: Converted the data from postgresDB to parquet file format for effecient storage of data and partitioned using a column for faster query time and less processing of data

Data Transformation: Analyzed and transformed the partitioned data using pyspark for fast processing and in distributed manner for effecient use of resources

Orchestration: Airflow for orchestrating the scripts and scheduling them according to the data loading interval

Visualization: Connected data to Grafana for real-time monitoring and visualization of metrics and insights


![Blank diagram](https://github.com/user-attachments/assets/e070eac1-12b8-4014-b866-9011131aadc1)
