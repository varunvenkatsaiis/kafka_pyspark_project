from pyspark.sql import SparkSession
from pyspark.sql.session import *
import os
import pandas as pd
from pyspark.sql.functions import sum, col

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

jdbc_driver_path = "/home/kali/Downloads/postgresql-42.7.4.jar"


spark = SparkSession.builder \
    .appName("Transformed Partitioned Parquet") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

# Now try reading the Parquet file again
parquet_path = "/home/kali/Documents/object_storage1/"

df = pd.read_parquet(parquet_path)

spark_df = spark.createDataFrame(df)




agg_df = spark_df.groupBy("state_name", "crop_year").agg(
    sum("production").alias("total_production"),
    sum("area").alias("total_area")
)


# Step 3: Calculate yield (production / area)
yield_df = agg_df.withColumn("yield", col("total_production") / col("total_area"))

yield_df.show()


distinct_crops_df = spark_df.select("state_name", "crop").distinct()

distinct_crops_df.show()


production_trends = spark_df.groupBy("crop_year").agg(sum("production").alias("total_production"))
production_trends.show()


seasonal_production = spark_df.groupBy("season").agg(sum("production").alias("total_production"))
seasonal_production.show()

url = "jdbc:postgresql://localhost:6000/cropdb"
properties = {
    "user": "postgres",
    "password": "mypassword",
    "driver": "org.postgresql.Driver"
}

# Write the DataFrame to PostgreSQL
yield_df.write.jdbc(url, "total_production_by_crop", mode="overwrite", properties=properties)
distinct_crops_df.write.jdbc(url, "average_yield", mode="overwrite", properties=properties)
production_trends.write.jdbc(url, "production_trends", mode="overwrite", properties=properties)
seasonal_production.write.jdbc(url, "seasonal_production", mode="overwrite", properties=properties)




