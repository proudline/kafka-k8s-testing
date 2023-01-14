from kafka import KafkaConsumer
import json
import time
import datetime

from secret import BOOTSTRAP_SERVER
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

flights_cols = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("time_position", StringType(), True),
    StructField("last_contact", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("baro_altitude", StringType(), True),
    StructField("on_ground", StringType(), True),
    StructField("velocity", StringType(), True),
    StructField("true_track", StringType(), True),
    StructField("vertical_rate", StringType(), True),
    StructField("sensors", StringType(), True),
    StructField("geo_altitude", StringType(), True),
    StructField("squawk", StringType(), True),
    StructField("spi", StringType(), True),
    StructField("position_source", StringType(), True)
])

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("opensky tracker") \
        .config("spark.jars", "./gcs-connector-hadoop3-latest.jar") \
        .getOrCreate()

    # sc = SparkContext("local", "First App")

    consumer = KafkaConsumer("opensky", bootstrap_servers=[BOOTSTRAP_SERVER])

    print("\nWaiting for message!")
    for message in consumer:
        print("recieved message")

        message = json.loads(message.value.decode("utf-8"))
        begin = message["time"]
        flights = message["states"]

        start_date = datetime.datetime.fromtimestamp(begin)
        df = spark.createDataFrame(data=flights, schema=flights_cols)

        # df.printSchema()
        # df.show(10)

        # origins = df.rdd 
        #                 .map(lambda x: x["origin_country"]) \
        #                 .mapValues(lambda x: (x, 1)) \
        #                 .reduceByKey(lambda x, y: x+y) \
        #                 .take(10)

        nb = df.groupBy("origin_country") \
            .agg(F.count("icao24").alias("count")) \
            .sort(F.desc("count")) \
            .show(7)

        records = open("states.txt", "a+")
        records.write("There is {} ongoing flights as of {}\n".format(len(flights), start_date))
        records.close()

        
    
