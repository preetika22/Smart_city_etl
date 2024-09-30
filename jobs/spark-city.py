from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming")\
        .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1,"  
            "org.apache.hadoop:hadoop-aws:3.3.1,"  
            "com.amazonaws:aws-java-sdk:1.11.469")\
        .config("spark.hadoop.fs.s3a.access.key", "")\
        .config("spark.hadoop.fs.s3a.secret.key", "")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
        .getOrCreate()

        # .config('spark.hadoop.fs.s3a.aws.credentials.provider', "org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentialsProvider")\
       

    # Adjust the log level 
    spark.sparkContext.setLogLevel('WARN')

    # Define schemas
    vehicle_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
            .format('kafka')
            .option("kafka.bootstrap.servers", "broker:29092")
            .option('subscribe', topic)
            .option('startingOffsets', 'earliest')
            .load()
            .selectExpr('CAST(value AS STRING)')
            .select(from_json(col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('timestamp', '2 minutes'))
    

    def streamWriter(input_df, checkpoint_location, output_path):
        return (input_df.writeStream
            .format('parquet')
            .option('checkpointLocation', checkpoint_location)
            .option('path', output_path)
            .outputMode('append')
            .start()
            )
        

    vehicleDF = read_kafka_topic('vehicle_data', vehicle_schema)
    gpsDF = read_kafka_topic('gps_data', gpsSchema)
    trafficDF = read_kafka_topic('traffic_data', trafficSchema)
    weatherDF = read_kafka_topic('weather_data', weatherSchema)
    emergencyDF = read_kafka_topic('emergency_topic', emergencySchema)

#     emergency_topic
# gps_data
# traffic_data
# vehicle_data
# weather_data

    query1 = streamWriter(vehicleDF, 's3a://project-smart-city/checkpoints/vehicle',
                       's3a://project-smart-city/data/vehicle_data')
    query2 = streamWriter(gpsDF, 's3a://project-smart-city/checkpoints/gps_data',
                      's3a://project-smart-city/data/gps_data')
    query3 = streamWriter(trafficDF, 's3a://project-smart-city/checkpoints/traffic_data',
                      's3a://project-smart-city/data/traffic_data')
    query4 = streamWriter(weatherDF, 's3a://project-smart-city/checkpoints/weather_data',
                      's3a://project-smart-city/data/weather_data')
    query5 = streamWriter(emergencyDF, 's3a://project-smart-city/checkpoints/emergency_data',
                      's3a://project-smart-city/data/emergency_data')
    query5.awaitTermination()


if __name__ == '__main__':
    main()

