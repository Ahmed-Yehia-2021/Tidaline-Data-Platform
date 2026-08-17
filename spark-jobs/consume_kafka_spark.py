from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("TidalineStream").getOrCreate()

df = (
    spark.readStream
    .format("Kafka")
    .option("kafka.bootstrap.servers","kafka:29092")
    .option("subsrcibe","maritime_server.public.earthquakes")
    .option("startingOffsets", "earliest")
    .load()
)