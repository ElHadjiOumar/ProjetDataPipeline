from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json


appName = "Kafka Examples"
master = "local"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

# Configuration de la connexion à Kafka
kafka_bootstrap_servers = "localhost:9092"
kafka_subscribe_type = "subscribe"
kafka_topics = "NiveauDeVieRecords"
df_niveau_de_vie = spark.read.format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option(kafka_subscribe_type, kafka_topics) \
  .option("startingOffsets", "earliest") \
  .option("encoding", "utf-8") \
  .load()

df_niveau_de_vie = df_niveau_de_vie.selectExpr("CAST(value AS STRING)")


print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
print(df_niveau_de_vie.head(1))