from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType
import logging

# Configura el nivel de log a WARN para reducir los mensajes INFO
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los datos de entrada
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", TimestampType())
])

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("SensorDataAnalysis") \
    .getOrCreate()

# Configurar el lector de streaming para leer desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Parsear los datos JSON recibidos
df_parsed = df.selectExpr("CAST(string AS STRING)") \
    .select(from_json(col("string"), schema).alias("data")) \
    .select("data.*")

# Escribir la salida en la consola en tiempo real
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
