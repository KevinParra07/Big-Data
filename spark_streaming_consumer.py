from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType
import logging

# Configurar sesión de Spark para Streaming
spark = SparkSession.builder \
    .appName("TRMStreamingAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los datos de la TRM en tiempo real
esquema_trm = StructType([
    StructField("id_registro", IntegerType()),
    StructField("trm_valor", FloatType()),
    StructField("moneda", StringType()),
    StructField("timestamp", TimestampType())
])

# Configurar el lector de streaming para leer desde Kafka
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trm_stream") \
    .load()

# Parsear el contenido JSON recibido
df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), esquema_trm).alias("data")
).select("data.*")

# Calcular estadísticas de la TRM por ventana de tiempo de 1 minuto
estadisticas_trm = df_parsed \
    .groupBy(window(col("timestamp"), "1 minute"), col("moneda")) \
    .agg({"trm_valor": "avg"})

# Escribir los resultados consolidados en la consola
consulta = estadisticas_trm \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

consulta.awaitTermination()
