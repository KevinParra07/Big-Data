from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, LongType

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
    StructField("marca_de_tiempo", LongType())
])

# Configurar el lector de streaming para leer desde Kafka
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trm_stream") \
    .load()

# Parsear el valor JSON recibido de Kafka
df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), esquema_trm).alias("data")) \
    .select("data.*")

# Mostrar el resultado del streaming en la consola
query = df_json.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
