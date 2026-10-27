from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, max, min

# 1. Inicializar la sesión de Spark para Batch
spark = SparkSession.builder \
    .appName("TRMBatchProcessingEDA") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar el conjunto de datos desde HDFS (apuntando a tu archivo recién subido)
ruta_hdfs = "hdfs://localhost:9000/Tarea3/Tasa_de_Cambio.csv"
print(f"--- Cargando dataset desde HDFS: {ruta_hdfs} ---")

df_trm = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(ruta_hdfs)

# Mostrar la estructura original (esquema)
print("--- Esquema Original del Dataset ---")
df_trm.printSchema()

# 3. Limpieza y Transformación (EDA)
# Eliminamos registros nulos y seleccionamos/renombramos columnas clave si es necesario
df_limpio = df_trm.dropna()

print(f"Total de registros válidos tras la limpieza: {df_limpio.count()}")

# Mostrar una vista previa de los datos limpios
print("--- Primeros 5 Registros del Dataset Limpio ---")
df_limpio.show(5, truncate=False)

# 4. Análisis Exploratorio de Datos (EDA) y Estadísticas Descriptivas
print("--- Resumen Estadístico de los Valores de la TRM ---")
df_limpio.describe("valor").show()

# 5. Operaciones analíticas adicionales (Top de cotizaciones más altas)
print("--- Top 5 Cotizaciones Históricas Más Altas ---")
df_limpio.orderBy(desc("valor")).show(5, truncate=False)

# 6. Cálculo de métricas globales (Visualización de resultados finales)
print("--- Métricas Globales del Mercado de Divisas (TRM) ---")
df_metricas = df_limpio.agg(
    max("valor").alias("TRM_Maxima"),
    min("valor").alias("TRM_Minima"),
    avg("valor").alias("TRM_Promedio")
)
df_metricas.show(truncate=False)

# Finalizar la sesión de Spark
spark.stop()
