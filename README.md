# Proyecto Big Data - Análisis y Procesamiento de la TRM (Tasa de Cambio)

Repositorio oficial para la entrega de la Tarea 3 del curso de Big Data de la UNAD. Este proyecto implementa un pipeline completo de procesamiento de datos utilizando tecnologías de Big Data sobre un entorno distribuido en máquina virtual (Ubuntu/Hadoop).

---

## 📋 1. Descripción del Proyecto
El proyecto tiene como objetivo principal procesar y analizar el comportamiento histórico y en tiempo real de la **Tasa de Cambio Representativa del Mercado (TRM)** en Colombia. Para cumplir con los requisitos académicos, la solución se divide en dos enfoques:
1. **Procesamiento Batch (Lotes):** Carga, limpieza, transformación y Análisis Exploratorio de Datos (EDA) sobre el dataset histórico almacenado en Hadoop (HDFS).
2. **Procesamiento en Tiempo Real (Streaming):** Simulación de flujos de datos continuos mediante Apache Kafka y consumo en micro-lotes con PySpark Structured Streaming.

---

## 🛠️ 2. Arquitectura e Infraestructura Tecnológica
* **Sistema Operativo:** Ubuntu (VM / Entorno Virtualizado - `vboxuser`)
* **Almacenamiento Distribuido:** Hadoop HDFS
* **Procesamiento por Lotes (Batch) y Streaming:** Apache Spark (PySpark)
* **Mensajería en Tiempo Real:** Apache Kafka y ZooKeeper
* **Lenguaje de Programación:** Python 3

---

## 📂 3. Estructura del Repositorio
```text
/
├── Tasa_de_Cambio.csv              # Dataset histórico oficial (fuente original)
├── procesamiento_batch.py          # Script de PySpark para limpieza, transformación y EDA (Batch)
├── kafka_producer.py               # Productor de Kafka para la simulación del flujo de datos
├── spark_streaming_consumer.py     # Consumidor de PySpark Structured Streaming (Tiempo Real)
└── README.md                       # Documentación oficial del proyecto
## Monitoreo y Resultados
Las evidencias fotográficas de la ejecución del productor, las tablas de resultados por lotes (Batches) y el análisis de rendimiento en la interfaz web de Spark UI (puerto 4040) se encuentran guardadas en la carpeta evidencias/.
