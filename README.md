# Análisis de la Tasa de Cambio (TRM) en Tiempo Real con Apache Kafka y Spark Streaming

**Curso:** Big Data 
**Universidad:** Universidad Nacional Abierta y a Distancia (UNAD)
**Actividad:** Proyecto Final - Procesamiento Batch y en Tiempo Real

## Definición del Problema y Conjunto de Datos

* Conjunto de Datos: Tasa de Cambio Representativa del Mercado (TRM) Histórico, obtenido del portal oficial de Datos Abiertos de Colombia (datos.gov.co) y suministrado por la Superintendencia Financiera de Colombia.   

* Objetivo Batch: Cargar, limpiar y realizar análisis exploratorio (EDA) de los registros históricos del dólar en Colombia para identificar tendencias y valores máximos o mínimos.
* Objetivo Streaming: Simular el flujo de cotizaciones del dólar en tiempo real mediante un generador y procesarlas con Spark Structured Streaming para calcular promedios móviles instantáneos.

## ⚙️ Prerrequisitos e Instalación 

### El entorno de trabajo opera sobre una máquina virtual con Ubuntu Linux (vboxuser).   

## 1. Instalación del cliente de Python para Kafka
pip install kafka-python

## 2. Descarga y configuración de Apache Kafka (v3.6.2)
wget [https://downloads.apache.org/kafka/3.6.2/kafka_2.13-3.6.2.tgz](https://downloads.apache.org/kafka/3.6.2/kafka_2.13-3.6.2.tgz)
tar -xzf kafka_2.13-3.6.2.tgz
sudo mv kafka_2.13-3.6.2 /opt/Kafka

## Guía de Ejecución Paso a Paso
## Paso 1: Iniciar los Servicios (ZooKeeper y Servidor Kafka)
### Ejecutar en la consola de PuTTY:

### Iniciar ZooKeeper en segundo plano
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &

### Iniciar el servidor de Apache Kafka en segundo plano
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &

### (Presiona Enter cuando finalice la carga de los logs en la terminal para que aparezca el prompt).

## Paso 2: Crear el Tópico en Kafka

/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic trm_stream

## Paso 3: Iniciar el Productor de Datos 
python3 kafka_producer.py

## Paso 4: Iniciar el Consumidor de Spark Streaming (Terminal 2)
### Abre una nueva ventana en PuTTY y ejecuta el comando de procesamiento distribuido:   
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_c

## Monitoreo y Resultados
Las evidencias fotográficas de la ejecución del productor, las tablas de resultados por lotes (Batches) y el análisis de rendimiento en la interfaz web de Spark UI (puerto 4040) se encuentran guardadas en la carpeta evidencias/.
