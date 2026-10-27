# Análisis de Datos en Tiempo Real con Apache Kafka y Spark Streaming

### El desarrollo e implementación del pipeline de datos en tiempo real se estructuró bajo una arquitectura desacoplada de productor-consumidor, garantizando la ingesta, transporte y procesamiento concurrente de eventos generados por dispositivos IoT simulados.

###  Aprovisionamiento y Despliegue del Entorno:
* El sistema opera sobre un entorno virtualizado basado en Ubuntu Linux (vboxuser). Para el almacenamiento y distribución de mensajes en streaming, se configuró e instaló Apache Kafka (versión 3.6.2) en conjunto con su gestor de coordinación ZooKeeper.

* Se estableció un canal de comunicación centralizado mediante la creación de un tópico específico denominado sensor_data, configurado con una partición y un factor de replicación unitario para asegurar el direccionamiento de los eventos.

### Simulación y Emisión de Flujos (Productor):
* Mediante la ejecución del script en Python kafka_producer.py, se emula de forma periódica una red de sensores distribuidos.   

* Dicho script genera estructuras de datos en formato JSON que integran un identificador único de sensor (sensor_id), métricas estocásticas de temperatura y humedad, y una marca temporal (timestamp), transmitiendo los registros de forma continua hacia el broker de Kafka.

###Procesamiento Distribuido y Ventanas Temporales (Consumidor):
* La lectura y análisis del flujo se realiza a través de un consumidor desarrollado en PySpark (spark_streaming_consumer.py), haciendo uso del conector de SQL para Kafka (spark-sql-kafka).   

* El motor de procesamiento deserializa los mensajes binarios y aplica un esquema tipado estricto. Asimismo, implementa una agregación basada en ventanas de tiempo deslizantes de 1 minuto (window), lo que permite calcular de manera dinámica el promedio de temperatura y humedad por cada sensor, arrojando los resultados consolidados por lotes directamente en la terminal de ejecución.

### Validación y Monitoreo del Rendimiento:

* El comportamiento operativo y la estabilidad del clúster de procesamiento se auditaron en tiempo de ejecución a través de la interfaz web de diagnóstico de Spark (puerto 4040). Esto permitió certificar la correcta distribución de las tareas entre los ejecutores y la ejecución eficiente de las etapas del flujo de streaming sin saturación de memoria.
