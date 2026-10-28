import time
import json
import random
from kafka import KafkaProducer

def generar_trm_en_vivo():
    return {
        "id_registro": random.randint(100, 999),
        "trm_valor": round(random.uniform(4000.0, 4500.0), 2),
        "moneda": "USD/COP",
        "marca_de_tiempo": int(time.time())
    }

productor = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    datos_trm = generar_trm_en_vivo()
    productor.send('trm_stream', value=datos_trm)
    print(f"Enviado TRM: {datos_trm}")
    time.sleep(1)
