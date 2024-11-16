from kafka import KafkaProducer
import pandas as pd
import json
import os
import logging
import time


#Importamos las feutures del modelo
df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../data/processed/happiness_dataset.csv'))

#Eliminamos la columna a predecirt
df = df.drop(columns=['happiness_score'])

# Create a producer object
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: str(v).encode('utf-8'))

for index, row in df.iterrows():
    feature_to_dict = dict(row)
    data = json.dumps(feature_to_dict)
    producer.send('predict-happiness', value=data)
    logging.info(f"Message sent {data}")
    time.sleep(1)
    
print("Mensajes envados para hacer prediccion")

