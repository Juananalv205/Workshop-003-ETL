# Import the necessary libraries
import pandas as pd
import json
import joblib
import logging
import psycopg2
from kafka import KafkaConsumer
from joblib import load
import os
from dotenv import load_dotenv  # Importamos para cargar el archivo .env

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Cargar el modelo entrenado
model_path = os.path.join(os.path.dirname(__file__), '../models/svr_model_cross_val.pkl')
model = load(model_path)
print("Modelo cargado exitosamente.")

# Cargar el escalador entrenado
scaler_path = os.path.join(os.path.dirname(__file__), '../models/scaler.pkl')
scaler = load(scaler_path)
print("Escalador cargado exitosamente.")

# Conexión a la base de datos PostgreSQL utilizando las variables de entorno
try:
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),       # Nombre de tu base de datos
        user=os.getenv('DB_USER'),         # Usuario de la base de datos
        password=os.getenv('DB_PASSWORD'), # Contraseña de la base de datos
        host=os.getenv('DB_HOST'),         # Dirección del servidor PostgreSQL (localhost si está local)
        port=os.getenv('DB_PORT')          # Puerto (por defecto es 5432)
    )
    cursor = conn.cursor()
    print("Conexión a la base de datos establecida.")
except Exception as e:
    print(f"Error al conectar a la base de datos: {e}")
    exit()

# Configuración del consumidor de Kafka
consumer = KafkaConsumer('predict-happiness', bootstrap_servers='localhost:9092',
                        value_deserializer=lambda m: m.decode('utf-8'),
                        consumer_timeout_ms=5000,
                        auto_offset_reset='earliest',
                        enable_auto_commit=True)

print("Esperando mensajes de Kafka...")

for message in consumer:
    # Extraer contenido del mensaje
    data = message.value

    # Convertir el mensaje en un DataFrame
    df = pd.DataFrame([data])

    # Seleccionar las columnas necesarias para el modelo en el orden correcto
    feature_order = [
        'economy_GDP_per_capita',
        'family',
        'health_life_expectancy',
        'freedom',
        'government_corruption',
        'generosity',
        'year'
    ]
    
    try:
        # Seleccionar y reordenar las columnas necesarias
        df_features = df[feature_order]

        # Escalar las características utilizando el mismo escalador
        X_scaled = scaler.transform(df_features.values)
        
        # Realizar predicción
        predicted_happiness = model.predict(X_scaled)
        
        # Añadir la predicción como una nueva columna
        df['happiness_score'] = predicted_happiness
        print(f"Predicción realizada: {predicted_happiness}")
        
        # Insertar los resultados en la base de datos PostgreSQL
        for index, row in df.iterrows():
            # Definir la consulta SQL para insertar los datos
            insert_query = """
            INSERT INTO happiness_predictions (happiness_rank, country, region, 
                                                economy_GDP_per_capita, family, 
                                                health_life_expectancy, freedom, 
                                                government_corruption, generosity, 
                                                year, happiness_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            
            # Ejecutar la inserción de los datos en la base de datos
            cursor.execute(insert_query, (
                row['happiness_rank'],
                row['country'],
                row['region'],
                row['economy_GDP_per_capita'],
                row['family'],
                row['health_life_expectancy'],
                row['freedom'],
                row['government_corruption'],
                row['generosity'],
                row['year'],
                row['happiness_score']
            ))

        # Confirmar los cambios en la base de datos
        conn.commit()
        print("Datos insertados correctamente en la base de datos.")

    except Exception as e:
        logging.error(f"Error {e}")
        
    except KeyError as e:
        logging.error(f"Error procesando mensaje. Faltan columnas necesarias: {e}")

# Cerrar la conexión cuando ya no sea necesario
print("Fin del consumo de mensajes.")
consumer.close()
cursor.close()
conn.close()
