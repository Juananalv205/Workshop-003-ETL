# Import the necessary libraries
import pandas as pd
import json
import joblib
import logging
import psycopg2
from kafka import KafkaConsumer
from joblib import load
import os
from dotenv import load_dotenv  # To load environment variables from a .env file

# Load environment variables from the .env file
load_dotenv()

# Load the trained model
model_path = os.path.join(os.path.dirname(__file__), '../models/svr_model_cross_val.pkl')
model = load(model_path)
print("Model loaded successfully.")

# Load the trained scaler
scaler_path = os.path.join(os.path.dirname(__file__), '../models/scaler.pkl')
scaler = load(scaler_path)
print("Scaler loaded successfully.")

# Connect to the PostgreSQL database using environment variables
try:
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),       # Database name
        user=os.getenv('DB_USER'),         # Database user
        password=os.getenv('DB_PASSWORD'), # Database password
        host=os.getenv('DB_HOST'),         # PostgreSQL server address (localhost if local)
        port=5432                          # Port (default is 5432)
    )
    cursor = conn.cursor()
    print("Database connection established.")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit()

# Configure the Kafka consumer
consumer = KafkaConsumer('predict-happiness', bootstrap_servers='localhost:9092',
                         value_deserializer=lambda m: m.decode('utf-8'),
                         consumer_timeout_ms=5000,
                         auto_offset_reset='earliest',
                         enable_auto_commit=True)

print("Waiting for messages from Kafka...")

for message in consumer:
    # Extract and parse the received message from Kafka
    data = message.value
    print(f"Message received: {data}")
    
    try:
        # Parse the JSON message into a dictionary
        parsed_data = json.loads(data)  # Convert the JSON to a dictionary
        
        # Create a DataFrame from the dictionary
        df = pd.DataFrame([parsed_data])

        # Check that all required columns are present
        feature_order = [
            'economy_GDP_per_capita',
            'family',
            'health_life_expectancy',
            'freedom',
            'government_corruption',
            'generosity',
            'year'
        ]
        missing_columns = [col for col in feature_order if col not in df.columns]
        if missing_columns:
            logging.error(f"Missing required columns in the message: {missing_columns}")
            continue  # Skip this message if columns are missing
        
        # Select and scale the features
        df_features = df[feature_order]
        X_scaled = scaler.transform(df_features.values)
        
        # Make the prediction
        predicted_happiness = model.predict(X_scaled)
        df['happiness_score'] = predicted_happiness

        # Insert the results into the PostgreSQL database
        for index, row in df.iterrows():
            # SQL query to insert the data
            insert_query = """
            INSERT INTO happiness_data (happiness_rank, country, region, 
                                                economy_GDP_per_capita, family, 
                                                health_life_expectancy, freedom, 
                                                government_corruption, generosity, 
                                                year, happiness_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            
            # Execute the query to insert the data
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

        # Commit the changes to the database
        conn.commit()
        print("Data successfully inserted into the database.")

    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode the JSON: {e}")
    except KeyError as e:
        logging.error(f"Missing required keys in the message: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

# Close the connections
print("Message consumption finished.")
consumer.close()
cursor.close()
conn.close()
