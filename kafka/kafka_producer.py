from kafka import KafkaProducer
import pandas as pd
import json
import os
import logging
import time

# Import the features from the model
df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../data/processed/happiness_dataset.csv'))

# Drop the column to be predicted
df = df.drop(columns=['happiness_score'])

# Create a producer object
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                        value_serializer=lambda v: str(v).encode('utf-8'))

for index, row in df.iterrows():
    feature_to_dict = dict(row)  # Convert the row into a dictionary
    data = json.dumps(feature_to_dict)  # Convert the dictionary to a JSON string
    producer.send('predict-happiness', value=data)  # Send the message to Kafka
    logging.info(f"Message sent {data}")  # Log the message sent
    time.sleep(1)  # Sleep for 1 second before sending the next message
    
print("Messages sent for prediction")  # Print when all messages are sent


