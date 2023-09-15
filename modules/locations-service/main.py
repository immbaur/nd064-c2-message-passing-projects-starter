import logging
import sys
import json
import os
import time
from kafka import KafkaConsumer

import psycopg2

# from app.udaconnect.services import LocationService

# Define Kafka consumer function

KAFKA_TOPIC = "location"
KAFKA_BROKER_URL = "kafka-service:9092"

# Configure logging to stdout
logger = logging.getLogger("my-logger")
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a StreamHandler to log to stdout
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

# Add the StreamHandler to the logger
logger.addHandler(stream_handler)

DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_USER = os.environ["DB_USERNAME"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]
DB_NAME = os.environ["DB_NAME"]

def add_location(location):
    logger.info(f"Adding location: {location}")
    # logger.info(f"DB_USER: {DB_USER}")
    # logger.info(f"DB_HOST: {DB_HOST}")
    # logger.info(f"DB_PORT: {DB_PORT}")
    # logger.info(f"DB_PASSWORD: {DB_PASSWORD}")
    # logger.info(f"DB_NAME: {DB_NAME}")
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        cur = conn.cursor()
        cur.execute("INSERT INTO location (person_id, creation_time, coordinate) VALUES (%s, %s, ST_Point(%s, %s))", (location["person_id"], location["creation_time"], location["latitude"], location["longitude"]))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


def consume_topic():
    
    consumer = KafkaConsumer(KAFKA_TOPIC, group_id='udaconnect', bootstrap_servers=KAFKA_BROKER_URL)
    for message in consumer:
        logger.info(message)
        try:
            value = message.value.decode('utf-8')
            logger.info(value)
            mess = json.loads(value)
            logger.info(mess)
            add_location(mess)

            #LocationService.create(mess)
            # Your code to work with 'mess' goes here
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error: {e}")
        except UnicodeDecodeError as e:
            logger.error(f"Unicode decoding error: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    logger.info("Starting the Kafka consumer service")
    location = {
        "person_id": 5,
        "creation_time": "2021-04-01T12:00:00Z",
        "latitude": 123,
        "longitude": 123
    }
    add_location(location)


    while True:
        consume_topic()
        time.sleep(1/100)


# location = {
#     "person_id": 3,
#     "latitude": 123,
#     "longitude": 123
# }

# ser = json.dumps(location).encode('utf-8')

# mess = json.loads(ser.decode('utf-8'))
# print(mess)
# LocationService.create(mess)
