import logging
import sys
import json
import time
import psycopg2
import os

# from app.udaconnect.services import LocationService

# Define Kafka consumer function

# Configure logging to stdout
logger = logging.getLogger("my-logger")
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Create a StreamHandler to log to stdout
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

# Add the StreamHandler to the logger
logger.addHandler(stream_handler)

def add_location(location):
    logger.info(f"Adding location: {location}")
    DB_PASSWORD = os.environ["DB_PASSWORD"]
    logger.info(f"DB_PASSWORD: {DB_PASSWORD}")    
    try:
        conn = psycopg2.connect(
            host="postgres",
            port="5432",
            user="ct_admin",
            password=DB_PASSWORD,
            dbname="geoconnections"
        )
        cur = conn.cursor()
        cur.execute("INSERT INTO location (person_id, creation_time, coordinate) VALUES (%s, %s, ST_Point(%s, %s))", (location["person_id"], location["creation_time"], location["latitude"], location["longitude"]))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")



if __name__ == '__main__':
    logger.info("Starting the Kafka consumer service")
    location = {
        "person_id": 333,
        "creation_time": "2021-04-01T12:00:00Z",
        "latitude": 123,
        "longitude": 123
    }
    while True:
        add_location(location)
        time.sleep(2)
    logger.info("Kafka consumer service has stopped")