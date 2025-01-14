import time
from concurrent import futures

import grpc
import location_pb2
import location_pb2_grpc
import json

from kafka import KafkaProducer

KAFKA_TOPIC = "location"
KAFKA_BROKER_URL = "kafka-service:9092"

producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER_URL])

class LocationServicer(location_pb2_grpc.LocationServiceServicer):
    def Create(self, request, context):
        print("received")
        location = {
            "person_id": request.person_id,
            "latitude": request.latitude,
            "longitude": request.longitude,
            "creation_time": request.creation_time
        }
        print("location: ", location)
        producer.send(KAFKA_TOPIC, json.dumps(location).encode('utf-8'))
        producer.flush()
        return location_pb2.LocationMessage(**location)

# Initialize gRPC server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
location_pb2_grpc.add_LocationServiceServicer_to_server(LocationServicer(), server)


print("Server (version 1) starting on port 5005...")
server.add_insecure_port("[::]:5005")
server.start()
# Keep thread alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)
