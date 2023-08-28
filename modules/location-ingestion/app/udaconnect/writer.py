import grpc
import location_pb2
import location_pb2_grpc

"""
Sample implementation of a writer that can be used to write messages to gRPC.
"""

print("Sending sample payload...")

channel = grpc.insecure_channel("127.0.0.1:30005")
stub = location_pb2_grpc.LocationServiceStub(channel)

message = location_pb2.LocationMessage(person_id=1, latitude="123", longitude="456")

response = stub.Create(message)

print(response)