from datetime import datetime, timedelta
import os
import random
from time import time 
from time import sleep
import uuid 
from confluent_kafka import SerializingProducer
import simplejson as json 


London_Coordinates = {"Latitude": 51.5074, "Longitude": -0.1278}
Birmingham_Coordinates = {"Latitude": 52.4862, "Longitude": -1.8904}
Latitude_increament = (Birmingham_Coordinates['Latitude'] - London_Coordinates['Latitude']) / 100
Longitude_increament = (Birmingham_Coordinates['Longitude'] - London_Coordinates['Longitude']) / 100

#Environment variables for the configuration 
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_topic')

random.seed(42)
start_time = datetime.now()
start_location = London_Coordinates.copy()

def simulate_vehicle_movement():
    global start_location
    start_location['Latitude'] += Latitude_increament
    start_location['Longitude'] += Longitude_increament
    start_location['Latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['Longitude'] += random.uniform(-0.0005, 0.0005)
    return start_location

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['Latitude'], location['Longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuel-type': 'Hybrid'
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'incident': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'incident_id': uuid.uuid4(),
        'status': random.choice(['Active', 'Resolved'])
    }

def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weather_condition': random.choice(['Sunny', 'Winter', 'Rain', 'Snowing']),
        'precipitation': random.uniform(0, 25),
        'windspeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'airQualityIndex': random.uniform(0, 100)
    }

def generate_gps_data(device_id, timestamp):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North_East',
        'vehicleType': 'private'
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

def json_serialize(obj):
    if isinstance(obj, uuid.UUID):
          return str(obj)
    return json.dumps(obj, default=str).encode('utf-8')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serialize),
        on_delivery=delivery_report
    )
    producer.flush()

def simulate_journey(producer, device_id):
     while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], 'Nikon-Camera123')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        if (vehicle_data['location'][0] >= Birmingham_Coordinates['Latitude']
                and vehicle_data['location'][1] <= Birmingham_Coordinates['Longitude']):
            print('Reached destination, simulation ending ...')
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        
        sleep(5)

if __name__ == '__main__':
     
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'security.protocol': 'PLAINTEXT',
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Preetika')
    except KeyboardInterrupt:
        print("Simulation ended by the user ")
    except Exception as e:
        print(f'Unexpected error occurred: {e}')
    

