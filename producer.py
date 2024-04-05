from config import read_config
from confluent_kafka import Producer
import time

def main():  
  data = [
        '{ "idSensor": "sensor_001", "timestamp": "2024-04-04T12:34:56Z", "tipoPoluente": "PM2.5","nivel": 35.2}',
        '{ "idSensor": "sensor_002", "timestamp": "2024-04-04T12:34:56Z", "tipoPoluente": "PM2.5","nivel": 35.2}',
        '{ "idSensor": "sensor_003", "timestamp": "2024-04-04T12:34:56Z", "tipoPoluente": "PM2.5","nivel": 35.2}',
        '{ "idSensor": "sensor_004", "timestamp": "2024-04-04T12:34:56Z", "tipoPoluente": "PM2.5","nivel": 35.2}',
        '{ "idSensor": "sensor_005", "timestamp": "2024-04-04T12:34:56Z", "tipoPoluente": "PM2.5","nivel": 35.2}',
        '{ "idSensor": "sensor_006", "timestamp": "2024-04-04T12:34:56Z", "tipoPoluente": "PM2.5","nivel": 35.2}',
        '{ "idSensor": "sensor_007", "timestamp": "2024-04-04T12:34:56Z", "tipoPoluente": "PM2.5","nivel": 35.2}'
        ]
  
  config = read_config()
  topic = "qualidadeAr"

  producer = Producer(config)
  
  for info in data:
        key = "sensor_data"
        producer.produce(topic, key=key, value=info)
        print(f"Produced message to topic {topic}: key = {key:12} value = {info:12}")
        time.sleep(3)

  producer.flush()

main()