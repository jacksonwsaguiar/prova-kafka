from confluent_kafka import Producer,Consumer
from config import read_config
from database_conn import DatabaseClass
from consumer import get_payload, save

received_messages = []

def test_data_persistency():
    print("Persistence data test")
    
    conn = DatabaseClass()
    data_expected = "(27, 'sensor_199', 'PM2.5', '35.2', '2024-04-04T12:34:56Z')"
    data = [
        '{ "idSensor": "sensor_199", "timestamp": "2024-04-04T12:34:56Z", "tipoPoluente": "PM2.5","nivel": 35.2}',
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
    consumer = Consumer(config)
    consumer.subscribe([topic])

    msg = consumer.poll(1.0)
    if msg is not None and msg.error() is None:
        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")
        received_messages.append(value)
        _id, _time,_poluentType, _nivel,_formated = get_payload(value)
            
        # print(_formated)
        save(_id, _poluentType, _nivel, _time)
            
        # print(f"Consumed message from topic {topic}: key = {key:12} value = {value:12}")

    time.sleep(10)
    consumer.close()
    
    last = conn.last_inserted()
    assert last == data_expected, "Dados não persistidos."
    
def test_data_integration():
    print("Testing confirm data sent and data validation")
    data = [
        '{ "idSensor": "sensor_001", "timestamp": "2024-04-04T12:34:56Z", "tipoPoluente": "PM2.5","nivel": 35.2}',
        ]
    expected_value='{ "idSensor": "sensor_007", "timestamp": "2024-04-04T12:34:56Z", "tipoPoluente": "PM2.5","nivel": 35.2}'
    
    config = read_config()
    topic = "qualidadeAr"

    producer = Producer(config)
    for info in data:
        key = "sensor_data"
        producer.produce(topic, key=key, value=info)
        print(f"Produced message to topic {topic}: key = {key:12} value = {info:12}")
        time.sleep(3)

    producer.flush()
     
    consumer = Consumer(config)
    consumer.subscribe([topic])

    msg = consumer.poll(1.0)
    if msg is not None and msg.error() is None:
        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")
        received_messages.append(value)
     
    time.sleep(10)
    consumer.close()
    print("Received message:", received_messages)

    assert received_messages[0] == expected_value, "Dados inválidos."
