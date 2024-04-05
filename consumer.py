
import json
from confluent_kafka import Consumer
from config import read_config
from database_conn import DatabaseClass

def get_payload(msg):
    _msg= json.loads(msg)
    
    _id = _msg["idSensor"]
    _time = _msg["timestamp"]
    _poluentType= _msg["tipoPoluente"]
    _nivel= _msg["nivel"]

    _formated= f"{_id}: {_nivel}/Poluente {_poluentType} - {_time}"
    return _id, _time,_poluentType, _nivel, _formated

def save(_id, _time,_poluentType, _nivel): 
    conn= DatabaseClass()
    
    conn.insert_data(_id, _time,_poluentType, _nivel)
    print("saved")

def main():
  config = read_config()
  topic = "qualidadeAr"
  
  consumer = Consumer(config)
  consumer.subscribe([topic])

  try:
    while True:
      msg = consumer.poll(1.0)
      if msg is not None and msg.error() is None:
        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")
        
        _id, _time,_poluentType, _nivel,_formated = get_payload(msg.value().decode("utf-8"))
        
        print(_formated)
        save(_id, _poluentType, _nivel, _time)
        
        print(f"Consumed message from topic {topic}: key = {key:12} value = {value:12}")
  except KeyboardInterrupt:
    pass
  finally:
    consumer.close()


main()
