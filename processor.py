import json
from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config


my_id = kafka_config['my_id']

consumer = KafkaConsumer(
    f"{my_id}_building_sensors",
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id=f'{my_id}_main_processor'
)

alert_producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Процесор запущено...")

for message in consumer:
    val = message.value
    if val['temperature'] > 40:
        alert = {**val, "message": "Зависока температура!"}
        alert_producer.send(f"{my_id}_temperature_alerts", value=alert)
    
    if val['humidity'] > 80 or val['humidity'] < 20:
        alert = {**val, "message": "Аномальна вологість!"}
        alert_producer.send(f"{my_id}_humidity_alerts", value=alert)
    
    alert_producer.flush()