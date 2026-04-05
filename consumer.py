import json
from kafka import KafkaConsumer
from configs import kafka_config


my_id = kafka_config['my_id']

consumer = KafkaConsumer(
    f"{my_id}_temperature_alerts",
    f"{my_id}_humidity_alerts",
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest'
)

print("Моніторинг записів ...")
for message in consumer:
    print(f"Отримано ALERT у топіку {message.topic}: {message.value}")