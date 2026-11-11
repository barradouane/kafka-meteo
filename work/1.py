"""Exercice 1 - Création du topic (si besoin) + envoi d'un message statique."""
import json
import os

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "weather_stream"


def ensure_topics(topics=("weather_stream", "weather_transformed"), partitions=3):
    """Crée les topics s'ils n'existent pas (sans erreur s'ils existent déjà)."""
    admin = KafkaAdminClient(bootstrap_servers=[BOOTSTRAP])
    for name in topics:
        try:
            admin.create_topics([NewTopic(name=name, num_partitions=partitions, replication_factor=1)])
            print(f"Topic créé : {name}")
        except TopicAlreadyExistsError:
            print(f"Topic déjà présent : {name}")
    admin.close()


def send_hello():
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    producer.send(TOPIC, {"msg": "Hello Kafka"})
    producer.flush()
    producer.close()
    print(f"Message {{'msg': 'Hello Kafka'}} envoyé dans '{TOPIC}'")


if __name__ == "__main__":
    ensure_topics()
    send_hello()