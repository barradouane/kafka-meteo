"""Exercice 2 - Consommateur Kafka : affiche les messages en temps réel."""
import json
import os

from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "weather_stream"  # <-- topic à lire (variable définie au début du script)


def _decode(raw):
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return raw.decode("utf-8", errors="replace")


def run_consumer(topic=TOPIC, max_messages=None, idle_timeout_s=None, from_beginning=True):
    """Lit `topic` et affiche chaque message.
    - idle_timeout_s : s'arrête après N secondes sans message (None = tourne jusqu'à Ctrl+C)
    - max_messages   : s'arrête après N messages (None = illimité)
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[BOOTSTRAP],
        auto_offset_reset="earliest" if from_beginning else "latest",
        enable_auto_commit=False,
        value_deserializer=_decode,
        consumer_timeout_ms=int(idle_timeout_s * 1000) if idle_timeout_s else float("inf"),
    )
    print(f"Lecture du topic '{topic}' sur {BOOTSTRAP} ... (Ctrl+C pour arrêter)")
    n = 0
    try:
        for message in consumer:
            n += 1
            print(f"[p{message.partition} o{message.offset}] {message.value}")
            if max_messages and n >= max_messages:
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    print(f"Fin de la lecture ({n} message(s) affiché(s)).")


if __name__ == "__main__":
    run_consumer()