"""Exercice 6 - Consommateur Kafka : weather_transformed -> /hdfs-data/"""
import json
import os
import threading
from collections import defaultdict
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "weather_transformed"
# On change le nom du groupe pour être sûr de repartir de zéro et tout relire
GROUP_ID = "hdfs-writer-group-v2"  

def resolve_base():
    candidates = [os.getenv("HDFS_BASE"), "/hdfs-data",
                  os.path.join(os.path.expanduser("~"), "work", "hdfs-data"),
                  os.path.abspath("hdfs-data")]
    for path in filter(None, candidates):
        try:
            os.makedirs(path, exist_ok=True)
            probe = os.path.join(path, ".write_test")
            with open(probe, "w") as f:
                f.write("ok")
            os.remove(probe)
            return path
        except OSError:
            continue
    raise RuntimeError("Aucun dossier inscriptible pour le stockage HDFS.")

def _safe(name):
    return str(name).replace("/", "-").replace("\\", "-").strip() or "unknown"

def alerts_path(base, country, city):
    return os.path.join(base, _safe(country), _safe(city), "alerts.json")

def append_records(base, records):
    by_file = defaultdict(list)
    for data in records:
        path = alerts_path(base, data.get("country", "unknown"), data.get("city", "unknown"))
        by_file[path].append(json.dumps(data, ensure_ascii=False))
    for path, lines in by_file.items():
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

def run_hdfs_writer(stop_event=None, base=None, topic=TOPIC, verbose=True):
    stop = stop_event or threading.Event()
    base = base or resolve_base()
    
    print(f"Connexion au broker Kafka ({BOOTSTRAP}) sur le topic '{topic}'...")
    consumer = KafkaConsumer(
        topic, 
        bootstrap_servers=[BOOTSTRAP], 
        group_id=GROUP_ID,
        auto_offset_reset="earliest", 
        enable_auto_commit=False,
        consumer_timeout_ms=5000 # Timeout de 5s pour voir si le consommateur reçoit quelque chose
    )
    
    if verbose:
        print(f"Écriture ciblée dans : {base}/{{country}}/{{city}}/alerts.json")
    
    total = 0
    try:
        while not stop.is_set():
            polled = consumer.poll(timeout_ms=2000)
            if not polled:
                print("⏳ En attente de messages sur Kafka...")
                continue
                
            records = []
            for tp, msgs in polled.items():
                for m in msgs:
                    try:
                        # Décodage robuste du message Kafka
                        val_str = m.value.decode("utf-8")
                        data = json.loads(val_str)
                        records.append(data)
                    except Exception as e:
                        print(f"⚠️ Erreur de parsing sur un message : {e} | Brut: {m.value}")
                        
            if records:
                append_records(base, records)
                consumer.commit()
                total += len(records)
                if verbose:
                    print(f"  ✅ +{len(records)} ligne(s) écrite(s) (total {total})")
                    
    except KeyboardInterrupt:
        print("\nArrêt demandé.")
    finally:
        consumer.close()
        if verbose:
            print("Consommateur HDFS arrêté.")

if __name__ == "__main__":
    run_hdfs_writer()