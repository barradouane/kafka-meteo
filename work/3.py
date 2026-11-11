import json
import os
import time
import requests
import builtins
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "weather_stream"

CITIES = [
    {"city": "Paris",      "country": "France",               "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Seville",    "country": "Spain",                "latitude": 37.3891, "longitude": -5.9845},
    {"city": "Dubai",      "country": "United Arab Emirates", "latitude": 25.2048, "longitude": 55.2708},
    {"city": "Reykjavik",  "country": "Iceland",              "latitude": 64.1466, "longitude": -21.9426},
    {"city": "Wellington", "country": "New Zealand",          "latitude": -41.2865, "longitude": 174.7762},
]

# Changement de l'URL : on demande l'historique horaire (hourly) sur les 3 derniers jours
API_URL_HISTORY = (
    "https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}"
    "&hourly=temperature_2m,wind_speed_10m,weather_code&wind_speed_unit=ms&past_days=3&forecast_days=0"
)

def run_historical_producer():
    print("Récupération de l'historique (3 jours) pour toutes les villes...")
    
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    
    # 1. On stocke d'abord les historiques complets de chaque ville
    all_city_data = {}
    for loc in CITIES:
        resp = requests.get(API_URL_HISTORY.format(**loc), timeout=15)
        resp.raise_for_status()
        all_city_data[loc["city"]] = {
            "loc_info": loc,
            "hourly": resp.json()["hourly"]
        }
    
    # 2. On détermine le nombre total d'heures récupérées (3 jours = 72 heures)
    nb_heures = len(all_city_data["Paris"]["hourly"]["time"])
    print(f"Historique chargé : {nb_heures} relevés trouvés. Début du streaming accéléré...")

    # 3. On injecte les données dans Kafka chronologiquement, en simulant le temps réel
    try:
        for i in range(nb_heures):
            for city_name, data in all_city_data.items():
                loc = data["loc_info"]
                hourly = data["hourly"]
                
                # On reconstruit le JSON exactement comme s'il venait de l'API "current"
                # pour que les exercices 4, 5 et 6 fonctionnent sans aucune modification
                payload = {
                    "current": {
                        "time": hourly["time"][i],
                        "temperature_2m": hourly["temperature_2m"][i],
                        "wind_speed_10m": hourly["wind_speed_10m"][i],
                        "weather_code": hourly["weather_code"][i]
                    },
                    "city": loc["city"],
                    "country": loc["country"]
                }
                
                producer.send(TOPIC, key=loc["city"], value=payload)
                
            producer.flush()
            
            # Affichage de contrôle pour la première ville (Paris)
            temp_paris = all_city_data["Paris"]["hourly"]["temperature_2m"][i]
            timestamp = all_city_data["Paris"]["hourly"]["time"][i]
            print(f"[{timestamp}] Lot envoyé (ex: Paris {temp_paris}°C)")
            
            # Pause d'une demi-seconde pour ne pas saturer Spark, mais simuler un flux rapide
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print("Producteur historique arrêté.")

if __name__ == "__main__":
    run_historical_producer()