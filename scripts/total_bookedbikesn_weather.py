import os
import json
import requests
import time
from datetime import datetime

def save_data_to_json(nextbike_data, weather_data, weather_20_data, filename='results_total_bikes/nextbike_weather_data.json'):
    """
    Speichert Nextbike- und Wetterdaten einfach in JSON
    """
    try:
        country = nextbike_data.get('countries', [{}])[0]
        current_weather = weather_data.get('current_condition', [{}])[0]
        
        # Wetter 2.0 aktuelle Werte extrahieren
        weather_20_current = weather_20_data.get('current', {}) if weather_20_data else {}

        entry = {
            'timestamp': datetime.now().isoformat(),
            'booked_bikes': country.get('booked_bikes'),
            'set_point_bikes': country.get('set_point_bikes'), 
            'available_bikes': country.get('available_bikes'),
            'current_weather_condition': current_weather,
            'weather_20_current': weather_20_current
        }
        
        try:
            if os.path.exists(filename):
                with open(filename, 'r') as f:
                    data = json.load(f)
            else:
                data = []
        except Exception as e:
            print(f"❌ Fehler beim Laden der Datei: {e}")
            data = []
        
        data.append(entry)
        
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"❌ Fehler beim Speichern: {e}")
            return False
        
        print(f"✅ Gespeichert: {entry.get('available_bikes')} verfügbare Bikes, {entry.get('booked_bikes')} gebucht")
        return True
        
    except Exception as e:
        print(f"❌ Fehler in save_data_to_json: {e}")
        return False

def collect_data():
    """
    Sammelt alle 10 Sekunden Nextbike- und Wetterdaten
    """
    nextbike_url = "https://api.nextbike.net/maps/nextbike-live.json?city=362"
    weather_url = "https://wttr.in/Berlin?format=j1"
    weather_20 = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m,precipitation&timezone=Europe%2FBerlin"

    print("🚀 Starte Datensammlung...")
    print("Drücke Ctrl+C zum Beenden")
    
    while True:
        nextbike_data = {}
        weather_data = {}
        weather_20_data = {}
        try:
            try:
                print("📡 Rufe Nextbike-Daten ab...")
                nextbike_response = requests.get(nextbike_url, timeout=10)
                nextbike_data = nextbike_response.json()
            except Exception as e:
                print(f"🌐 Fehler beim Abrufen der Nextbike-Daten: {e}")
            
            try:
                print("🌤️ Rufe Wetterdaten ab...")
                weather_response = requests.get(weather_url, timeout=10)
                weather_data = weather_response.json()
            except Exception as e:
                print(f"🌐 Fehler beim Abrufen der Wetterdaten: {e}")

            try:
                print("🌦️ Rufe Wetter 2.0 Daten ab...")
                weather_20_response = requests.get(weather_20, timeout=10)
                weather_20_data = weather_20_response.json()
            except Exception as e:
                print(f"🌐 Fehler beim Abrufen der Wetter 2.0 Daten: {e}")
            
            try:
                save_data_to_json(nextbike_data, weather_data, weather_20_data)
            except Exception as e:
                print(f"❌ Fehler beim Speichern der Daten: {e}")
            
            print(f"⏱️ Warte 10 Sekunden...")
            print("-" * 50)
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\n🛑 Datensammlung beendet.")
            print(f"📁 Daten gespeichert in: nextbike_weather_data.json")
            break
        except Exception as e:
            print(f"❌ Unerwarteter Fehler in collect_data: {e}")
            print("⏱️ Warte 5 Sekunden und mache weiter...")
            time.sleep(5)

if __name__ == "__main__":
    collect_data()
