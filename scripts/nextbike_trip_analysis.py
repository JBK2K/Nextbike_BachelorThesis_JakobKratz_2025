import requests
import time
import json
import csv
import os
from datetime import datetime, timedelta
from shapely.geometry import Point, Polygon
import threading
import os

# Konfiguration
city_id = 362  # Berlin
api_url = f"https://api.nextbike.net/maps/nextbike-live.json?city={city_id}"
flexzone_url = "https://api.nextbike.net/reservation/geojson/flexzone_bn.json"

# CSV-Dateiname für abgeschlossene Fahrten
nextbike_trips_csv = "results_trips/nextbike_trips.csv"

os.makedirs("results_trips", exist_ok=True)

# Speicher
station_status = {}         # Für Stationen
bike_status = {}            # Für freistehende Fahrräder
all_bikes_status = {}       # Für ALLE Fahrräder (egal wo)
flex_polygons = []          # Flexzonen
first_run = True            # Flag für ersten Durchlauf
bikes_in_transit = {}       # Fahrräder, die gerade ausgeliehen sind
bikes_pending_return = {}   # Fahrräder, die zurückgegeben wurden aber auf finale Position warten
freebike_booked = {}        # Freistehende Fahrräder mit ihrem vorherigen booked_bikes Status
newly_booked_bikes = set()  # Fahrräder, die im aktuellen Durchlauf gebucht wurden
bike_last_locations = {}    # Letzte bekannte Position jedes Fahrrads
bike_last_station = {}      # Speichert für jedes Bike die letzte Station (für Station→Flexzone Erkennung)
bikes_removed_from_stations = set() # Fahrräder, die kürzlich aus Stationen entfernt wurden

# Häufiger abfragen für weniger verpasste Fahrten
polling_interval = 5  # 5 Sekunden

# Wartezeit für finale Positionsbestimmung bei Rückgabe (in Sekunden)
return_confirmation_delay = 120  # 2 Minuten

# Debug-Level: 0=minimal, 1=normal, 2=ausführlich, 3=alle Details
debug_level = 1

# CSV-Datei initialisieren
def init_csv_file():
    header_line = "Bike-Number,Rental-Time,Rental-Type,Rental-Location,Rental-Lat,Rental-Lng,Return-Time,Return-Type,Return-Location,Return-Lat,Return-Lng,Duration-Minutes,Movement-Type\n"
    
    if os.path.exists(nextbike_trips_csv):
        with open(nextbike_trips_csv, 'r', encoding='utf-8') as file:
            content = file.readlines()
        
        if not content or "Bike-Number" not in content[0]:
            content.insert(0, header_line)
            with open(nextbike_trips_csv, 'w', encoding='utf-8') as file:
                file.writelines(content)
    else:
        with open(nextbike_trips_csv, 'w', encoding='utf-8') as file:
            file.write(header_line)
            
        print(f"CSV-Datei '{nextbike_trips_csv}' mit Header erstellt")

# Debug-Log mit konfigurierbarem Level
def debug_log(message, level=1):
    if level <= debug_level:
        print(message)

# Bewegungstyp bestimmen (erweitert um alle Kombinationen)
def get_movement_type(rental_type, return_type, rental_location, return_location):
    def is_noflex(loc):
        return loc == "außerhalb Flexzone"
    def is_flex(loc):
        return loc == "Flexzone"
    def is_station(typ):
        return typ.startswith("Station")
    def is_freistehend(typ):
        return typ == "Freistehend"

    # Station zu Station
    if is_station(rental_type) and is_station(return_type):
        return "Station:Station"
    # Station zu Flexzone/Noflexzone
    elif is_station(rental_type) and is_freistehend(return_type):
        if is_noflex(return_location):
            return "Station:NoFlexzone"
        elif is_flex(return_location):
            return "Station:Flexzone"
        else:
            return "Station:Freistehend"
    # Flexzone/Noflexzone zu Station
    elif is_freistehend(rental_type) and is_station(return_type):
        if is_noflex(rental_location):
            return "NoFlexzone:Station"
        elif is_flex(rental_location):
            return "Flexzone:Station"
        else:
            return "Freistehend:Station"
    # Flexzone/Noflexzone zu Flexzone/Noflexzone
    elif is_freistehend(rental_type) and is_freistehend(return_type):
        if is_noflex(rental_location) and is_noflex(return_location):
            return "NoFlexzone:NoFlexzone"
        elif is_noflex(rental_location) and is_flex(return_location):
            return "NoFlexzone:Flexzone"
        elif is_flex(rental_location) and is_noflex(return_location):
            return "Flexzone:NoFlexzone"
        elif is_flex(rental_location) and is_flex(return_location):
            return "Flexzone:Flexzone"
        else:
            return "Freistehend:Freistehend"
    else:
        return "Unbekannt"

# Kompletten Ausleihvorgang ins CSV schreiben
def write_trip_to_csv(bike_number, trip_data):
    # Dauer berechnen
    rental_time = datetime.strptime(trip_data['rental_time'], "%Y-%m-%d %H:%M:%S")
    return_time = datetime.strptime(trip_data['return_time'], "%Y-%m-%d %H:%M:%S")
    duration_seconds = (return_time - rental_time).total_seconds()
    duration_minutes = duration_seconds / 60  # in Minuten

    # Bewegungstyp bestimmen (erweitert)
    movement_type = get_movement_type(
        trip_data['rental_type'],
        trip_data['return_type'],
        trip_data['rental_location'],
        trip_data['return_location']
    )

    with open(nextbike_trips_csv, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([
            bike_number,
            trip_data['rental_time'],
            trip_data['rental_type'],
            trip_data['rental_location'],
            trip_data['rental_lat'],
            trip_data['rental_lng'],
            trip_data['return_time'],
            trip_data['return_type'],
            trip_data['return_location'],
            trip_data['return_lat'],
            trip_data['return_lng'],
            f"{duration_minutes:.1f}",
            movement_type
        ])

    debug_log(f"[{trip_data['return_time']}] Vollständige Fahrt für {bike_number} ins CSV geschrieben: "
          f"{movement_type}, ausgeliehen um {trip_data['rental_time']} in {trip_data['rental_location']}, "
          f"zurückgegeben um {trip_data['return_time']} bei {trip_data['return_location']}, "
          f"Dauer: {duration_minutes:.1f} Minuten")

# Flexzonen laden
def load_flexzones():
    global flex_polygons
    try:
        response = requests.get(flexzone_url)
        data = response.json()
        features = data.get('features', [])
        
        for feature in features:
            if feature['geometry']['type'] == 'Polygon':
                coords = feature['geometry']['coordinates']
                exterior = [(c[0], c[1]) for c in coords[0]]
                interiors = [[(c[0], c[1]) for c in inner] for inner in coords[1:]]
                flex_polygons.append(Polygon(exterior, interiors))
        
        debug_log(f"Flexzonen geladen: {len(flex_polygons)} Polygone")
    except Exception as e:
        debug_log(f"Fehler beim Laden der Flexzonen: {e}")

# Prüfen, ob ein Punkt in der Flexzone liegt
def is_in_flexzone(lng, lat):
    point = Point(lng, lat)
    return any(poly.contains(point) for poly in flex_polygons)

# API-Daten abrufen
def fetch_nextbike_data():
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        debug_log(f"Fehler beim Abrufen der API-Daten: {response.status_code}")
        return None

# Verzögerte finale Positionsprüfung und CSV-Eintrag
def finalize_bike_return(bike_number, trip_data):
    global bikes_pending_return
    
    # Ursprüngliche Rückgabezeit speichern
    original_return_time = trip_data['return_time']
    
    # Warte die vorgegebene Zeit
    time.sleep(return_confirmation_delay)
    
    # Aktuelle Zeit nach dem Warten
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    
    debug_log(f"[{current_time}] Prüfe finale Position für Bike {bike_number}, zurückgegeben um {original_return_time}")
    
    # Hole aktuelle API-Daten für finale Position
    current_data = fetch_nextbike_data()
    if not current_data:
        debug_log(f"Fehler beim Abrufen der finalen Position für Bike {bike_number}, verwende Ausgangsdaten")
        write_trip_to_csv(bike_number, trip_data)
        if bike_number in bikes_pending_return:
            del bikes_pending_return[bike_number]
        return
    
    found_bike = False
    
    # Nach dem Fahrrad suchen
    for country in current_data['countries']:
        for city in country.get('cities', []):
            if city.get('uid') == city_id:
                for place in city.get('places', []):
                    bike_list_full = place.get('bike_list', [])
                    
                    # Suche nach dem Fahrrad
                    for bike in bike_list_full:
                        if bike.get('number') == bike_number:
                            found_bike = True
                            lat = place.get('lat')
                            lng = place.get('lng')
                            in_flexzone = is_in_flexzone(lng, lat)
                            zone_type = "Flexzone" if in_flexzone else "außerhalb Flexzone"
                            
                            # Prüfen, ob das Fahrrad tatsächlich zurückgegeben wurde (nicht mehr gebucht ist)
                            is_still_booked = place.get('booked_bikes', 0) > 0 and not bike.get('active', True)
                            
                            if is_still_booked:
                                debug_log(f"[{current_time}] Bike {bike_number} ist noch gebucht! Fahrt wird fortgesetzt.")
                                # Zurück in Transit-Liste verschieben
                                bikes_in_transit[bike_number] = trip_data
                                if bike_number in bikes_pending_return:
                                    del bikes_pending_return[bike_number]
                                return
                            
                            # Update return_location
                            if not place.get('spot', False):
                                trip_data['return_location'] = zone_type
                                trip_data['return_lat'] = lat
                                trip_data['return_lng'] = lng
                                
                                debug_log(f"[{current_time}] FINALE POSITION für Bike {bike_number}: {zone_type} ({lat}, {lng})")
                            else:
                                station_type = "virtuell" if place.get('terminal_type') == "free" else "physisch"
                                trip_data['return_type'] = f"Station ({station_type})"
                                trip_data['return_location'] = place.get('name', 'Unbekannte Station')
                                trip_data['return_lat'] = lat
                                trip_data['return_lng'] = lng
                                
                                debug_log(f"[{current_time}] FINALE POSITION für Bike {bike_number}: Station {place.get('name')}")
                            
                            break
                    
                    if found_bike:
                        break
                
                if found_bike:
                    break
            
            if found_bike:
                break
    
    if not found_bike:
        debug_log(f"[{current_time}] Bike {bike_number} nicht mehr gefunden für finale Positionsbestimmung")
    
    # Ins CSV schreiben
    write_trip_to_csv(bike_number, trip_data)
    
    # Aus der pending-Liste entfernen
    if bike_number in bikes_pending_return:
        del bikes_pending_return[bike_number]

# Prüfe, ob ein kürzlich aus einer Station entferntes Fahrrad als eigenes Objekt erscheint
def check_removed_bikes_transformation(current_data):
    global bikes_removed_from_stations, bikes_in_transit, bikes_pending_return
    
    bikes_to_remove = set()  # Bikes, die aus der Tracking-Liste entfernt werden sollen
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    
    # Bike-Objekte in der aktuellen API-Antwort durchsuchen
    for country in current_data['countries']:
        for city in country.get('cities', []):
            if city.get('uid') == city_id:
                for place in city.get('places', []):
                    # Nur freistehende Fahrräder überprüfen
                    if place.get('bike', True) and not place.get('spot', False):
                        bike_numbers = place.get('bike_numbers', [])
                        if not bike_numbers:
                            continue
                            
                        bike_number = bike_numbers[0]
                        booked_bikes = place.get('booked_bikes', 0)
                        lat = place.get('lat')
                        lng = place.get('lng')
                        in_flexzone = is_in_flexzone(lng, lat)
                        zone_type = "Flexzone" if in_flexzone else "außerhalb Flexzone"
                        
                        # Ist dieses Fahrrad eines, das kürzlich aus einer Station entfernt wurde?
                        if bike_number in bikes_removed_from_stations:
                            bikes_to_remove.add(bike_number)
                            
                            # Ist es als ausgeliehen markiert (booked_bikes = 1)?
                            if booked_bikes == 1:
                                debug_log(f"[{current_time}] BESTÄTIGT: Bike {bike_number} aus Station entfernt und als eigenes Array mit booked_bikes=1 gefunden!", 1)
                                
                                # Zusätzliche Informationen über das letzte Rental tracken
                                if bike_number in bike_last_station:
                                    station_info = bike_last_station[bike_number]
                                    debug_log(f"    Ursprünglich ausgeliehen von: {station_info['name']} ({station_info['type']})", 2)
                            else:
                                debug_log(f"[{current_time}] DIREKTE VERSCHIEBUNG: Bike {bike_number} aus Station entfernt und direkt als freistehend (booked_bikes={booked_bikes}) gefunden!", 1)
                                
                                # In diesem Fall behandeln wir es als abgeschlossene Fahrt (wahrscheinlich durch Service-Team bewegt)
                                if bike_number in bikes_in_transit:
                                    trip_data = bikes_in_transit[bike_number]
                                    trip_data['return_time'] = current_time
                                    trip_data['return_type'] = "Freistehend"
                                    trip_data['return_location'] = zone_type
                                    trip_data['return_lat'] = lat
                                    trip_data['return_lng'] = lng
                                    
                                    debug_log(f"[{current_time}] ERFASSTE DIREKTE STATION→FLEXZONE BEWEGUNG für Bike {bike_number}!", 1)
                                    
                                    # In Pending-Liste aufnehmen und aus Transit entfernen
                                    bikes_pending_return[bike_number] = trip_data
                                    del bikes_in_transit[bike_number]
                                    
                                    # Finale Position verzögert prüfen in separatem Thread
                                    threading.Thread(target=finalize_bike_return, args=(bike_number, trip_data)).start()

    # Bikes aus dem Tracking entfernen, die jetzt als eigene Arrays identifiziert wurden
    bikes_removed_from_stations -= bikes_to_remove

# Tracking der Bewegungen
def track_bike_movements():
    global station_status, bike_status, first_run, bikes_in_transit, all_bikes_status, bikes_pending_return
    global freebike_booked, newly_booked_bikes, bike_last_locations, bike_last_station, bikes_removed_from_stations
    
    # CSV-Datei initialisieren
    init_csv_file()
    
    # Flexzonen laden
    load_flexzones()
    
    debug_log("Starte das Tracking der Fahrräder...")
    
    while True:
        # Leere die Liste der neu gebuchten Fahrräder für diesen Durchlauf
        newly_booked_bikes = set()
        
        data = fetch_nextbike_data()
        if not data:
            time.sleep(polling_interval)
            continue

        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        visible_bike_numbers = set()  # Alle aktuell sichtbaren Bike-Nummern
        current_all_bikes = {}        # Aktueller Status aller Fahrräder
        
        # Nach aus Stationen entfernten Fahrrädern suchen, die jetzt als eigene Arrays erscheinen
        if bikes_removed_from_stations:
            check_removed_bikes_transformation(data)
        
        # Alle Stationen und freistehenden Fahrräder durchgehen
        for country in data['countries']:
            for city in country.get('cities', []):
                if city.get('uid') == city_id:
                    # 1. Stationen verarbeiten
                    for place in city.get('places', []):
                        # Fahrradliste und Buchungsstatus für alle Plätze
                        bike_list_full = place.get('bike_list', [])
                        
                        # Alle Fahrräder an diesem Ort erfassen
                        for bike in bike_list_full:
                            bike_number = bike.get('number')
                            if bike_number:
                                visible_bike_numbers.add(bike_number)
                                current_all_bikes[bike_number] = {
                                    'lat': place.get('lat'),
                                    'lng': place.get('lng'),
                                    'active': bike.get('active', True),
                                    'state': bike.get('state', 'ok'),
                                    'spot': place.get('spot', False),
                                    'spot_name': place.get('name', '') if place.get('spot', False) else None,
                                    'terminal_type': place.get('terminal_type', ''),
                                    'is_booked': not bike.get('active', True),
                                    'booked_bikes': place.get('booked_bikes', 0),
                                    'place_uid': place.get('uid')
                                }
                                
                                # Position für jedes Fahrrad speichern
                                bike_last_locations[bike_number] = {
                                    'time': current_time,
                                    'lat': place.get('lat'),
                                    'lng': place.get('lng'),
                                    'spot': place.get('spot', False),
                                    'spot_name': place.get('name', '') if place.get('spot', False) else None,
                                    'terminal_type': place.get('terminal_type', ''),
                                    'in_flexzone': is_in_flexzone(place.get('lng'), place.get('lat'))
                                }
                                
                        # 1. Stationen verarbeiten
                        if place.get('spot', False):
                            station_id = place.get('uid')
                            name = place.get('name', 'Unbenannte Station')
                            terminal_type = place.get('terminal_type', '')
                            station_type = "virtuell" if terminal_type == "free" else "physisch"
                            bike_list = [bike['number'] for bike in place.get('bike_list', [])]
                            
                            # Station initialisieren, wenn neu
                            if station_id not in station_status:
                                station_status[station_id] = {
                                    "name": name,
                                    "station_type": station_type,
                                    "last_bike_list": bike_list
                                }
                                if not first_run:
                                    debug_log(f"[{current_time}] Neue Station gefunden: {name} ({station_type})")
                            
                            last_status = station_status[station_id]
                            
                            # Nur wenn nicht erster Durchlauf: Änderungen prüfen
                            if not first_run:
                                # Neu hinzugekommene Fahrräder identifizieren (Rückgabe an Station)
                                new_bikes_at_station = set(bike_list) - set(last_status["last_bike_list"])
                                
                                # Prüfen, ob eines der neuen Fahrräder in Transit ist
                                for bike in new_bikes_at_station:
                                    if bike in bikes_in_transit and bike not in newly_booked_bikes:
                                        # Als Stationsrückgabe erfassen
                                        trip_data = bikes_in_transit[bike]
                                        trip_data['return_time'] = current_time
                                        trip_data['return_type'] = f"Station ({station_type})"
                                        trip_data['return_location'] = name
                                        trip_data['return_lat'] = place.get('lat')
                                        trip_data['return_lng'] = place.get('lng')
                                        
                                        # Rückgabeart identifizieren
                                        rental_type = trip_data['rental_type']
                                        if rental_type.startswith("Station"):
                                            trans_type = "Station → Station"
                                        else:
                                            trans_type = "Flexzone → Station"
                                            
                                        debug_log(f"[{current_time}] Fahrrad {bike} wurde an Station {name} zurückgegeben ({trans_type}) - warte auf finale Position...")
                                        
                                        # In Pending-Liste aufnehmen und aus Transit entfernen
                                        bikes_pending_return[bike] = trip_data
                                        del bikes_in_transit[bike]
                                        
                                        # Finale Position verzögert prüfen in separatem Thread
                                        threading.Thread(target=finalize_bike_return, args=(bike, trip_data)).start()
                                
                                # Fahrräder, die entfernt wurden (aus der bike_list verschwunden)
                                removed_bikes = set(last_status["last_bike_list"]) - set(bike_list)
                                for bike in removed_bikes:
                                    debug_log(f"[{current_time}] Fahrrad {bike} wurde von {station_type}r Station {name} entfernt")
                                    
                                    # Wichtig: Dieses Fahrrad tracken, um zu überprüfen, ob es als eigenes Array erscheint
                                    bikes_removed_from_stations.add(bike)
                                    
                                    # Stationsdaten für spätere Referenz speichern
                                    bike_last_station[bike] = {
                                        'name': name,
                                        'type': station_type,
                                        'time': current_time,
                                        'lat': place.get('lat'),
                                        'lng': place.get('lng')
                                    }
                                    
                                    # Fahrrad in Transit-Liste aufnehmen
                                    bikes_in_transit[bike] = {
                                        'rental_time': current_time,
                                        'rental_type': f"Station ({station_type})",
                                        'rental_location': name,
                                        'rental_lat': place.get('lat'),
                                        'rental_lng': place.get('lng')
                                    }
                                    
                                    # Als neu gebucht markieren
                                    newly_booked_bikes.add(bike)

                            # Aktualisiere den Status der Station
                            last_status["last_bike_list"] = bike_list
                        
                        # 2. Freistehende Fahrräder verarbeiten
                        elif place.get('bike', False) and not place.get('spot', False):
                            bike_number = place.get('bike_numbers', ['unbekannt'])[0] if place.get('bike_numbers') else 'unbekannt'
                            if bike_number == 'unbekannt':
                                continue
                                
                            lat = place.get('lat')
                            lng = place.get('lng')
                            in_flexzone = is_in_flexzone(lng, lat)
                            zone_type = "Flexzone" if in_flexzone else "außerhalb Flexzone"
                            booked_bikes = place.get('booked_bikes', 0)
                            
                            # Prüfe, ob es einen vorherigen Wert für booked_bikes gibt
                            bike_uid = place.get('uid')
                            if bike_uid not in freebike_booked:
                                freebike_booked[bike_uid] = {
                                    "booked_bikes": booked_bikes,
                                    "bike_number": bike_number,
                                    "last_seen_time": current_time,
                                    "last_position": {"lat": lat, "lng": lng}
                                }
                            
                            # Wenn booked_bikes von 0 auf 1 wechselt -> Ausleihe starten
                            if not first_run and freebike_booked[bike_uid]["booked_bikes"] == 0 and booked_bikes == 1:
                                debug_log(f"[{current_time}] Fahrrad {bike_number} wurde freistehend in {zone_type} gebucht (booked_bikes: 0->1)")
                                
                                # Fahrrad in Transit-Liste aufnehmen
                                bikes_in_transit[bike_number] = {
                                    'rental_time': current_time,
                                    'rental_type': "Freistehend",
                                    'rental_location': zone_type,
                                    'rental_lat': lat,
                                    'rental_lng': lng
                                }
                                
                                # Als neu gebucht markieren
                                newly_booked_bikes.add(bike_number)
                            
                            # Wenn booked_bikes von 1 auf 0 wechselt -> Rückgabe erkennen
                            elif not first_run and freebike_booked[bike_uid]["booked_bikes"] == 1 and booked_bikes == 0:
                                debug_log(f"[{current_time}] Fahrrad {bike_number} wurde freistehend zurückgegeben (booked_bikes: 1->0)")
                                
                                # Prüfen, ob das Fahrrad in der Transit-Liste ist
                                if bike_number in bikes_in_transit:
                                    # Komplette Fahrt erfassen
                                    trip_data = bikes_in_transit[bike_number]
                                    trip_data['return_time'] = current_time
                                    trip_data['return_type'] = "Freistehend"
                                    trip_data['return_location'] = zone_type
                                    trip_data['return_lat'] = lat
                                    trip_data['return_lng'] = lng
                                    
                                    # Rückgabeart identifizieren (von wo nach wo)
                                    rental_type = trip_data['rental_type']
                                    if rental_type.startswith("Station"):
                                        trans_type = "Station → Flexzone"
                                        debug_log(f"[{current_time}] ERFOLGREICHE STATION→FLEXZONE BEWEGUNG für Bike {bike_number}!", 1)
                                    else:
                                        trans_type = "Flexzone → Flexzone"
                                        
                                    debug_log(f"[{current_time}] Fahrrad {bike_number} wurde freistehend ({zone_type}) zurückgegeben ({trans_type}) - warte auf finale Position...")
                                    
                                    # In Pending-Liste aufnehmen und aus Transit entfernen
                                    bikes_pending_return[bike_number] = trip_data
                                    del bikes_in_transit[bike_number]
                                    
                                    # Finale Position verzögert prüfen in separatem Thread
                                    threading.Thread(target=finalize_bike_return, args=(bike_number, trip_data)).start()
                            
                            # Update den booked_bikes Status und Position
                            freebike_booked[bike_uid]["booked_bikes"] = booked_bikes
                            freebike_booked[bike_uid]["bike_number"] = bike_number
                            freebike_booked[bike_uid]["last_seen_time"] = current_time
                            freebike_booked[bike_uid]["last_position"] = {"lat": lat, "lng": lng}
                            
                            # Fahrrad initialisieren, wenn neu
                            if bike_number not in bike_status:
                                bike_status[bike_number] = {
                                    "visible": True,
                                    "last_seen": current_time,
                                    "last_position": {"lat": lat, "lng": lng},
                                    "in_flexzone": in_flexzone
                                }
                                if not first_run:
                                    debug_log(f"[{current_time}] Neues freies Fahrrad {bike_number} gefunden in {zone_type}")
                                
                            # Fahrrad-Status aktualisieren
                            last_status = bike_status[bike_number]
                            last_status["visible"] = True
                            last_status["last_seen"] = current_time
                            last_status["last_position"] = {"lat": lat, "lng": lng}
                            last_status["in_flexzone"] = in_flexzone

        # Aktualisiere den Status aller Fahrräder für die nächste Iteration
        all_bikes_status = current_all_bikes

        # Wenn mehr als 5 Minuten seit Entfernung eines Fahrrads vergangen sind und es nicht als eigenes Array gefunden wurde
        current_datetime = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
        bikes_to_clear = set()
        
        for bike in bikes_removed_from_stations:
            if bike in bike_last_station:
                removal_time = datetime.strptime(bike_last_station[bike]['time'], "%Y-%m-%d %H:%M:%S")
                if (current_datetime - removal_time).total_seconds() > 300:  # 5 Minuten
                    debug_log(f"[{current_time}] Fahrrad {bike} aus Station entfernt aber nach 5 Minuten nicht als eigenes Array gefunden", 1)
                    bikes_to_clear.add(bike)
        
        bikes_removed_from_stations -= bikes_to_clear

        # Nach dem ersten Durchlauf Flag zurücksetzen
        if first_run:
            first_run = False
            debug_log(f"[{current_time}] Initialisierung abgeschlossen. Beginne mit Tracking von Änderungen.")
        
        # Prüfe auf "verlorene" Fahrten (Fahrräder in Transit seit über 24 Stunden)
        current_datetime = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
        bikes_to_remove = []
        
        for bike_number, trip_data in bikes_in_transit.items():
            rental_time = datetime.strptime(trip_data['rental_time'], "%Y-%m-%d %H:%M:%S")
            if (current_datetime - rental_time).total_seconds() > 86400:  # Mehr als 24 Stunden
                debug_log(f"[{current_time}] Fahrt für Bike {bike_number} wird als verloren markiert (>24h)")
                trip_data['return_time'] = current_time
                trip_data['return_type'] = "Unbekannt (verloren)"
                trip_data['return_location'] = "Unbekannt"
                trip_data['return_lat'] = None
                trip_data['return_lng'] = None
                write_trip_to_csv(bike_number, trip_data)
                bikes_to_remove.append(bike_number)

        for bike in bikes_to_remove:
            del bikes_in_transit[bike]
        
        # Status speichern
        with open("results_trips/bike_movements.json", "w") as file:
            json.dump({
                "timestamp": current_time,
                "stations": station_status,
                "bikes": bike_status,
                "in_transit": bikes_in_transit,
                "pending_return": bikes_pending_return,
                "freebike_booked": freebike_booked,
                "bike_last_station": bike_last_station,
                "bikes_removed_from_stations": list(bikes_removed_from_stations),
                "stats": {
                    "stations_count": len(station_status),
                    "bikes_count": len(bike_status),
                    "in_transit_count": len(bikes_in_transit),
                    "pending_return_count": len(bikes_pending_return)
                }
            }, file, indent=4)
        
        time.sleep(polling_interval)

if __name__ == "__main__":
    # Starte das Tracking mit robuster Fehlerbehandlung und automatischem Neustart bei Fehlern
    while True:
        try:
            track_bike_movements()
        except KeyboardInterrupt:
            debug_log("\nTracking wurde vom Benutzer beendet")
            break
        except Exception as e:
            debug_log(f"Fehler im Tracking: {e}")
            # Fehler protokollieren
            with open("error_log.txt", "a") as file:
                file.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')}: {str(e)}\n")
            debug_log("Script wird in 10 Sekunden automatisch neu gestartet...", 1)
            time.sleep(10)