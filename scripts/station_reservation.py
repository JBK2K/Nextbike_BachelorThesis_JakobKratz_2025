import requests
import csv
import time
import threading
from datetime import datetime
import os

API_URL = "https://api.nextbike.net/maps/nextbike-live.json?city=362"
CSV_FILE = "results_station_reservation/station_reservations.csv"
POLL_INTERVAL = 5  # Sekunden

os.makedirs("results_station_reservation", exist_ok=True)

CSV_HEADER = [
    "timestamp", "station_name", "event_type", "duration_seconds",
    "booked_bikes_entry", "booked_bikes_exit",
    "available_bikes_before", "available_bikes_after",
    "bikes_available_to_rent", "bike_racks", "free_racks", "special_racks"
]

def get_station_data():
    resp = requests.get(API_URL)
    resp.raise_for_status()
    data = resp.json()
    stations = []
    for place in data['countries'][0]['cities'][0]['places']:
        if not place.get("bike", False):
            stations.append({
                "name": place.get("name"),
                "booked_bikes": place.get("booked_bikes", 0),
                "bikes": place.get("bikes", 0),
                "bikes_available_to_rent": place.get("bikes_available_to_rent", 0),
                "bike_racks": place.get("bike_racks", 0),
                "free_racks": place.get("free_racks", 0),
                "special_racks": place.get("special_racks", 0),
                "bike_list": [b["number"] for b in place.get("bike_list", [])]
            })
    return stations

def delayed_log(log_row, name, bike_number, bikes_before, bikes_after, last_logged_event):
    time.sleep(10)
    stations = get_station_data()
    for s in stations:
        if s["name"] == name:
            # Nur loggen, wenn das Bike wirklich weg ist UND die Anzahl gesunken ist
            if bike_number not in s["bike_list"] and s["bikes"] < bikes_before:
                # Duplikate verhindern
                if last_logged_event.get((name, bike_number)) != log_row:
                    with open(CSV_FILE, "a", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow(log_row)
                    last_logged_event[(name, bike_number)] = log_row
            break

def main():
    last_state = {}
    last_logged_event = {}

    # Schreibe Header, falls Datei nicht existiert
    try:
        with open(CSV_FILE, "x", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)
    except FileExistsError:
        pass

    while True:
        stations = get_station_data()
        now = datetime.now().isoformat(timespec="seconds")

        for s in stations:
            name = s["name"]
            booked = s["booked_bikes"]
            bikes = s["bikes"]
            avail = s["bikes_available_to_rent"]
            racks = s["bike_racks"]
            free_racks = s["free_racks"]
            special_racks = s["special_racks"]
            bike_list = set(s["bike_list"])

            # Stationen ohne verfügbare Bikes ignorieren
            if bikes == 0:
                if name in last_state:
                    del last_state[name]
                continue

            prev = last_state.get(name, {"pending": [], "bike_list": set(), "booked_taken_bikes": set()})
            prev_booked = prev.get("booked_bikes", 0)
            prev_bike_list = prev.get("bike_list", set())
            pending = prev.get("pending", [])
            booked_taken_bikes = prev.get("booked_taken_bikes", set())
            available_bikes_before = prev.get("bikes", 0)
            available_bikes_after = bikes

            # Neue Bookings erkennen
            if booked > prev_booked:
                for _ in range(booked - prev_booked):
                    pending.append({
                        "start_time": now,
                        "start_bike_list": bike_list.copy(),
                        "booked_bikes_entry": booked,
                        "bikes": bikes,
                        "bikes_available_to_rent": avail,
                        "bike_racks": racks,
                        "free_racks": free_racks,
                        "special_racks": special_racks
                    })

            # Bookings beendet
            if booked < prev_booked:
                for _ in range(prev_booked - booked):
                    if pending:
                        event = pending.pop(0)
                        duration = (datetime.fromisoformat(now) - datetime.fromisoformat(event["start_time"])).total_seconds()
                        bike_taken = event["start_bike_list"] - bike_list
                        event_type = "booked:bike_taken" if bike_taken else "booked:not_taken"
                        # Nur loggen, wenn sich die Anzahl der Bikes verändert hat
                        if event_type == "booked:bike_taken" and available_bikes_after < available_bikes_before:
                            log_row = [
                                now, name, event_type, int(duration),
                                event["booked_bikes_entry"], booked,
                                available_bikes_before, available_bikes_after,
                                avail, racks, free_racks, special_racks
                            ]
                            if last_logged_event.get((name, tuple(bike_taken))) != log_row:
                                with open(CSV_FILE, "a", newline="") as f:
                                    writer = csv.writer(f)
                                    writer.writerow(log_row)
                                last_logged_event[(name, tuple(bike_taken))] = log_row
                            booked_taken_bikes.update(bike_taken)
                        elif event_type == "booked:not_taken":
                            log_row = [
                                now, name, event_type, int(duration),
                                event["booked_bikes_entry"], booked,
                                available_bikes_before, available_bikes_after,
                                avail, racks, free_racks, special_racks
                            ]
                            if last_logged_event.get((name, "not_taken")) != log_row:
                                with open(CSV_FILE, "a", newline="") as f:
                                    writer = csv.writer(f)
                                    writer.writerow(log_row)
                                last_logged_event[(name, "not_taken")] = log_row

            # --- NEU: Bike verschwindet ohne Booking ---
            if booked == 0 and not pending:
                bikes_gone = prev_bike_list - bike_list
                # Nur Bikes loggen, die NICHT schon als booked:bike_taken geloggt wurden
                bikes_gone = bikes_gone - booked_taken_bikes
                # Nur loggen, wenn sich die Anzahl der Bikes verändert hat
                if bikes_gone and available_bikes_after < available_bikes_before:
                    for bike_number in bikes_gone:
                        log_row = [
                            now, name, "not_booked:bike_taken", 0,
                            0, 0, available_bikes_before, available_bikes_after,
                            avail, racks, free_racks, special_racks
                        ]
                        threading.Thread(
                            target=delayed_log,
                            args=(log_row, name, bike_number, available_bikes_before, available_bikes_after, last_logged_event)
                        ).start()
                # Nach dem Loggen zurücksetzen
                booked_taken_bikes = set()

            # Update State
            last_state[name] = {
                "booked_bikes": booked,
                "bike_list": bike_list,
                "bikes": bikes,
                "bikes_available_to_rent": avail,
                "bike_racks": racks,
                "free_racks": free_racks,
                "special_racks": special_racks,
                "pending": pending,
                "booked_taken_bikes": booked_taken_bikes
            }

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()