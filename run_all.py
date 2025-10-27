import subprocess
import time
import datetime

SCRIPTS = [
    "scripts/station_reservation.py",
    "scripts/create_save_copies.py",
    "scripts/total_bookedbikesn_weather.py",
    "scripts/nextbike_trip_analysis.py",
]

PROCESSES = []

def log(msg):
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def start_script(script):
    try:
        proc = subprocess.Popen(["python3", script])
        log(f"Gestartet: {script} (PID: {proc.pid})")
        return proc
    except Exception as e:
        log(f"Fehler beim Starten von {script}: {e}")
        return None

def main():
    for script in SCRIPTS:
        proc = start_script(script)
        PROCESSES.append((script, proc))

    try:
        while True:
            for i, (script, proc) in enumerate(PROCESSES):
                if proc is None or proc.poll() is not None:
                    log(f"{script} ist abgestürzt oder konnte nicht gestartet werden. Starte neu ...")
                    time.sleep(2)  # Kurze Pause vor Neustart
                    new_proc = start_script(script)
                    PROCESSES[i] = (script, new_proc)
            time.sleep(10)
    except KeyboardInterrupt:
        log("Beende alle Prozesse ...")
        for _, proc in PROCESSES:
            if proc and proc.poll() is None:
                proc.terminate()

if __name__ == "__main__":
    main()