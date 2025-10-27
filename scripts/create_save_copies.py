import os
import shutil
import time
from datetime import datetime

# Projekt-Root (eine Ebene über dem scripts-Ordner)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

FOLDERS_TO_COPY = [
    os.path.join(BASE_DIR, "results_station_reservation"),
    os.path.join(BASE_DIR, "results_total_bikes"),
    os.path.join(BASE_DIR, "results_trips"),
]
# Sicherungsordner-Basis im Projekt-Root
BACKUP_BASE = os.path.join(BASE_DIR, "safety_copies")

# Alle 4 Stunden (in Sekunden)
INTERVAL = 4 * 60 * 60  # 4 Stunden

def make_backup():
    now = datetime.now()
    folder_name = now.strftime("%Y-%m-%d_%H-%M")
    backup_dir = os.path.join(BACKUP_BASE, folder_name)
    os.makedirs(backup_dir, exist_ok=True)
    for folder in FOLDERS_TO_COPY:
        if os.path.exists(folder):
            try:
                dest = os.path.join(backup_dir, os.path.basename(folder))
                if os.path.isdir(folder):
                    if os.path.exists(dest):
                        shutil.rmtree(dest)
                    shutil.copytree(folder, dest)
                else:
                    shutil.copy2(folder, dest)
            except Exception as e:
                print(f"Fehler beim Kopieren von {folder}: {e}")
        else:
            print(f"Ordner/Datei nicht gefunden: {folder}")

def main():
    while True:
        try:
            make_backup()
            print(f"Sicherheitskopie erstellt um {datetime.now().isoformat(timespec='seconds')}")
        except Exception as e:
            print(f"Backup-Fehler: {e}")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()