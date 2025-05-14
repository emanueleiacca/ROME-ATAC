import csv
import json

input_file = "data/stops.txt"
output_file = "stop_coords.json"

stop_coords = {}

with open(input_file, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        stop_id = row["stop_id"]
        try:
            lat = float(row["stop_lat"])
            lon = float(row["stop_lon"])
            stop_coords[stop_id] = [lat, lon]
        except:
            continue  # Salta se i valori non sono validi

with open(output_file, "w", encoding='utf-8') as f:
    json.dump(stop_coords, f, indent=2)

print(f"Salvato {len(stop_coords)} fermate in {output_file}")
