from flask import Flask, jsonify
import boto3
import pandas as pd
from io import StringIO

app = Flask(__name__)

BUCKET_NAME = 'gtfs-data-bucket-emanuele'
PREFIX = 'ingestion/'
STATIC_PATHS = {
    'stops': 'stops/stops.txt',
    'stop_times': 'stop_times /stop_times.txt',
    'routes': 'routes/routes.txt',
    'trips': 'trips/trips.txt'
}

s3 = boto3.client('s3')

def load_static_file(key, usecols=None):
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    df = pd.read_csv(obj['Body'])
    if usecols:
        df = df[usecols]
    df.columns = df.columns.str.strip().str.lower()
    return df

# Carica tutti i file statici
stops_df = load_static_file(STATIC_PATHS['stops'], ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'])
stop_times_df = load_static_file(STATIC_PATHS['stop_times'], ['trip_id', 'stop_id', 'arrival_time', 'departure_time'])
routes_df = load_static_file(STATIC_PATHS['routes'], ['route_id', 'route_short_name', 'route_long_name'])
trips_df = load_static_file(STATIC_PATHS['trips'], ['trip_id', 'route_id', 'trip_headsign'])

@app.route('/vehicle_positions')
def get_latest_vehicle_positions():
    # Trova il file più recente
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    files = [obj['Key'] for obj in response.get('Contents', []) if 'vehicle_positions' in obj['Key']]
    if not files:
        return jsonify([])

    files.sort(reverse=True)
    latest_file_key = files[0]

    # Leggi vehicle_positions.csv
    csv_obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_file_key)
    body = csv_obj['Body'].read().decode('utf-8')
    vehicles_df = pd.read_csv(StringIO(body))
    vehicles_df.columns = vehicles_df.columns.str.strip().str.lower()

    # Join con stop_times su trip_id + stop_id
    vehicles_df['stop_id'] = vehicles_df['current_stop_id']

    merged_df = vehicles_df.merge(stop_times_df, how='left', on=['trip_id', 'stop_id'])

    # Join con stops su stop_id
    merged_df = merged_df.merge(stops_df, how='left', on='stop_id')

    # Join con trips per trip_headsign e route_id
    merged_df = merged_df.merge(trips_df, how='left', on='trip_id')

    # Join con routes per nome linea
    merged_df = merged_df.merge(routes_df, how='left', on='route_id')

    # Rinomina e ordina le colonne chiave
    final = merged_df[[
        'vehicle_id', 'trip_id', 'trip_headsign', 'route_id', 'route_short_name',
        'current_stop_id', 'stop_id', 'stop_name',
        'arrival_time', 'departure_time', 'timestamp',
        'stop_lat', 'stop_lon'
    ]].rename(columns={
        'stop_lat': 'latitude',
        'stop_lon': 'longitude'
    })
    print("vehicle_positions:", vehicles_df.shape)
    print("+ stop_times:", stop_times_df.shape)
    print("merged:", merged_df.shape)
    print("final columns:", final.columns.tolist())

    return jsonify(final.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)
