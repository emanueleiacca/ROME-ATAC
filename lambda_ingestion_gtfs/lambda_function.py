import boto3
import requests
import csv
import time
from datetime import datetime
from google.transit import gtfs_realtime_pb2

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = 'gtfs-data-bucket-emanuele'
    base_folder = 'ingestion/'
    
    now = datetime.utcnow()
    date_str = now.strftime('%Y-%m-%d')
    hour_str = now.strftime('%H')
    s3_folder = f'{base_folder}date={date_str}/hour={hour_str}/'
    
    # URLs feed ATAC
    trip_updates_url = 'https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb'
    vehicle_positions_url = 'https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb'
    
    #### --- Scaricare Trip Updates
    response_trip = requests.get(trip_updates_url)
    feed_trip = gtfs_realtime_pb2.FeedMessage()
    feed_trip.ParseFromString(response_trip.content)
    
    trip_filename = f'{int(time.time())}_trip_updates.csv'
    trip_local_path = f'/tmp/{trip_filename}'
    
    with open(trip_local_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['trip_id', 'route_id', 'stop_id', 'arrival_time', 'departure_time'])
        
        for entity in feed_trip.entity:
            if entity.HasField('trip_update'):
                trip = entity.trip_update.trip
                for stop_time_update in entity.trip_update.stop_time_update:
                    arrival = stop_time_update.arrival.time if stop_time_update.HasField('arrival') else ''
                    departure = stop_time_update.departure.time if stop_time_update.HasField('departure') else ''
                    writer.writerow([
                        trip.trip_id,
                        trip.route_id,
                        stop_time_update.stop_id,
                        arrival,
                        departure
                    ])
    
    s3.upload_file(trip_local_path, bucket_name, s3_folder + trip_filename)
    
    #### --- Scaricare Vehicle Positions
    response_vehicle = requests.get(vehicle_positions_url)
    feed_vehicle = gtfs_realtime_pb2.FeedMessage()
    feed_vehicle.ParseFromString(response_vehicle.content)
    
    vehicle_filename = f'{int(time.time())}_vehicle_positions.csv'
    vehicle_local_path = f'/tmp/{vehicle_filename}'
    
    with open(vehicle_local_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['trip_id', 'route_id', 'vehicle_id', 'current_stop_id', 'timestamp'])
        
        for entity in feed_vehicle.entity:
            if entity.HasField('vehicle'):
                trip = entity.vehicle.trip
                vehicle = entity.vehicle
                writer.writerow([
                    trip.trip_id,
                    trip.route_id,
                    vehicle.vehicle.id,
                    vehicle.stop_id,
                    vehicle.timestamp
                ])
    
    s3.upload_file(vehicle_local_path, bucket_name, s3_folder + vehicle_filename)
    
    return {
        'statusCode': 200,
        'body': f'Uploaded {trip_filename} and {vehicle_filename} to {s3_folder}'
    }

