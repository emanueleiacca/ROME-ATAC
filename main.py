from fastapi import FastAPI, Query
from typing import Optional
import boto3
import os
import json
from datetime import datetime, timedelta
from geopy.distance import geodesic
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Setup boto3 client per Athena
athena = boto3.client(
    'athena',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

DATABASE = "gtfs"
TABLE = "vehicle_positions_parquet"
S3_OUTPUT = "s3://gtfs-data-bucket-emanuele/athena-results/"

def run_athena_query(query: str):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": S3_OUTPUT}
    )
    execution_id = response['QueryExecutionId']

    while True:
        result = athena.get_query_execution(QueryExecutionId=execution_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

    if state != 'SUCCEEDED':
        raise Exception(f"Query failed: {state}")

    result = athena.get_query_results(QueryExecutionId=execution_id)
    rows = result['ResultSet']['Rows'][1:]  # skip header

    bus_data = []
    for row in rows:
        values = [col.get("VarCharValue", "") for col in row['Data']]
        bus_data.append({
            "trip_id": values[0],
            "route_id": values[1],
            "vehicle_id": values[2],
            "current_stop_id": values[3],
            "timestamp": values[4],
            "timestamp_datetime": values[5]
        })

    return bus_data

with open("stop_coords.json", "r", encoding="utf-8") as f:
    STOP_COORDS = json.load(f)

@app.get("/bus_positions")
def get_bus_positions(
    lat: float = Query(..., description="Latitudine dell'utente"),
    lon: float = Query(..., description="Longitudine dell'utente"),
    linea: Optional[str] = Query(None, description="Linea bus es: '64'"),
    raggio_km: float = Query(1.0, description="Raggio in km")
):
    try:
        now = datetime.utcnow()
        three_minutes_ago = now - timedelta(minutes=10)
        time_filter = three_minutes_ago.strftime('%Y-%m-%d %H:%M:%S')

        query = f"""
        SELECT trip_id, route_id, vehicle_id, current_stop_id, timestamp, timestamp_datetime
        FROM {TABLE}
        WHERE timestamp_datetime > TIMESTAMP '{time_filter}'
        """
        if linea:
            query += f" AND route_id = '{linea}'"

        bus_positions = run_athena_query(query)

        filtered = []
        for bus in bus_positions:
            stop_id = bus["current_stop_id"]
            if stop_id in STOP_COORDS:
                stop_lat, stop_lon = STOP_COORDS[stop_id]
                distance = geodesic((lat, lon), (stop_lat, stop_lon)).km
                if distance <= raggio_km:
                    bus["lat"] = stop_lat
                    bus["lon"] = stop_lon
                    filtered.append(bus)

        return filtered

    except Exception as e:
        print(f"❌ ERRORE: {e}")
        return {"error": str(e)}

