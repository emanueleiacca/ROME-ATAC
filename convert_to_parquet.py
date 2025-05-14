import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime, timedelta
import s3fs

# === Config ===
s3 = boto3.client('s3')
BUCKET = 'gtfs-data-bucket-emanuele'
OUTPUT_PREFIX = 'parquet_vehicle_positions/'

# === Ora attuale UTC
from datetime import datetime, timedelta

# Configurazione
MAX_HOURS_LOOKBACK = 3  # quante ore vuoi controllare a ritroso

now = datetime.utcnow()
found = False

for i in range(MAX_HOURS_LOOKBACK):
    check_time = now - timedelta(hours=i)
    today = check_time.strftime('%Y-%m-%d')
    hour = check_time.strftime('%H')
    prefix = f'ingestion/date={today}/hour={hour}/'

    print(f"🔍 Cerco in: {prefix}")
    res = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    csv_keys = sorted([obj['Key'] for obj in res.get('Contents', []) if obj['Key'].endswith('.csv')])

    if csv_keys:
        latest_key = csv_keys[-1]
        print(f"📄 Converto: {latest_key}")
        found = True
        break

if not found:
    print(f"❌ Nessun file trovato nelle ultime {MAX_HOURS_LOOKBACK} ore.")
    exit(1)


# === Leggi CSV
obj = s3.get_object(Bucket=BUCKET, Key=latest_key)
df = pd.read_csv(BytesIO(obj['Body'].read()))
df['current_stop_id'] = df['current_stop_id'].astype(str).str.replace(r'\.0$', '', regex=True)
# === Converti timestamp + filtra ultimi 15 min
df['timestamp_datetime'] = pd.to_datetime(df['timestamp'], unit='s')
cutoff = now - timedelta(minutes=15)
df = df[df['timestamp_datetime'] > cutoff]

# === Aggiungi colonne di partizione
df['date'] = today
df['hour'] = hour

# === Scrivi Parquet su S3 con s3fs
fs = s3fs.S3FileSystem()
output_path = f"s3://{BUCKET}/{OUTPUT_PREFIX}"

# Forza anche il tipo schema Arrow come stringa
schema = pa.schema([
    ('trip_id', pa.string()),
    ('route_id', pa.string()),
    ('vehicle_id', pa.string()),
    ('current_stop_id', pa.string()),
    ('timestamp', pa.int64()),
    ('timestamp_datetime', pa.timestamp('ms')),
    ('date', pa.string()),
    ('hour', pa.string()),
])
table = pa.Table.from_pandas(df, schema=schema)

pq.write_to_dataset(
    table,
    root_path=output_path,
    partition_cols=['date', 'hour'],
    filesystem=fs
)

print(f"✅ Parquet salvato su {output_path} con {len(df)} righe.")
