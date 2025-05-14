import boto3
from io import BytesIO
import pandas as pd
from datetime import datetime

# === Configurazione iniziale ===
s3 = boto3.client('s3')
BUCKET = 'gtfs-data-bucket-emanuele'

# === Oggi / ora attuale (UTC) ===
now = datetime.utcnow()
today = now.strftime('%Y-%m-%d')
hour = now.strftime('%H')

prefix = f'ingestion/date={today}/hour={hour}/'
print(f"📂 Cerco in: {prefix}")

# === Lista dei file CSV nella cartella ===
res = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
csv_files = [obj['Key'] for obj in res.get('Contents', []) if obj['Key'].endswith('.csv')]

# === Prendi il più recente ===
latest_key = sorted(csv_files)[-1]
print(f"📄 Ultimo file: {latest_key}")

# === Leggilo in pandas ===
obj = s3.get_object(Bucket=BUCKET, Key=latest_key)
df = pd.read_csv(BytesIO(obj['Body'].read()))

print(df.head())
print(df.columns)