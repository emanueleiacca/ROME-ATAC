import os
import subprocess
import boto3

# 1. Lancia lo script convert_to_parquet.py
print("▶️ Eseguo convert_to_parquet.py...")
subprocess.run(["python", "convert_to_parquet.py"], check=True)

# 2. Esegui query MSCK su Athena
print("🔄 Eseguo MSCK REPAIR TABLE...")

athena = boto3.client("athena", region_name="eu-north-1")

DATABASE = "gtfs"
S3_OUTPUT = "s3://gtfs-data-bucket-emanuele/athena-results/"
REPAIR_QUERY = "MSCK REPAIR TABLE vehicle_positions_parquet"

response = athena.start_query_execution(
    QueryString=REPAIR_QUERY,
    QueryExecutionContext={"Database": DATABASE},
    ResultConfiguration={"OutputLocation": S3_OUTPUT}
)

execution_id = response["QueryExecutionId"]
print(f"✅ MSCK Query started with ID: {execution_id}")
