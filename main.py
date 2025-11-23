import base64
import json
import csv
import datetime
import tempfile
from google.cloud import bigquery, storage
import pytz

def function1(event, context):
    # Decode Pub/Sub message
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        data = json.loads(pubsub_message)
    except Exception as e:
        print(f"Error decoding Pub/Sub message: {e}")
        return

    file_name = data.get("name")
    bucket_name = data.get("bucket")
    print(f"Received event for file: {file_name} in bucket: {bucket_name}")

    if bucket_name != "manola11" or file_name != "table1.csv":
        print(f"Skipping file {file_name} from bucket {bucket_name}")
        return

    storage_client = storage.Client()
    bq_client = bigquery.Client()

    dataset_id = "raw"
    table_id = "table1"
    table_ref = bq_client.dataset(dataset_id).table(table_id)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    with tempfile.NamedTemporaryFile(mode="w+b", delete=False) as temp_file:
        blob.download_to_filename(temp_file.name)
        temp_file_path = temp_file.name

    print(f"Downloaded {file_name} to {temp_file_path}")

    # Read CSV and add Thailand timestamp
    rows = []
    thailand_tz = pytz.timezone("Asia/Bangkok")
    try:
        with open(temp_file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["id"] = int(row["id"])
                row["age"] = int(row["age"])
                row["salary"] = int(row["salary"])
                
                # Current Thailand time
                thailand_time = datetime.datetime.now(thailand_tz)
                # Convert to UTC for BigQuery TIMESTAMP
                row["created_at"] = thailand_time.astimezone(pytz.UTC).isoformat()
                
                rows.append(row)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    if not rows:
        print("No rows to load.")
        return

    schema = [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("age", "INTEGER"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("salary", "INTEGER"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        load_job = bq_client.load_table_from_json(rows, table_ref, job_config=job_config)
        load_job.result()
        print(f"✅ Loaded {len(rows)} rows into {dataset_id}.{table_id}")
    except Exception as e:
        print(f"❌ Error loading into BigQuery: {e}")
        raise
