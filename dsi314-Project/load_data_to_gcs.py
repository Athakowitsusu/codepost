import json
import os

from google.cloud import bigquery, storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "dsi314"
location = "asia-southeast1"

# Prepare and Load Credentials to Connect to GCP Services
keyfile_gcs = "dsi-gcs-to-bigquery-9475d17d9b2b.json"
service_account_info_gcs = json.load(open(keyfile_gcs))
credentials_gcs = service_account.Credentials.from_service_account_info(
    service_account_info_gcs
)

# keyfile_bigquery = "YOUR_KEYFILE_PATH_FOR_GCS_TO_BIGQUERY"
# service_account_info_bigquery = json.load(open(keyfile_bigquery))
# credentials_bigquery = service_account.Credentials.from_service_account_info(
#     service_account_info_bigquery
# )

project_id = "DSI-314"

# Load data from Local to GCS
bucket_name = "dsi-314-project"
storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcs,
)
bucket = storage_client.bucket(bucket_name)

data = "table_promotions_pratunam"
file_path = f"{DATA_FOLDER}/{data}.csv"
destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"

# YOUR CODE HERE TO LOAD DATA TO GCS

# # Load data from GCS to BigQuery
# bigquery_client = bigquery.Client(
#     project=project_id,
#     credentials=credentials_bigquery,
#     location=location,
# )
# table_id = f"{project_id}.dsi-314.{data}"
# job_config = bigquery.LoadJobConfig(
#     skip_leading_rows=1,
#     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#     source_format=bigquery.SourceFormat.CSV,
#     autodetect=True,
# )
# job = bigquery_client.load_table_from_uri(
#     f"gs://{bucket_name}/{destination_blob_name}",
#     table_id,
#     job_config=job_config,
#     location=location,
# )
# job.result()

# table = bigquery_client.get_table(table_id)
# print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")