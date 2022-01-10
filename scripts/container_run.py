import os
import subprocess

from google.cloud import bigquery, storage

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
USER = os.environ["USER"]


bq_client = bigquery.Client(project=GCP_PROJECT_ID, location="EU")
gcs_client = storage.Client(project=GCP_PROJECT_ID)


print("Erasing bucket working directory")

for blob in gcs_client.list_blobs(BUCKET_NAME, prefix=USER):
    blob.delete()


print("Querying EPC table and exporting to GCS")

job_config = bigquery.ExtractJobConfig(destination_format="PARQUET")

epc_query_job = bq_client.query(
    f"""
select distinct
    address_matching_id as epc_id,
    building_reference_number,
    address_line_1,
    address_line_2,
    address_line_3,
    post_town,
    postcode
from `{GCP_PROJECT_ID}.prod_epc.stg_epc__england_wales_certificates`
"""
)  # you can't parameterize table names
epc_query_job.result()
bq_client.extract_table(
    epc_query_job.destination,
    os.path.join("gs://", BUCKET_NAME, USER, "epc_addresses-*.parquet"),
    job_config=job_config,
).result()


print("Querying PPD table and exporting to GCS")

ppd_query_job = bq_client.query(
    f"""
select distinct
    address_matching_id as ppd_id,
    primary_addressable_object_name,
    secondary_addressable_object_name,
    street,
    town_city,
    postcode
from `{GCP_PROJECT_ID}.prod_ppd.stg_ppd__england_wales_sales`
"""
)
ppd_query_job.result()
bq_client.extract_table(
    ppd_query_job.destination,
    os.path.join("gs://", BUCKET_NAME, USER, "ppd_addresses-*.parquet"),
    job_config=job_config,
).result()


print("Downloading extracts from GCS")

os.makedirs("data/epc_addresses", exist_ok=True)
for blob in gcs_client.list_blobs(
    BUCKET_NAME, prefix=os.path.join(USER, "epc_addresses")
):
    filename = os.path.split(blob.name)[-1]
    blob.download_to_filename(f"data/epc_addresses/{filename}")

os.makedirs("data/ppd_addresses", exist_ok=True)
for blob in gcs_client.list_blobs(
    BUCKET_NAME, prefix=os.path.join(USER, "ppd_addresses")
):
    filename = os.path.split(blob.name)[-1]
    blob.download_to_filename(f"data/ppd_addresses/{filename}")


print("Partitioning data")

subprocess.run(
    [
        "python",
        "-m",
        "matching.partition",
        "data/epc_addresses",
        "data/partitioned_epc_addresses",
    ]
)
subprocess.run(
    [
        "python",
        "-m",
        "matching.partition",
        "data/ppd_addresses",
        "data/partitioned_ppd_addresses",
    ]
)


print("Matching records...")

os.makedirs("data/matches", exist_ok=True)
subprocess.run(
    (
        'parallel --link --tagstring "Job {#}"'
        ' python -m matching --cluster-prefix "{#}-" {} "data/matches/matches-{#}.parquet"'
        ' ::: "$(ls -d data/partitioned_epc_addresses/**)"'
        ' ::: "$(ls -d data/partitioned_ppd_addresses/**)"'
    ),
    executable="/bin/bash",
    shell=True,
    check=True,
)


print("Uploading matches to GCS")

bucket = storage.Bucket(gcs_client, BUCKET_NAME)
match_dir = "data/matches"
for file in os.listdir(match_dir):
    blob_name = os.path.split(file)[-1]
    blob = bucket.blob(os.path.join(USER, blob_name))
    blob.upload_from_filename(os.path.join(match_dir, file))


print("Loading into BigQuery")

table_id = "src_epc_ppd_address_matching.matches"
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
)
load_job = bq_client.load_table_from_uri(
    os.path.join("gs://", BUCKET_NAME, USER, "matches-*.parquet"),
    table_id,
    job_config=job_config,
    location="EU",
)
load_job.result()

table = bq_client.get_table(table_id)
print(f"Loaded {table.num_rows} rows into {table_id}")
