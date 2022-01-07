import os
import sys

from google.cloud import bigquery

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCS_URI = sys.argv[1]

EPC_QUERY = f"""
select distinct
    address_matching_id as epc_id,
    building_reference_number,
    address_line_1,
    address_line_2,
    address_line_3,
    post_town,
    postcode
from `{GCP_PROJECT_ID}.prod_epc.stg_epc__england_wales_certificates`
"""  # you can't parameterize table names


PPD_QUERY = f"""
select distinct
    primary_addressable_object_name,
    secondary_addressable_object_name,
    street,
    town_city,
    postcode
from `{GCP_PROJECT_ID}.prod_ppd.stg_ppd__england_wales_sales`
"""

client = bigquery.Client(project=GCP_PROJECT_ID)

print("Querying EPC table and exporting to GCS...")
epc_query_job = client.query(EPC_QUERY)
epc_query_job.result()
client.extract_table(
    epc_query_job.destination, os.path.join(GCS_URI, "epc_addresess-*.parquet")
).result()

print("Querying PPD table and exporting to GCS...")
ppd_query_job = client.query(PPD_QUERY)
ppd_query_job.result()
client.extract_table(
    ppd_query_job.destination, os.path.join(GCS_URI, "ppd_addresses-*.parquet")
).result()
