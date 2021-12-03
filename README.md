# epc-ppd-address-matching

Match addresses in the [Energy Performance Certificates](https://epc.opendatacommunities.org/) (EPC) and [HM Land Registry Price Paid Data](https://www.gov.uk/government/collections/price-paid-data) (PPD).

## Installation

You need `pipenv`.

1. Clone this repo.
2. `pipenv sync --dev` to install dependencies.
3. If you're a CNZ colleague and downloading the data from our data warehouse, `cp .env.template .env` and fill in the blanks.
4. `pipenv run pytest` to run tests.

Alternatively, if you are familiar with Docker you can use the included Dockerfile to build an image and match addresses in a container.

## Data Requirements

You need the EPC and PPD addresses in Parquet format.
At CNZ we use BigQuery and dbt to transform data.
We hope to make our dbt sources and models for the EPC and PPD datasets public soon.
Until then, you need to download each dataset and produce Parquet files with the columns below, e.g. using [`pandas.DataFrame.to_parquet`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_parquet.html).

PPD:

```
ppd_id
primary_addressable_object_name
secondary_addressable_object_name
street
town_city
postcode
```

EPC:

```
epc_id
address_line_1
address_line_2
address_line_3
post_town
postcode
```

`ppd_id` and `epc_id` must uniquely identify each address record across **both** datasets.
You can use whatever identifier you want.
We use the MD5 hash of each record in JSON format so that we can join the matches back to our data warehouse.
In BigQuery, this is `md5(to_json_string(addresses))`.

If you're a CNZ colleague, you can download the data from BigQuery.

## Matching Addresses

```
python -m matching epc_addresses/ ppd_addresses/ matches/
```

On multicore machines you can get much faster performance by matching addresses in parallel.
You need to partition the data using `python -m matching.partition` and have [GNU Parallel](https://www.gnu.org/software/parallel/) installed.

We use [`./scripts/container_run.sh`](https://github.com/centrefornetzero/epc-ppd-address-matching/blob/main/scripts/container_run.sh) to extract and partition data, match records and finally load the matches back into BigQuery.
A [Github Action](https://github.com/centrefornetzero/epc-ppd-address-matching/blob/main/.github/workflows/container.yaml) runs this in a container on GCP.

# Acknowledgements

> Tange, O. (2021, November 22). GNU Parallel 20211122 ('Peng Shuai'). Zenodo. https://doi.org/10.5281/zenodo.5719513

Reminder for future us: email Ole Tange a copy of any articles we publish as a result of this work.
