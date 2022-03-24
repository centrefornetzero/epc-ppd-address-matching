# `epc-ppd-address-matching`

Match addresses in the [Energy Performance Certificates](https://epc.opendatacommunities.org/) (EPC) dataset and [HM Land Registry Price Paid Data](https://www.gov.uk/government/collections/price-paid-data) (PPD).
A simple, but effective, rule-based approach.


## Installation

You need Python 3.9 and [`pipenv`](https://github.com/pypa/pipenv).
If you don't have them, see [our instructions for macOS](https://gist.github.com/tomwphillips/715d4fd452ef5d52b4708c0fc5d4f30f).

1. Clone this repo.
2. `pipenv sync --dev` to install dependencies.
3. If you're a CNZ colleague and downloading the data from our data warehouse, `cp .env.template .env` and fill in the blanks.
4. `pipenv run pytest` to run tests.

Alternatively, if you are familiar with Docker you can use the included Dockerfile to build an image and match addresses in a container.

## Data Requirements

You need the EPC and PPD addresses in Parquet format.
At CNZ we manage EPC and PPD [using dbt in BigQuery](https://github.com/centrefornetzero/domestic-heating-data).
You can use that code to clean up EPC and PPD and transform it into the schema required for matching.
Alternatively you can produce Parquet files with the following schemas however you want (all columns are strings).

### PPD schema

```
ppd_id
primary_addressable_object_name
secondary_addressable_object_name
street
town_city
postcode
```

### EPC schema

```
epc_id
address_line_1
address_line_2
address_line_3
post_town
postcode
```

### Record IDs

`ppd_id` and `epc_id` must uniquely identify each address record across **both** datasets.
You can use whatever identifier you want.
We use the MD5 hash of each record in JSON format so that we can join the matches back to our data warehouse.
In BigQuery, this is `md5(to_json_string(addresses))`.

## Matching Addresses

```
python -m matching epc_addresses/ ppd_addresses/ matches/
```

On multicore machines you can get much faster performance by matching addresses in parallel.
You need to partition the data using `python -m matching.partition` and have [GNU Parallel](https://www.gnu.org/software/parallel/) installed.

We use [`./scripts/container_run.sh`](https://github.com/centrefornetzero/epc-ppd-address-matching/blob/main/scripts/container_run.sh) to extract and partition data, match records and finally load the matches back into BigQuery.
A [Github Action](https://github.com/centrefornetzero/epc-ppd-address-matching/blob/main/.github/workflows/container.yaml) runs this in a container on GCP.

# Acknowledgements

Tange, O. (2021, November 22). GNU Parallel 20211122 ('Peng Shuai'). Zenodo. https://doi.org/10.5281/zenodo.5719513
