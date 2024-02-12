Gireesh Deepak Rajangam Home work 3
------------------------------------

I used Mage to create a pipline with data loader and data exporter. 
Data loader, loads data from the API "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page" in in a loop and returns the data to pipeline downstream data exporter in parquet format. Data exporter to export green taxi 2022 data to GCS bucket "mage-zoomcamp-gireesh-deepak-r/green_taxi_2022".

Mage Data Loader, Python API block code
-----------------------------------
```Python
import io
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    # Create a date range from 2022-01-01 to 2022-12-31 with day frequency
    daterange = pd.date_range(start='2022-01-01', end='2022-12-31', freq='M')
    #Initialize dataframe
    dataframes = []
    #Loop over the date range
    for date in daterange:
        year = date.year
        month = date.strftime('%m')
        # Construct the API end point url
        api_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet"
        #print(f"api url: {api_url}")
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            data = io.BytesIO(response.content)
            df = pq.read_table(data).to_pandas()
    
        except requests.HTTPError as e:
            print(f"HTPP Error: {e}")

        # Append the dataframe to the list
        dataframes.append(df)
    # Concatenate the dataframes into one
    data = pd.concat(dataframes)
    return data

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
```

Mage Data Exporter Python GCS Block code
--------------------------------------------

```Python
import os
import pyarrow as pa
import pyarrow.parquet as pq
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/dezoomcamp2024.json"
bucket_name = 'mage-zoomcamp-gireesh-deepak-r'
project_id = 'vivid-partition-412012'
table_name = 'green_taxi_2022'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(data, *args, **kwargs) -> None:
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    table = pa.Table.from_pandas(data)
    gcs = pa.fs.GcsFileSystem()
    
    pq.write_to_dataset(
        table,
        root_path=root_path,
        filesystem=gcs
    )

```

Created empty Dataset with name: "ny_green_taxi" in bigquery

Following are Bigquery SQL statements
----------------------------------------

```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `vivid-partition-412012.ny_green_taxi.green_taxi_2022` 
OPTIONS( 
    format = 'PARQUET', 
    uris = ['gs://mage-zoomcamp-gireesh-deepak-r/green_taxi_2022/402aa13fa59544a2b64080d5962fdcb4-0.parquet'] 
    )
```
```sql
-- Check green taxi 2022 trip data
SELECT * FROM ny_green_taxi.green_taxi_2022 limit 10;
```
```sql
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `vivid-partition-412012.ny_green_taxi.green_taxi_2022_partitioned_and_clustered`
PARTITION BY DATE(cleaned_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT *, TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS cleaned_pickup_datetime, TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS cleaned_dropoff_datetime FROM `ny_green_taxi.green_taxi_2022`;
```
```sql
-- Creating a non partition and non cluster table
CREATE OR REPLACE TABLE `vivid-partition-412012.ny_green_taxi.green_taxi_2022_non_partitioned_non_clustered` AS
SELECT *, TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS cleaned_pickup_datetime, TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS cleaned_dropoff_datetime FROM `ny_green_taxi.green_taxi_2022`;
```


```sql
SELECT DISTINCT PULocationID
FROM `vivid-partition-412012.ny_green_taxi.green_taxi_2022_non_partitioned_non_clustered`
WHERE DATE(cleaned_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
```
```sql
SELECT DISTINCT PULocationID
FROM `vivid-partition-412012.ny_green_taxi.green_taxi_2022_partitioned_and_clustered`
WHERE DATE(cleaned_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
```

Home Work
------------
**Question 1:** What is count of records for the 2022 Green Taxi Data??
**Ans:** Number of rows 840,402

**Question 2:** Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
**Ans:** 0 MB for the External Table and 6.41MB for the Materialized Table
```sql
SELECT COUNT(DISTINCT PULocationID) AS unique_PULocationIDs
FROM `vivid-partition-412012.ny_green_taxi.green_taxi_2022`

SELECT COUNT(DISTINCT PULocationID) AS unique_PULocationIDs
FROM `ny_green_taxi.green_taxi_2022_non_partitioned_clustered`
```

**Question 3:** How many records have a fare_amount of 0?
**Ans:** 1622

SELECT COUNT(*) AS zero_fare_records
FROM `vivid-partition-412012.ny_green_taxi.green_taxi_2022`
WHERE fare_amount = 0

SELECT COUNT(*) AS zero_fare_records
FROM `vivid-partition-412012.ny_green_taxi.green_taxi_2022_non_partitioned_clustered`
WHERE fare_amount = 0

**Question 4:** What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
**Ans:** Partition by lpep_pickup_datetime Cluster on PUlocationID

**Question 5:** Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? 
**Ans:** 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

**Question 6:** Where is the data stored in the External Table you created?
**Ans:** GCP Bucket

**Question 7:** It is best practice in Big Query to always cluster your data
**Ans:** True


# Data Warehouse and BigQuery

- [Slides](https://docs.google.com/presentation/d/1a3ZoBAXFk8-EhUsd7rAZd-5p_HpltkzSeujjRGB2TAI/edit?usp=sharing)  
- [Big Query basic SQL](big_query.sql)

# Videos

## Data Warehouse

- [Data Warehouse and BigQuery](https://www.youtube.com/watch?v=jrHljAoD6nM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

## :movie_camera: Partitoning and clustering

- [Partioning and Clustering](https://www.youtube.com/watch?v=jrHljAoD6nM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)  
- [Partioning vs Clustering](https://www.youtube.com/watch?v=-CqXf7vhhDs&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

## :movie_camera: Best practices

- [BigQuery Best Practices](https://www.youtube.com/watch?v=k81mLJVX08w&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

## :movie_camera: Internals of BigQuery

- [Internals of Big Query](https://www.youtube.com/watch?v=eduHi1inM4s&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

## Advanced topics

### :movie_camera: Machine Learning in Big Query

* [BigQuery Machine Learning](https://www.youtube.com/watch?v=B-WtpB0PuG4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
* [SQL for ML in BigQuery](big_query_ml.sql)

**Important links**

- [BigQuery ML Tutorials](https://cloud.google.com/bigquery-ml/docs/tutorials)
- [BigQuery ML Reference Parameter](https://cloud.google.com/bigquery-ml/docs/analytics-reference-patterns)
- [Hyper Parameter tuning](https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-create-glm)
- [Feature preprocessing](https://cloud.google.com/bigquery-ml/docs/reference/standard-sql/bigqueryml-syntax-preprocess-overview)

### :movie_camera: Deploying ML model

- [BigQuery Machine Learning Deployment](https://www.youtube.com/watch?v=BjARzEWaznU&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)
- [Steps to extract and deploy model with docker](extract_model.md)  



# Homework

* [2024 Homework](../cohorts/2024/)


# Community notes

Did you take notes? You can share them here.

* [Notes by Alvaro Navas](https://github.com/ziritrion/dataeng-zoomcamp/blob/main/notes/3_data_warehouse.md)
* [Isaac Kargar's blog post](https://kargarisaac.github.io/blog/data%20engineering/jupyter/2022/01/30/data-engineering-w3.html)
* [Marcos Torregrosa's blog post](https://www.n4gash.com/2023/data-engineering-zoomcamp-semana-3/) 
* [Notes by Victor Padilha](https://github.com/padilha/de-zoomcamp/tree/master/week3)
* [Notes from Xia He-Bleinagel](https://xiahe-bleinagel.com/2023/02/week-3-data-engineering-zoomcamp-notes-data-warehouse-and-bigquery/)
* [Bigger picture summary on Data Lakes, Data Warehouses, and tooling](https://medium.com/@verazabeida/zoomcamp-week-4-b8bde661bf98), by Vera
* [Notes by froukje](https://github.com/froukje/de-zoomcamp/blob/main/week_3_data_warehouse/notes/notes_week_03.md)
* [Notes by Alain Boisvert](https://github.com/boisalai/de-zoomcamp-2023/blob/main/week3.md)
* [Notes from Vincenzo Galante](https://binchentso.notion.site/Data-Talks-Club-Data-Engineering-Zoomcamp-8699af8e7ff94ec49e6f9bdec8eb69fd)
* Add your notes here (above this line)
