Gireesh Deepak Rajangam Homework
--------------------------------

Tripdata count
```sql
SELECT COUNT(tripid) AS num_trips
FROM dbt_grajangam.stg_yellow_tripdata;

56100630

SELECT COUNT(tripid) AS num_trips
FROM dbt_grajangam.stg_green_tripdata;

SELECT COUNT(tripid) AS num_trips
FROM dbt_grajangam.stg_fhv_tripdata;
37187003
```


```sql
dbt build --select fact_fhv_trips --vars '{'is_test_run': 'false'}'
20:08:59 1 of 1 START sql table model dbt_grajangam.fact_fhv_trips ...................... [RUN]
20:09:07 1 of 1 OK created sql table model dbt_grajangam.fact_fhv_trips ................. [CREATE TABLE (17.9m rows, 1.6 GiB processed) in 7.73s]
```



```sql
dbt build --select +fact_trips+ --vars '{'is_test_run': 'false'}'   ----   
20:07:13 5 of 5 START sql table model dbt_grajangam.fact_trips .......................... [RUN]
20:07:26 5 of 5 OK created sql table model dbt_grajangam.fact_trips ..................... [CREATE TABLE (61.5m rows, 14.2 GiB processed) in 12.63s]
```



dbt generate_surrogate_key link https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#generate_surrogate_key-source 

Ingested Yellow and Green 2019 & 2020, and fhv 2019 data into GCS using Python script located at https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/extras/web_to_gcs.py 

  



Created Bigquery tables using below SQL queries 
```sql
-- Creating external table referring to gcs path for Green 2019 and 2020 
CREATE OR REPLACE EXTERNAL TABLE `vivid-partition-412012.trips_data_all.green_2019_2020_external_table` 
OPTIONS( 
    format = 'PARQUET', 
    uris = ['gs://mage-zoomcamp-gireesh-deepak-r/green_final/*.parquet'] 
    )
```

```sql
--Check External Green table
SELECT * FROM trips_data_all.green_2019_2020_external_table limit 10;
```
```sql
-- Creating a green_tripdata table from Green external table
CREATE OR REPLACE TABLE `vivid-partition-412012.trips_data_all.green_tripdata`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM `trips_data_all.green_2019_2020_external_table`;
```
```sql
-- Check green_tripdata
SELECT * FROM trips_data_all.green_tripdata LIMIT 10;
```

```sql
-- Create Yellow external table for 2019 and 2020
CREATE OR REPLACE EXTERNAL TABLE `vivid-partition-412012.trips_data_all.yellow_2019_2020_external_table` 
OPTIONS( 
    format = 'PARQUET', 
    uris = ['gs://mage-zoomcamp-gireesh-deepak-r/yellow_final/*.parquet'] 
    )
```
```sql
-- Check yellow external table
SELECT * FROM trips_data_all.yellow_2019_2020_external_table limit 10;
```
```sql
-- Creating a yellow_tripdata table
CREATE OR REPLACE TABLE `vivid-partition-412012.trips_data_all.yellow_tripdata`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM `trips_data_all.yellow_2019_2020_external_table`;
```
```sql
-- Check yellow_tripdata table
SELECT * FROM trips_data_all.yellow_tripdata LIMIT 10;
```

```sql
-- Create fhv 2019 external table
CREATE OR REPLACE EXTERNAL TABLE `vivid-partition-412012.trips_data_all.fhv_2019_external_table` 
OPTIONS( 
    format = 'PARQUET', 
    uris = ['gs://mage-zoomcamp-gireesh-deepak-r/fhv_final/*.parquet'] 
    )

-- Check fhv 2019 external table
SELECT * FROM trips_data_all.fhv_2019_external_table LIMIT 10;

-- Creating a fhv_tripdata table
CREATE OR REPLACE TABLE `vivid-partition-412012.trips_data_all.fhv_tripdata`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM `trips_data_all.fhv_2019_external_table`;


--Check fhv_tripdata table
SELECT * FROM trips_data_all.fhv_tripdata LIMIT 10;
```

**Question1**:
dbt build  ----  default limit is 100
dbt build --select <model_name> --vars '{'is_test_run': 'false'}'  -- no limit for this build
dbt build --vars '{'is_test_run':'true'}'  --- limit is 100

```sql
-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```
**Question2**
What is the code that our CI job will run? Where is this code coming from?
Ans: The code from any development branch that has been opened based on main




# Week 4: Analytics Engineering 
Goal: Transforming the data loaded in DWH to Analytical Views developing a [dbt project](taxi_rides_ny/README.md).
[Slides](https://docs.google.com/presentation/d/1xSll_jv0T8JF4rYZvLHfkJXYqUjPtThA/edit?usp=sharing&ouid=114544032874539580154&rtpof=true&sd=true)

# Prerequisites

We will build a project using dbt and a running data warehouse. 

By this stage of the course you should have already: 

- A running warehouse (BigQuery or postgres) 
- A set of running pipelines ingesting the project dataset (week 3 completed)

[Datasets list](https://github.com/DataTalksClub/nyc-tlc-data/)
* Yellow taxi data - Years 2019 and 2020
* Green taxi data - Years 2019 and 2020 
* fhv data - Year 2019. 

Note:
* A quick hack has been shared to load that data quicker, check instructions in [week3/extras](../03-data-warehouse/extras)
* If you recieve an error stating "Permission denied while globbing file pattern." when attemting to run fact_trips.sql this video may be helpful in resolving the issue
 
:movie_camera: [Video](https://www.youtube.com/watch?v=kL3ZVNL9Y4A&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)


# Preparation

## Setting up dbt for using BigQuery (Alternative A - preferred)

You will need to create a dbt cloud account using [this link](https://www.getdbt.com/signup/) and connect to your warehouse [following these instructions](https://docs.getdbt.com/docs/dbt-cloud/cloud-configuring-dbt-cloud/cloud-setting-up-bigquery-oauth). More detailed instructions in [dbt_cloud_setup.md](dbt_cloud_setup.md)

_Optional_: If you feel more comfortable developing locally you could use a local installation of dbt as well. You can follow the [official dbt documentation](https://docs.getdbt.com/dbt-cli/installation) or follow the [dbt with BigQuery on Docker](docker_setup/README.md) guide to setup dbt locally on docker. You will need to install the latest version (1.0) with the BigQuery adapter (dbt-bigquery). 

### Setting up dbt for using Postgres locally (Alternative B)

As an alternative to the cloud, that require to have a cloud database, you will be able to run the project installing dbt locally.
You can follow the [official dbt documentation](https://docs.getdbt.com/dbt-cli/installation) or use a docker image from oficial [dbt repo](https://github.com/dbt-labs/dbt/). You will need to install the latest version (1.0) with the postgres adapter (dbt-postgres).
After local installation you will have to set up the connection to PG in the `profiles.yml`, you can find the templates [here](https://docs.getdbt.com/reference/warehouse-profiles/postgres-profile)


# Content

## :movie_camera: Introduction to analytics engineering

* What is analytics engineering?
* ETL vs ELT 
* Data modeling concepts (fact and dim tables)

 :movie_camera: [Video](https://www.youtube.com/watch?v=uF76d5EmdtU&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=32)

## :movie_camera: What is dbt? 

* Intro to dbt 

 :movie_camera: [Video](https://www.youtube.com/watch?v=4eCouvVOJUw&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=33)

## Starting a dbt project

### :movie_camera: Alternative A: Using BigQuery + dbt cloud
* Starting a new project with dbt init (dbt cloud and core)
* dbt cloud setup
* project.yml

 :movie_camera: [Video](https://www.youtube.com/watch?v=iMxh6s_wL4Q&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=34)
 
### :movie_camera: Alternative B: Using Postgres + dbt core (locally)
* Starting a new project with dbt init (dbt cloud and core)
* dbt core local setup
* profiles.yml
* project.yml

:movie_camera: [Video](https://www.youtube.com/watch?v=1HmL63e-vRs&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=35)

## :movie_camera: dbt models

* Anatomy of a dbt model: written code vs compiled Sources
* Materialisations: table, view, incremental, ephemeral  
* Seeds, sources and ref  
* Jinja and Macros 
* Packages 
* Variables

:movie_camera: [Video](https://www.youtube.com/watch?v=UVI30Vxzd6c&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=36)

_Note: This video is shown entirely on dbt cloud IDE but the same steps can be followed locally on the IDE of your choice_

## :movie_camera: Testing and documenting dbt models
* Tests  
* Documentation 

:movie_camera: [Video](https://www.youtube.com/watch?v=UishFmq1hLM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=37)

_Note: This video is shown entirely on dbt cloud IDE but the same steps can be followed locally on the IDE of your choice_

## Deployment

### :movie_camera: Alternative A: Using BigQuery + dbt cloud
* Deployment: development environment vs production 
* dbt cloud: scheduler, sources and hosted documentation

:movie_camera: [Video](https://www.youtube.com/watch?v=rjf6yZNGX8I&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=38)

### :movie_camera: Alternative B: Using Postgres + dbt core (locally)
* Deployment: development environment vs production 
* dbt cloud: scheduler, sources and hosted documentation

:movie_camera: [Video](https://www.youtube.com/watch?v=Cs9Od1pcrzM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=39)

## :movie_camera: Visualising the transformed data
* Google data studio 
* [Metabase (local installation)](https://www.metabase.com/)

:movie_camera: [Google data studio Video](https://www.youtube.com/watch?v=39nLTs74A3E&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=42) 
 
:movie_camera: [Metabase Video](https://www.youtube.com/watch?v=BnLkrA7a6gM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=43) 

 
## Advanced concepts

 * [Make a model Incremental](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/configuring-incremental-models)
 * [Use of tags](https://docs.getdbt.com/reference/resource-configs/tags)
 * [Hooks](https://docs.getdbt.com/docs/building-a-dbt-project/hooks-operations)
 * [Analysis](https://docs.getdbt.com/docs/building-a-dbt-project/analyses)
 * [Snapshots](https://docs.getdbt.com/docs/building-a-dbt-project/snapshots)
 * [Exposure](https://docs.getdbt.com/docs/building-a-dbt-project/exposures)
 * [Metrics](https://docs.getdbt.com/docs/building-a-dbt-project/metrics)


# Community notes

Did you take notes? You can share them here.

* [Notes by Alvaro Navas](https://github.com/ziritrion/dataeng-zoomcamp/blob/main/notes/4_analytics.md)
* [Sandy's DE learning blog](https://learningdataengineering540969211.wordpress.com/2022/02/17/week-4-setting-up-dbt-cloud-with-bigquery/)
* [Notes by Victor Padilha](https://github.com/padilha/de-zoomcamp/tree/master/week4)
* [Marcos Torregrosa's blog (spanish)](https://www.n4gash.com/2023/data-engineering-zoomcamp-semana-4/)
* [Notes by froukje](https://github.com/froukje/de-zoomcamp/blob/main/week_4_analytics_engineering/notes/notes_week_04.md)
* [Notes by Alain Boisvert](https://github.com/boisalai/de-zoomcamp-2023/blob/main/week4.md)
* [Setting up Prefect with dbt by Vera](https://medium.com/@verazabeida/zoomcamp-week-5-5b6a9d53a3a0)
* [Blog by Xia He-Bleinagel](https://xiahe-bleinagel.com/2023/02/week-4-data-engineering-zoomcamp-notes-analytics-engineering-and-dbt/)
* [Setting up DBT with BigQuery by Tofag](https://medium.com/@fagbuyit/setting-up-your-dbt-cloud-dej-9-d18e5b7c96ba)
* [Blog post by Dewi Oktaviani](https://medium.com/@oktavianidewi/de-zoomcamp-2023-learning-week-4-analytics-engineering-with-dbt-53f781803d3e)
* [Notes from Vincenzo Galante](https://binchentso.notion.site/Data-Talks-Club-Data-Engineering-Zoomcamp-8699af8e7ff94ec49e6f9bdec8eb69fd)
* [Notes from Balaji](https://github.com/Balajirvp/DE-Zoomcamp/blob/main/Week%204/Data%20Engineering%20Zoomcamp%20Week%204.ipynb)
* Add your notes here (above this line)

## Useful links

- [Visualizing data with Metabase course](https://www.metabase.com/learn/visualization/)

