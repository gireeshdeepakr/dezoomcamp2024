{{
    config(
        materialized='table'
    )
}}
with fhv_tripdata as (
    select *, 
        'Fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),
trips_unioned as (
    select * from fhv_tripdata 
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select trips_unioned.tripid, 
    trips_unioned.dispatching_base_num, 
    trips_unioned.service_type,
    trips_unioned.pickup_locationid, 
    trips_unioned.dropoff_locationid, 
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid