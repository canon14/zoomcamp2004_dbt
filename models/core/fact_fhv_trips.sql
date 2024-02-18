{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    fhv.dispatching_base_num, 
    fhv.pickup_location_id, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv.pickup_datetime, 
    fhv.dropoff_datetime, 
    fhv.store_and_fwd_flag, 
    fhv.affiliated_base_number, 
from fhv_tripdata fhv
inner join dim_zones as pickup_zone
on fhv.pickup_location_id = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv.dropoff_location_id = dropoff_zone.locationid