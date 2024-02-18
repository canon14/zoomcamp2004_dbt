{{ config(materialized='view') }}

with fhv_tripdata as (
  select 
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_location_id,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_location_id,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as store_and_fwd_flag,
    Affiliated_base_number as affiliated_base_number
  from {{ source('staging','nyc_fhv_tripdata_2019') }}
)

select *
from fhv_tripdata
where extract(year from date(pickup_datetime)) = 2019
    and pickup_location_id is not null and dropoff_location_id is not null

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}