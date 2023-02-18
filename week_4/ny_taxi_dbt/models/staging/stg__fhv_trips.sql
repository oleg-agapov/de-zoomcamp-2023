{{
    config(materialized='view')
}}

select
    'fhv' as service_type,
    cast(dispatching_base_num as string) as dispatching_base_num,
    
    cast(pickup_datetime as datetime) as pickup_datetime,
    cast(dropOff_datetime as datetime) as dropoff_datetime,
    
    cast(PUlocationID as integer) as pickup_location_id,
    cast(DOlocationID as integer) as dropoff_location_id,
    
    cast(SR_Flag as string) as sr_flag,
    cast(Affiliated_base_number as string) as affiliated_base_number

from {{ source('raw', 'fhv_trips') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}
limit 100
{% endif %}
