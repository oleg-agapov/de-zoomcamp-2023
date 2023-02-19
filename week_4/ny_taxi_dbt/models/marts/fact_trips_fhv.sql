{{
    config(materialized='table')
}}

with trips as (
    select *
    from {{ ref('stg__fhv_trips') }}
),
zones as (
    select *
    from {{ ref('dim_zones') }}
    -- where borough != 'Unknown'
)

select
    'fhv' as service_type,
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pickup_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_location_id,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    sr_flag,
    affiliated_base_number
from trips
left join zones as pickup_zone
    on trips.pickup_location_id = pickup_zone.location_id
left join zones as dropoff_zone
    on trips.dropoff_location_id = dropoff_zone.location_id
where pickup_datetime between '2019-01-01' and '2019-12-31 23:59:59'
