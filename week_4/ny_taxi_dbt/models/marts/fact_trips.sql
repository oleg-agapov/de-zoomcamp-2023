{{
    config(materialized='table')
}}

with trips as (
    {{ dbt_utils.union_relations([
        ref('stg__green_trips'),
        ref('stg__yellow_trips')
    ]) }}
),
zones as (
    select *
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    service_type,
    trip_id,
    vendor_id,
    ratecode_id,
    pickup_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_location_id,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type,
    payment_type_description,
    congestion_surcharge
from trips
inner join zones as pickup_zone
    on trips.pickup_location_id = pickup_zone.location_id
inner join zones as dropoff_zone
    on trips.dropoff_location_id = dropoff_zone.location_id
where pickup_datetime between '2019-01-01' and '2020-12-31 23:59:59'
