{{
    config(schema="production")
}}

select
    cast(LocationID as integer) as location_id,
    Borough as borough,
    Zone as zone,
    replace(service_zone,'Boro','Green') as service_zone
from {{ ref('taxi_zones_lookup') }}
