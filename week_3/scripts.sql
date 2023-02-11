-- create external table from .csv files on Cloud Storage 
create or replace external table 
    `booming-landing-375221.ny_taxi_trips.external_fhv_trips`
options (
    format = 'CSV',
    uris = [
        'gs://de-zoomcamp-2023_booming-landing-375221/trips/raw/fhv/fhv_tripdata_2019-*.csv'
    ]
);

select * from ny_taxi_trips.external_fhv_trips limit 10;


-- create native table from external table without partitioning and clustering
create or replace table `booming-landing-375221.ny_taxi_trips.fhv_trips` as
select * from ny_taxi_trips.external_fhv_trips;


-- question 1

select count(1) from `ny_taxi_trips.fhv_trips`;


-- question 2

select count(distinct Affiliated_base_number) 
from `ny_taxi_trips.external_fhv_trips`;

select count(distinct Affiliated_base_number) 
from `ny_taxi_trips.fhv_trips`;

-- question 3

select count(1)
from `ny_taxi_trips.fhv_trips`
where PUlocationID is null and DOlocationID is null

-- question 5

select count(distinct Affiliated_base_number)
from `ny_taxi_trips.fhv_trips`
where pickup_datetime between '2019-03-01' and '2019-03-31 23:59:59';


create or replace table `ny_taxi_trips.fhv_trips_partitioned`
partition by DATE(pickup_datetime)
cluster by Affiliated_base_number
as
select * from `ny_taxi_trips.external_fhv_trips`;


select count(distinct Affiliated_base_number)
from `ny_taxi_trips.fhv_trips_partitioned`
where pickup_datetime between '2019-03-01' and '2019-03-31 23:59:59';

-- question 8

create or replace external table 
    `booming-landing-375221.ny_taxi_trips.external_fhv_trips_parquet`
options (
    format = 'PARQUET',
    uris = [
        'gs://de-zoomcamp-2023_booming-landing-375221/trips/parquet/fhv/fhv_tripdata_2019-*.parquet'
    ]
);
