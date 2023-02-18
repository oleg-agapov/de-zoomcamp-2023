-- create external tables
create or replace external table
    `booming-landing-375221.ny_taxi_trips.external_fhv_trips`
options (
    format = 'CSV',
    uris = [
        'gs://de-zoomcamp-2023_booming-landing-375221/trips/raw/fhv/fhv_tripdata_2019-*.csv.gz'
    ]
);

create or replace external table
    `booming-landing-375221.ny_taxi_trips.external_green_trips`
options (
    format = 'CSV',
    uris = [
        'gs://de-zoomcamp-2023_booming-landing-375221/trips/raw/green/green_tripdata_2019-*.csv.gz',
        'gs://de-zoomcamp-2023_booming-landing-375221/trips/raw/green/green_tripdata_2020-*.csv.gz'
    ]
);

create or replace external table
    `booming-landing-375221.ny_taxi_trips.external_yellow_trips`
options (
    format = 'CSV',
    uris = [
        'gs://de-zoomcamp-2023_booming-landing-375221/trips/raw/yellow/yellow_tripdata_2019-*.csv.gz',
        'gs://de-zoomcamp-2023_booming-landing-375221/trips/raw/yellow/yellow_tripdata_2020-*.csv.gz'
    ]
);

-- create native tables
create or replace table `booming-landing-375221.ny_taxi_trips.fhv_trips`
as
select * from ny_taxi_trips.external_fhv_trips;

create or replace table `booming-landing-375221.ny_taxi_trips.green_trips`
as
select * from ny_taxi_trips.external_green_trips;

create or replace table `booming-landing-375221.ny_taxi_trips.yellow_trips`
as
select * from ny_taxi_trips.external_yellow_trips;
