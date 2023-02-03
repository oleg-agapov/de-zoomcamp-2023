import pandas as pd
from io import BytesIO
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task()
def get_dataframe(storage_path: str) -> pd.DataFrame:
    gcs = GcsBucket.load('gcs-creds')
    temp_file = BytesIO()
    gcs.download_object_to_file_object(
        from_path=storage_path, 
        to_file_object=temp_file
    )
    temp_file.seek(0)
    df = pd.read_parquet(temp_file)
    print(df.head())
    return df


@task()
def append_to_table(df: pd.DataFrame, target_table: str) -> None:
    gcp = GcpCredentials.load('gcp-creds') 
    df.to_gbq(
        destination_table=target_table,
        project_id='booming-landing-375221',
        credentials=gcp.get_credentials_from_service_account(), 
        chunksize=500_000,
        if_exists='append'
    )


@flow(name='Append rows to BigQuery')
def upload_to_bigquery(storage_path: str, target_table: str) -> int:
    df = get_dataframe(storage_path)
    append_to_table(df, target_table)
    return len(df)

@flow(name='Upload from Cloud Storage to BigQuery table', log_prints=True)
def main_flow(color: str, year: int, months: list) -> None:
    total_rows = 0
    for month in months:
        target_table = f'ny_taxi_trips.{color}_trips'
        storage_path = f'trips/parquet/{color}_tripdata_{year}-{month:02}.parquet'
        new_rows = upload_to_bigquery(storage_path, target_table)
        total_rows += new_rows
    print(total_rows)

if __name__ == '__main__':
    color = 'yellow'
    year = 2021
    month = [1, 2]
    main_flow(color, year, month)
