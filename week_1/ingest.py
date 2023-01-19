import pandas as pd
from sqlalchemy import create_engine

pg_url = 'postgresql://root:root@pg_database:5432/ny_taxi'

def load_file(file_path: str, table_name: str, pg_url: str) -> None:
    
    if file_path.endswith('.parquet'):
        df = pd.read_parquet(file_path)
    elif file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        return
    print('Ingesting', file_path)
    df.columns = [c.lower() for c in df.columns] # postgres doesn't like capitals or spaces
    engine = create_engine(pg_url)
    df.to_sql(name=table_name, con=engine, if_exists='replace', chunksize=100000)

load_file('data/yellow_tripdata_2021_01.parquet', 'f_trip', pg_url)
load_file('data/taxi_zone_lookup.csv', 'd_zone', pg_url)
