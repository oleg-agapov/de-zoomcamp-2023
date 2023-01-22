import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine

pg_url = 'postgresql://root:root@pg_database:5432/ny_taxi'

def chunker(df, size):
    return (df[pos:pos + size] for pos in range(0, len(df), size))

def insert_with_progress(df, table_name, con, mode="replace"):
    chunk_size = int(len(df) / 100)
    with tqdm(total=len(df)) as progress_bar:
        for i, _df in enumerate(chunker(df, chunk_size)):
            _df.to_sql(con=con, name=table_name, if_exists=mode, index=False)
            progress_bar.update(chunk_size)


def load_file(file_path: str, table_name: str, pg_url: str) -> None:
    if file_path.endswith('.parquet'):
        df = pd.read_parquet(file_path)
    elif file_path.endswith('.csv') or file_path.endswith('.csv.gz'):
        df = pd.read_csv(file_path)
    else:
        return
    df.columns = [c.lower() for c in df.columns] # postgres doesn't like capitals or spaces
    engine = create_engine(pg_url)
    insert_with_progress(df, table_name, engine)


if __name__ == "__main__":
    # https://github.com/ziritrion/dataeng-zoomcamp/blob/main/1_intro/ingest_data.py
    load_file('data/yellow_tripdata_2021_01.parquet', 'f_trip', pg_url)
    load_file('data/taxi_zone_lookup.csv', 'd_zone', pg_url)
