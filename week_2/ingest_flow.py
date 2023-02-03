import datetime
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(
	retries=1,
	log_prints=True,
	cache_key_fn=task_input_hash,
	cache_expiration=datetime.timedelta(hours=1)
)
def extract_data(url: str) -> pd.DataFrame:
	if url.endswith('.csv') or url.endswith('.csv.gz'):
		df = pd.read_csv(url)
	elif url.endswith('.parquet'):
		df = pd.read_parquet(url)
	df.columns = [c.lower() for c in df.columns]
	print(df.head())
	return df


@task(log_prints=True)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
	date_cols = [x for x in df.columns if 'date' in x]
	for col in date_cols:
		df[col] = pd.to_datetime(df[col])
	print(df.dtypes)
	return df


@task()
def load_data(df: pd.DataFrame, table_name: str) -> None:
	# connection_block = SqlAlchemyConnector.load("pg-database")
	# with connection_block.get_connection(begin=False) as engine:
	# 	df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
	# 	df.to_sql(name=table_name, con=engine, if_exists='append')
	
	from sqlalchemy import create_engine
	pg_url = 'postgresql://root:root@pg_database:5432/ny_taxi'
	engine = create_engine(pg_url)
	df.to_sql(con=engine, name=table_name, if_exists='replace', index=False)


@flow(name="Ingestion script")
def ingest_data(url, table_name):
	raw_df = extract_data(url)
	df = transform_data(raw_df)
	load_data(df, table_name)


if __name__ == "__main__":
	params = dict(
		# url='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz',
		# table_name='yellow_tripdata_2021_01',
		
		url='https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv',
		table_name='taxi_zones',
		
		# url='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz',
		# table_name='green_tripdata_2019_01',
	)
	
	ingest_data(params['url'], table_name=params['table_name'])
