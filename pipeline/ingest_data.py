import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

DATASET_CONFIG = {
    "yellow_taxi_data": {
        "url": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz",  
        "dtype": {
            "VendorID": "Int64",
            "passenger_count": "Int64",
            "trip_distance": "float64",
            "RatecodeID": "Int64",
            "store_and_fwd_flag": "string",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "payment_type": "Int64",
            "fare_amount": "float64",
            "extra": "float64",
            "mta_tax": "float64",
            "tip_amount": "float64",
            "tolls_amount": "float64",
            "improvement_surcharge": "float64",
            "total_amount": "float64",
            "congestion_surcharge": "float64"
        },
        "parse_dates":[
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime"
        ],
        "chunksize": 100000,
        "table_name": "yellow_taxi_data"
    },
    "zones": {
        "url": "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv",
        "dtype": {
            "LocationID": "Int64",
            "Borough": "string",
            "Zone": "string",
            "service_zone": "string"
        },
        "parse_dates": None,
        "chunksize": None,
        "table_name": "zones"
    }
}

@click.command()
@click.option('--year', default=2021, type=int, help='Year to ingest')
@click.option('--month', default=1, type=int, help='Month to ingest')
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_password', default='root', help='PostgreSQL password')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default=5432, help='PostgreSQL port')
def run(year, month, pg_user, pg_password, pg_db, pg_host, pg_port):    
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    for dataset_name, dataset_config in DATASET_CONFIG.items():
        if dataset_name == "yellow_taxi_data":
            url = dataset_config["url"].format(year=year, month=month)
        else:
            url = dataset_config["url"]

        dtype = dataset_config["dtype"]
        parse_dates = dataset_config["parse_dates"]
        table_name = dataset_config["table_name"]
        chunksize = dataset_config["chunksize"]

        print(f'Ingesting {dataset_name} from {url}...')
            
        df_iter = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunksize
        )

        first=True

        for df_chunk in tqdm(df_iter):
            if first:
                df_chunk.head(0).to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='replace'
                )
                first=False
            df_chunk.to_sql(
                name=table_name,
                con=engine,
                if_exists='append'
            )
            print(len(df_chunk))

if __name__ == '__main__':
    run()