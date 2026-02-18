import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
from config_data import DATASET_CONFIG as DATASET

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

    for dataset_name, dataset_config in DATASET.items():
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