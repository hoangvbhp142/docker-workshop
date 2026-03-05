import click
import pandas as pd
from sqlalchemy import create_engine, text
from tqdm.auto import tqdm

@click.command()
@click.option('--year', default=2021, type=int, help='Year to ingest')
@click.option('--month', default=1, type=int, help='Month to ingest')
@click.option('--pg_user', default='root', help='PostgreSQL user')
@click.option('--pg_password', default='root', help='PostgreSQL password')
@click.option('--pg_db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--pg_host', default='localhost', help='PostgreSQL host')
@click.option('--pg_port', default=5432, help='PostgreSQL port')
def ingest_trip(year, month, pg_user, pg_password, pg_db, pg_host, pg_port):
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    ingest_zone(engine)

    file_name = f"yellow_tripdata_{year}-{month:02d}.csv.gz"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{file_name}"
    
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM ingestion_log WHERE file_name = :file"),
            {"file": file_name}
        ).fetchone()

        if result:
            print("File already ingested. Skipping...")
            return

    with engine.begin() as conn:
        conn.execute(f"CREATE TABLE IF NOT EXISTS zones (LocationID INT PRIMARY KEY, Borough TEXT, Zone TEXT, service_zone TEXT);")

        conn.execute(f"""CREATE TABLE IF NOT EXISTS yellow_taxi_data (
            VendorID INT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count INT,
            trip_distance FLOAT,
            RatecodeID INT,
            store_and_fwd_flag TEXT,
            PULocationID INT,
            DOLocationID INT,
            payment_type INT,
            fare_amount FLOAT,
            extra FLOAT,
            mta_tax FLOAT,
            tip_amount FLOAT,
            tolls_amount FLOAT,
            improvement_surcharge FLOAT,
            total_amount FLOAT,
            congestion_surcharge FLOAT,
            trip_key TEXT PRIMARY KEY
        );""")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS staging_yellow_taxi_data (LIKE yellow_taxi_data INCLUDING DEFAULTS);
        """)

        conn.execute(f"""CREATE TABLE IF NOT EXISTS ingestion_log (
            file_name TEXT PRIMARY KEY,
            loaded_at TIMESTAMP DEFAULT NOW(),
            row_count INT
        );""")

        print("Database tables created or verified successfully.")

    total_rows = 0

    with engine.begin() as conn:

        conn.execute(f"TRUNCATE TABLE staging_yellow_taxi_data;")

        df_iter = pd.read_csv(
            url,
            parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
            iterator=True,
            chunksize=100000
        )

        for df_chunk in tqdm(df_iter):

            df_chunk["trip_key"] = (
                df_chunk["VendorID"].astype(str) + "_" +
                df_chunk["tpep_pickup_datetime"].dt.strftime('%Y%m%d%H%M%S') + "_" +
                df_chunk["tpep_dropoff_datetime"].dt.strftime('%Y%m%d%H%M%S') + "_" +
                df_chunk["PULocationID"].astype(str) + "_" +
                df_chunk["DOLocationID"].astype(str)
            )

            df_chunk.to_sql(
                name="staging_yellow_taxi_data",
                con=conn,
                index=False,
                method='multi',
                if_exists='append'
            )

            total_rows += len(df_chunk)
        
        conn.execute(f"""INSERT INTO yellow_taxi_data
            SELECT *
            FROM staging_yellow_taxi_data
            ON CONFLICT (trip_key) DO NOTHING;""")

        conn.execute(
            text("""
                INSERT INTO ingestion_log (file_name, row_count)
                VALUES (:file, :rows)
            """),
            {"file": file_name, "rows": total_rows}
        )
    
def ingest_zone(engine):
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
    table_name = "zones"
    chunk_size = 100000
    parse_dates = None

    with engine.begin() as conn:

        conn.execute("""
            CREATE TABLE IF NOT EXISTS zones (
                LocationID INT PRIMARY KEY,
                Borough TEXT,
                Zone TEXT,
                service_zone TEXT
            );
        """)

        conn.execute(f"TRUNCATE TABLE {table_name};")

        df_iter = pd.read_csv(
            url,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunk_size
        )

        for df_chunk in tqdm(df_iter):
            df_chunk.to_sql(
                name=table_name,
                con=conn,
                index=False,
                method='multi',
                if_exists='append'
            )

if __name__ == '__main__':
    ingest_trip()