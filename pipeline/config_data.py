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