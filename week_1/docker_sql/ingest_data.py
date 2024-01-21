#!/usr/bin/env python
# coding: utf-8

import argparse

import pandas as pd
import pyarrow.parquet as pq
import os
from time import time
from sqlalchemy import create_engine, types


# #### Below code doesn't convert the column datatypes to appropriate and gives all columns as datatype text. Should be using the other code block after this block of code

# engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')  
# # Extract and print column names
# column_names = schema.names

# # Create an empty DataFrame with these columns
# empty_df = pd.DataFrame(columns=column_names)

# # Specify the data types using the mapping

# empty_df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# # Iterating over Parquet file in chunks
# i = 1
# for df in parquet_file.iter_batches(batch_size=100000):
#     t_start = time()
#     df = df.to_pandas()
#     df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
#     df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
#     df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
#     t_end = time()
#     print(f'Inserted Chunk {i}, time taken- {t_end-t_start}')
#     i+=1


# #### Below code gives the column datatypes according to the conversion


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_file = 'data.parquet'

    os.system(f"wget {url} -O {parquet_file}")

    print("File reading done")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print("Engine created")

    parquet_table = pq.read_table(parquet_file)

    print("Data read into table")

    dtype_mapping = {
        'VendorID': types.Integer,
        'tpep_pickup_datetime': types.DateTime(),
        'tpep_dropoff_datetime': types.DateTime(),
        'passenger_count': types.Double,
        'trip_distance': types.Double,
        'RatecodeID': types.Double,
        'store_and_fwd_flag': types.VARCHAR,
        'PULocationID': types.Integer,
        'DOLocationID': types.Integer,
        'payment_type': types.Integer,
        'fare_amount': types.Double,
        'extra':types.Double,
        'mta_tax':    types.Double,
        'tip_amount': types.Double,
        'tolls_amount': types.Double,
        'improvement_surcharge':     types.Double,
        'total_amount':    types.Double,
        'congestion_surcharge':     types.Double,
        'airport_fee':     types.Double,
    }
    # Write the DataFrame to PostgreSQL in batches
    batch_size = 100000
    total_rows = len(parquet_table)
    i = 1
    # Iterate over batches
    for i in range(0, total_rows, batch_size):
        t_start = time()
        batch_df = parquet_table[i:i+batch_size].to_pandas()
        # Manually cast the problematic columns to DateTime
        for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
            batch_df[col] = pd.to_datetime(batch_df[col])

        # Write the DataFrame to PostgreSQL
        batch_df.to_sql(name=table_name, con=engine, if_exists='append', index=False, dtype=dtype_mapping)

        t_end = time()
        print(f'Inserted Chunk {i}, time taken - {t_end - t_start}')
        i += 1
    print("Done inserting into table Yellow taxi data")

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest Parquet file to Postgres')

    #user, password, host, port, database name, table name,
    #url of parquet file

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db for postgres')
    parser.add_argument('--table_name', help='table name for postgres where we will write the data')
    parser.add_argument('--url', help='url of parquet file')

    args = parser.parse_args()
    # print(args.accumulate(args.integers))
    main(args)