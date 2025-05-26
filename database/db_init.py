# Script to initialize the NBA database and create tables from pandas DataFrames
import psycopg2
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from .db_client import PostgresClient

def create_database_if_not_exists(dbname, user, password, host='localhost', port=5432):
    # Connect to the default 'postgres' database to check/create the target db
    conn = psycopg2.connect(dbname='postgres', user=user, password=password, host=host, port=port)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
    exists = cur.fetchone()
    if not exists:
        cur.execute(f'CREATE DATABASE {dbname}')
        print(f"Database '{dbname}' created.")
    else:
        print(f"Database '{dbname}' already exists.")
    cur.close()
    conn.close()

def create_table_from_dataframe(client: PostgresClient, table_name: str, df: pd.DataFrame, if_exists='fail', index=False):
    raise NotImplementedError("This function has been moved to PostgresClient.create_table_from_dataframe. Use that method instead.")
