from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, OperationalError
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import MetaData, Table

from database.creds import creds


class PostgresClient:

    def __init__(self, dbname, user, password, host='localhost', port=5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.engine = self._create_engine()

    def _create_engine(self):
        return create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        )


    def read(self, query, params=None):
        """
        Execute a SELECT query and return the result as a pandas DataFrame.
        """
        try:
            return pd.read_sql_query(text(query), self.engine, params=params)
        except (ProgrammingError, OperationalError) as e:
            print(f"Database error: {e}")
            return None

    def write(self, df, table_name, if_exists='append', index=True, on_conflict='replace'):
        """
        Write a pandas DataFrame to a table. Handles id collision based on 'on_conflict' parameter.
        if_exists: {'fail', 'replace', 'append'}
        on_conflict: None, 'replace', or 'ignore'. If set, will use PostgreSQL ON CONFLICT clause for id collision.
        """
        # Check if table exists
        if not self.engine.dialect.has_table(self.engine.connect(), table_name):
            # Table does not exist, create it

            # Always use lowercase 'id' for index_label and primary key
            df.to_sql(table_name, self.engine, if_exists='fail', index=index, index_label='id')
            self.set_table_columns_not_null(table_name)
            self.set_primary_key_id(table_name)

            print(f"Table '{table_name}' did not exist and was created from DataFrame.")
            return

        if on_conflict is None or if_exists != 'append':
            # Use default pandas to_sql behavior
            try:
                df.to_sql(table_name, self.engine, if_exists=if_exists, index=index)
                print(f"Table '{table_name}' written from DataFrame.")
            except ValueError as e:
                if 'already exists' in str(e):
                    print(f"Table '{table_name}' already exists.")
                else:
                    raise
            except (ProgrammingError, OperationalError) as e:
                print(f"Database error: {e}")
        else:
            # Use ON CONFLICT for id collision handling (only works with if_exists='append')
            # This requires manual insert using SQLAlchemy Table object
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=self.engine)
            records = []
            for idx, row in df.iterrows():
                data = row.to_dict()
                data['id'] = idx  # Ensure the index is included as 'id'
                records.append(data)

            with self.engine.begin() as conn:
                stmt = insert(table)
                if on_conflict == 'replace':
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['id'],
                        set_={k: stmt.excluded[k] for k in df.columns}
                    )
                elif on_conflict == 'ignore':
                    stmt = stmt.on_conflict_do_nothing(index_elements=['id'])

                with self.engine.begin() as conn:
                    conn.execute(stmt, records)
            print(f"Table '{table_name}' written from DataFrame with on_conflict='{on_conflict}'.")

    def create_table_from_dataframe(self, table_name: str, df, if_exists='append', index=True):
        """
        Create a table from a pandas DataFrame using this PostgresClient.
        if_exists: {'fail', 'replace', 'append'}
        index: whether to write row indices as a column
        """
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=index)
            print(f"Table '{table_name}' created from DataFrame.")
        except ValueError as e:
            if 'already exists' in str(e):
                print(f"Table '{table_name}' already exists.")
            else:
                raise
        except (ProgrammingError, OperationalError) as e:
            print(f"Database error: {e}")

    def set_primary_key_id(self, table_name):
        """
        Alters the given table to set the 'id' column as the primary key.
        If a primary key already exists, this will fail unless it is dropped first.
        """
        alter_sql = f"ALTER TABLE {table_name} ADD PRIMARY KEY (id);"
        with self.engine.begin() as conn:
            conn.execute(text(alter_sql))
        print(f"Primary key set to 'id' for table '{table_name}'.")

    def set_table_columns_not_null(self, table_name):
        """
        Alters the given table to set all columns as NOT NULL.
        Skips columns that cannot be set to NOT NULL due to existing NULL values.
        Each column is altered in its own transaction to avoid aborting the whole block.
        """
        # Reflect table columns
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=self.engine)
        for col in table.columns:
            alter_sql = f"ALTER TABLE {table_name} ALTER COLUMN {col.name} SET NOT NULL;"
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(alter_sql))
            except Exception as e:
                print(f"Could not set NOT NULL on column {col.name}: {e}")
    
    def close(self):
        self.engine.dispose()

    
database_client = PostgresClient(
        dbname=creds.dbname,
        user=creds.user,
        password=creds.password,
        host=creds.host,
        port=creds.port
    )