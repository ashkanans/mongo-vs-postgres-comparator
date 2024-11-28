import psycopg2
from psycopg2 import sql


class PostgresDBHandler:
    def __init__(self, config, use_persistent_connection=False):
        self.host = config['host']
        self.port = config['port']
        self.user = config['user']
        self.password = config['password']
        self.database = config['database']
        self.use_persistent_connection = use_persistent_connection
        self.connection = None
        if self.use_persistent_connection:
            self.connection = self._connect()

    def _connect(self):
        """Establish a connection to the PostgreSQL database."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password
        )

    def _get_connection(self):
        """Get the active connection based on the persistence flag."""
        if self.use_persistent_connection:
            return self.connection
        return self._connect()

    def _close_connection(self, conn):
        """Close the connection if not using persistent mode."""
        if not self.use_persistent_connection:
            conn.close()

    def close_persistent_connection(self):
        """Close the persistent connection if it is open."""
        if self.use_persistent_connection and self.connection:
            self.connection.close()
            self.connection = None

    def create_database(self):
        """Delete and recreate the database."""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname="postgres"  # Always connect to the default postgres database
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Drop the database if it exists
            cursor.execute(
                sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s;"),
                [self.database]
            )
            if cursor.fetchone():
                cursor.execute(sql.SQL("DROP DATABASE {};").format(sql.Identifier(self.database)))
                print(f"Database '{self.database}' dropped successfully.")

            # Create the new database
            cursor.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(self.database)))
            print(f"Database '{self.database}' created successfully.")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Error processing database: {e}")

    def create_reviews_table(self):
        """Create the `reviews` table."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS reviews (
                product_id TEXT,
                user_id TEXT,
                profile_name TEXT,
                helpfulness TEXT,
                score FLOAT,
                review_time BIGINT,
                summary TEXT,
                review_text TEXT
            );
            """
            cursor.execute(create_table_sql)
            conn.commit()
            print("Table `reviews` created successfully.")
            cursor.close()
            self._close_connection(conn)
        except Exception as e:
            print(f"Error creating table `reviews`: {e}")

    def insert_one(self, record):
        """Insert a single record into the `reviews` table."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO reviews (
                product_id, user_id, profile_name, helpfulness, score, review_time, summary, review_text
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                record.get("product/productId"),
                record.get("review/userId"),
                record.get("review/profileName"),
                record.get("review/helpfulness"),
                float(record.get("review/score", 0)),
                int(record.get("review/time", 0)),
                record.get("review/summary"),
                record.get("review/text")
            ))
            conn.commit()
            cursor.close()
            self._close_connection(conn)
        except Exception as e:
            print(f"Error inserting record into PostgreSQL: {e}")

    def insert_many(self, records):
        """Insert multiple records into the `reviews` table."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            insert_query = """
            INSERT INTO reviews (
                product_id, user_id, profile_name, helpfulness, score, review_time, summary, review_text
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            values = [
                (
                    record.get("product/productId"),
                    record.get("review/userId"),
                    record.get("review/profileName"),
                    record.get("review/helpfulness"),
                    float(record.get("review/score", 0)),
                    int(record.get("review/time", 0)),
                    record.get("review/summary"),
                    record.get("review/text")
                )
                for record in records
            ]
            cursor.executemany(insert_query, values)
            conn.commit()
            # print(f"Inserted {len(records)} records into `reviews`.")
            cursor.close()
            self._close_connection(conn)
        except Exception as e:
            print(f"Error inserting multiple records: {e}")

    def update_one(self, filter_query, update_query):
        """Update a single record in the `reviews` table."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(update_query, filter_query)
            conn.commit()
            print("Updated one record in `reviews`.")
            cursor.close()
            self._close_connection(conn)
        except Exception as e:
            print(f"Error updating record: {e}")

    def create_single_column_index(self, table, column):
        """Create an index on a single column."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            index_name = f"{table}_{column}_idx"
            cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(index_name),
                sql.Identifier(table),
                sql.Identifier(column)
            ))
            conn.commit()
            print(f"Index '{index_name}' created on column '{column}' in table '{table}'.")
            cursor.close()
            self._close_connection(conn)
        except Exception as e:
            print(f"Error creating single-column index: {e}")

    def create_compound_index(self, table, columns):
        """Create a compound index on multiple columns."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            index_name = f"{table}_{'_'.join(columns)}_idx"
            columns_sql = sql.SQL(', ').join(sql.Identifier(col) for col in columns)
            cursor.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(index_name),
                sql.Identifier(table),
                columns_sql
            ))
            conn.commit()
            print(f"Compound index '{index_name}' created on columns '{', '.join(columns)}' in table '{table}'.")
            cursor.close()
            self._close_connection(conn)
        except Exception as e:
            print(f"Error creating compound index: {e}")

    def is_empty(self, table_name):
        """Check if a PostgreSQL table is empty."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            cursor.close()
            self._close_connection(conn)
            return count == 0
        except Exception as e:
            print(f"Error checking if PostgreSQL table '{table_name}' is empty: {e}")
            return False
