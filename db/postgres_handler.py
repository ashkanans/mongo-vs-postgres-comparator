import psycopg2
from psycopg2 import sql
from psycopg2.pool import SimpleConnectionPool


class PostgresDBHandler:
    def __init__(self, config, use_persistent_connection=False, use_connection_pooling=False, pool_size=10):
        self.host = config['host']
        self.port = config['port']
        self.user = config['user']
        self.password = config['password']
        self.database = config['database']
        self.use_persistent_connection = use_persistent_connection
        self.use_connection_pooling = use_connection_pooling
        self.pool = None

        if self.use_persistent_connection:
            self.connection = self._connect()
        else:
            self.connection = None

        if self.use_connection_pooling:
            self.pool = SimpleConnectionPool(
                minconn=1,
                maxconn=pool_size,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )

    def _connect(self):
        """Establish a connection to the PostgreSQL database."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def _get_connection(self):
        """Get a connection, either from the pool, persistent, or temporary."""
        if self.use_connection_pooling and self.pool:
            return self.pool.getconn()
        elif self.use_persistent_connection:
            return self.connection
        else:
            return self._connect()

    def _close_connection(self, conn):
        """Close the connection if not using persistent mode or return it to the pool."""
        if self.use_connection_pooling and self.pool:
            self.pool.putconn(conn)
        elif not self.use_persistent_connection:
            conn.close()

    def close_persistent_connection(self):
        """Close the persistent connection if it is open."""
        if self.use_persistent_connection and self.connection:
            self.connection.close()
            self.connection = None

    def close_connection_pool(self):
        """Close all connections in the connection pool."""
        if self.use_connection_pooling and self.pool:
            self.pool.closeall()
            self.pool = None
            print("Connection pool closed successfully.")

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

            # Check if the database exists and drop it
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
            print(f"Error during database creation: {e}")
            try:
                # Connect to the default database to perform cleanup
                conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    dbname="postgres"
                )
                conn.autocommit = True
                cursor = conn.cursor()

                # Drop all tables in case of an error
                cleanup_conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    dbname=self.database  # Connect to the target database
                )
                cleanup_conn.autocommit = True
                cleanup_cursor = cleanup_conn.cursor()

                cleanup_cursor.execute(
                    """
                    DO $$ BEGIN
                        EXECUTE (
                            SELECT string_agg('DROP TABLE IF EXISTS ' || tablename || ' CASCADE;', ' ')
                            FROM pg_tables
                            WHERE schemaname = 'public'
                        );
                    END $$;
                    """
                )
                print("All tables dropped successfully.")

                cleanup_cursor.close()
                cleanup_conn.close()
                cursor.close()
                conn.close()
            except Exception as cleanup_error:
                print(f"Error during cleanup: {cleanup_error}")

    def create_reviews_table(self):
        """Create the `reviews` table with an `id` column as the primary key."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS reviews (
                id SERIAL PRIMARY KEY,
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
            print("Table `reviews` created successfully with an `id` column as the primary key.")
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
        finally:
            return len(records)

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

    def update_one(self, update_query):
        """Update a single record in the `reviews` table."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(update_query)
            conn.commit()
            # print("Updated one record in `reviews`.")
            cursor.close()
            self._close_connection(conn)
        except Exception as e:
            print(f"Error updating one record: {e}")

    def update_many_bulk(self, bulk_queries):
        """Update multiple records in bulk in the `reviews` table using a single query."""
        ids = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Create the `WHERE` clause with `OR` filters for the bulk IDs
            ids = [query["filter_query"][0] for query in bulk_queries]
            ids_placeholder = ', '.join(map(str, ids))
            update_query = f"UPDATE reviews SET score = score + 0.123 WHERE id IN ({ids_placeholder})"

            # Execute the bulk update as a single query
            cursor.execute(update_query)
            conn.commit()
            # print(f"Executed bulk update for {len(ids)} records in `reviews`.")
            cursor.close()
            self._close_connection(conn)
        except Exception as e:
            print(f"Error updating many records in bulk: {e}")
        finally:
            return len(ids)

    def delete_one(self, record_id):
        """Delete a single record from the `reviews` table."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM reviews WHERE id = %s", (record_id,))
            conn.commit()
            # print(f"Deleted record with id {record_id} in `reviews`.")
            cursor.close()
            self._close_connection(conn)
        except Exception as e:
            print(f"Error deleting record with id {record_id}: {e}")

    def delete_many_bulk(self, bulk_ids):
        """Delete multiple records in bulk from the `reviews` table."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Construct the `WHERE` clause using `IN` for bulk deletion
            ids_placeholder = ', '.join(map(str, bulk_ids))
            query = f"DELETE FROM reviews WHERE id IN ({ids_placeholder})"
            cursor.execute(query)

            conn.commit()
            # print(f"Deleted {len(bulk_ids)} records in `reviews`.")
            cursor.close()
            self._close_connection(conn)
        except Exception as e:
            print(f"Error deleting records in bulk: {e}")
        finally:
            return len(bulk_ids)

    def get_all_review_ids(self):
        """Retrieve all IDs from the `reviews` table."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            query = "SELECT id FROM reviews;"
            cursor.execute(query)

            # Fetch all rows (IDs will be returned as a list of tuples)
            ids = cursor.fetchall()

            # Convert list of tuples to a flat list of IDs
            id_list = [row[0] for row in ids]

            print(f"Retrieved {len(id_list)} IDs from the `reviews` table.")

            cursor.close()
            self._close_connection(conn)
            return id_list
        except Exception as e:
            print(f"Error retrieving IDs from `reviews`: {e}")
            return []
