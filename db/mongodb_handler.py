from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError


class MongoDBHandler:
    def __init__(self, config, use_persistent_connection=False):
        self.host = config['host']
        self.port = config['port']
        self.database = config['database']
        self.use_persistent_connection = use_persistent_connection
        self.client = None
        self.db = None

        if self.use_persistent_connection:
            self._connect()

    def _connect(self):
        """Establish a connection to the MongoDB server."""
        if not self.client:
            self.client = MongoClient(host=self.host, port=self.port)
            self.db = self.client[self.database]

    def _get_connection(self):
        """Ensure a connection is available."""
        if not self.use_persistent_connection:
            self._connect()

    def _close_connection(self):
        """Close the connection if not using persistent mode."""
        if not self.use_persistent_connection and self.client:
            self.client.close()
            self.client = None
            self.db = None

    def close_persistent_connection(self):
        """Close the persistent connection."""
        if self.use_persistent_connection and self.client:
            self.client.close()
            self.client = None
            self.db = None

    def create_mongo_db(self):
        """Delete and recreate the MongoDB database."""
        try:
            self._get_connection()
            if self.database in self.client.list_database_names():
                self.client.drop_database(self.database)
                print(f"MongoDB database '{self.database}' deleted successfully.")
            self.db = self.client[self.database]  # Reinitialize the database
            print(f"MongoDB database '{self.database}' created successfully.")
        except PyMongoError as e:
            print(f"Error processing MongoDB database: {e}")
        finally:
            self._close_connection()

    def initialize_collection(self, collection_name):
        """Delete a collection if it exists and reinitialize it."""
        try:
            self._get_connection()
            if collection_name in self.db.list_collection_names():
                self.db[collection_name].drop()
                print(f"Collection '{collection_name}' dropped.")
            print(f"Collection '{collection_name}' initialized.")
        except PyMongoError as e:
            print(f"Error initializing collection: {e}")
        finally:
            self._close_connection()

    def insert_one(self, collection_name, document):
        """Insert a single document into a collection."""
        try:
            self._get_connection()
            self.db[collection_name].insert_one(document)
        except PyMongoError as e:
            print(f"Error inserting one document: {e}")
        finally:
            self._close_connection()

    def insert_many(self, collection_name, documents):
        """Insert multiple documents into a collection."""
        try:
            self._get_connection()
            self.db[collection_name].insert_many(documents)
            # print(f"Inserted {len(documents)} documents into '{collection_name}'.")
        except PyMongoError as e:
            print(f"Error inserting many documents: {e}")
        finally:
            self._close_connection()

    def update_one(self, collection_name, filter_query, update_query):
        """Update a single document in a collection."""
        try:
            self._get_connection()
            result = self.db[collection_name].update_one(filter_query, update_query)
            print(f"Updated {result.modified_count} document in '{collection_name}'.")
        except PyMongoError as e:
            print(f"Error updating one document: {e}")
        finally:
            self._close_connection()

    def update_many(self, collection_name, filter_query, update_query):
        """Update multiple documents in a collection."""
        try:
            self._get_connection()
            result = self.db[collection_name].update_many(filter_query, update_query)
            print(f"Updated {result.modified_count} documents in '{collection_name}'.")
        except PyMongoError as e:
            print(f"Error updating many documents: {e}")
        finally:
            self._close_connection()

    def delete_one(self, collection_name, filter_query):
        """Delete a single document from a collection."""
        try:
            self._get_connection()
            result = self.db[collection_name].delete_one(filter_query)
            print(f"Deleted {result.deleted_count} document from '{collection_name}'.")
        except PyMongoError as e:
            print(f"Error deleting one document: {e}")
        finally:
            self._close_connection()

    def delete_many(self, collection_name, filter_query):
        """Delete multiple documents from a collection."""
        try:
            self._get_connection()
            result = self.db[collection_name].delete_many(filter_query)
            print(f"Deleted {result.deleted_count} documents from '{collection_name}'.")
        except PyMongoError as e:
            print(f"Error deleting many documents: {e}")
        finally:
            self._close_connection()

    def query_one_field(self, collection_name, field, value, use_index=False):
        """Query documents based on one field."""
        try:
            self._get_connection()
            if use_index:
                self.create_single_field_index(collection_name, field)
            results = self.db[collection_name].find({field: value})
            return list(results)
        except PyMongoError as e:
            print(f"Error querying on one field: {e}")
            return []
        finally:
            self._close_connection()

    def query_multiple_fields(self, collection_name, filter_query):
        """Query documents based on multiple fields."""
        try:
            self._get_connection()
            results = self.db[collection_name].find(filter_query)
            return list(results)
        except PyMongoError as e:
            print(f"Error querying on multiple fields: {e}")
            return []
        finally:
            self._close_connection()

    def create_single_field_index(self, collection_name, field, order=ASCENDING):
        """Create an index on a single field."""
        try:
            self._get_connection()
            self.db[collection_name].create_index([(field, order)])
            print(f"Index created on field '{field}' in collection '{collection_name}'.")
        except PyMongoError as e:
            print(f"Error creating single field index: {e}")
        finally:
            self._close_connection()

    def create_compound_index(self, collection_name, fields, orders=None):
        """Create a compound index on multiple fields."""
        try:
            self._get_connection()
            if orders is None:
                orders = [ASCENDING] * len(fields)
            index_spec = [(field, order) for field, order in zip(fields, orders)]
            self.db[collection_name].create_index(index_spec)
            print(f"Compound index created on fields {fields} in collection '{collection_name}'.")
        except PyMongoError as e:
            print(f"Error creating compound index: {e}")
        finally:
            self._close_connection()

    def list_indexes(self, collection_name):
        """List all indexes in a collection."""
        try:
            self._get_connection()
            indexes = self.db[collection_name].index_information()
            for name, details in indexes.items():
                print(f"Index: {name}, Details: {details}")
        except PyMongoError as e:
            print(f"Error listing indexes: {e}")
        finally:
            self._close_connection()

    def is_empty(self, collection_name):
        """Check if a MongoDB collection is empty."""
        try:
            self._get_connection()
            count = self.db[collection_name].count_documents({})
            return count == 0
        except PyMongoError as e:
            print(f"Error checking if MongoDB collection '{collection_name}' is empty: {e}")
            return False
        finally:
            if not self.use_persistent_connection:
                self._close_connection()
