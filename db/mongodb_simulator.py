import time

from tqdm import tqdm

from db.mongodb_handler import MongoDBHandler


class MongoSimulator:
    def __init__(self, config, use_persistent_connection=False):
        self.handler = MongoDBHandler(config, use_persistent_connection)

    def setup(self):
        """Set up the database and collection for testing."""
        print("Setting up MongoDB database and collection...")
        self.handler.create_mongo_db()
        self.handler.initialize_collection('reviews')
        print("MongoDB setup complete.")

    def test_insertion(self, records):
        """Test insertion of records into MongoDB with a progress bar."""
        print("Testing MongoDB insertion...")
        start_time = time.time()
        individual_times = []

        # Use tqdm to create a progress bar
        for record in tqdm(records, desc="Inserting Records", unit="record"):
            record_start = time.time()  # Record the start time for this insertion
            self.handler.insert_one('reviews', {
                "product_id": record.get("product/productId"),
                "user_id": record.get("review/userId"),
                "profile_name": record.get("review/profileName"),
                "helpfulness": record.get("review/helpfulness"),
                "score": float(record.get("review/score", 0)),
                "review_time": int(record.get("review/time", 0)),
                "summary": record.get("review/summary"),
                "review_text": record.get("review/text")
            })
            record_end = time.time()
            individual_times.append(record_end - record_start)

        end_time = time.time()
        total_time = end_time - start_time  # Total insertion time
        print(f"Inserted {len(records)} records into MongoDB in {total_time:.2f} seconds.")
        return total_time, individual_times

    def test_query_performance(self, filter_query):
        """Test query performance in MongoDB."""
        print("Testing MongoDB query performance...")
        start_time = time.time()
        results = self.handler.query_multiple_fields('reviews', filter_query)
        end_time = time.time()
        total_time = end_time - start_time
        print(f"Query completed in {total_time:.2f} seconds, returned {len(results)} documents.")
        return total_time, results

    def test_index_performance(self, field):
        """Test performance with and without index."""
        print(f"Testing MongoDB index performance on field '{field}'...")
        # Without Index
        print("Testing without index...")
        filter_query = {field: "some_value"}
        no_index_time, _ = self.test_query_performance(filter_query)

        # With Index
        print("Creating index and testing with index...")
        self.handler.create_single_field_index('reviews', field)
        index_time, _ = self.test_query_performance(filter_query)

        print(f"Performance comparison: Without Index: {no_index_time:.2f}s, With Index: {index_time:.2f}s.")
        return no_index_time, index_time

    def test_insertion_many(self, records, bulk_size=-1):
        """Test bulk insertion of records into MongoDB."""
        print("Testing MongoDB bulk insertion...")
        start_time = time.time()
        individual_times = []
        total_records = len(records)

        # Determine bulk size
        if bulk_size == -1:
            # Insert all records as one bulk
            bulk_size = total_records
            print("Inserting all records in a single bulk.")

        # Prepare data for insertion
        formatted_records = [
            {
                "product_id": record.get("product/productId"),
                "user_id": record.get("review/userId"),
                "profile_name": record.get("review/profileName"),
                "helpfulness": record.get("review/helpfulness"),
                "score": float(record.get("review/score", 0)),
                "review_time": int(record.get("review/time", 0)),
                "summary": record.get("review/summary"),
                "review_text": record.get("review/text")
            }
            for record in records
        ]

        # Split the data into chunks based on the bulk size
        for i in tqdm(range(0, total_records, bulk_size), desc="Inserting Bulk Records", unit="bulk"):
            bulk_start = time.time()
            bulk = formatted_records[i:i + bulk_size]
            self.handler.insert_many('reviews', bulk)
            bulk_end = time.time()
            individual_times.append(bulk_end - bulk_start)

        end_time = time.time()
        total_time = end_time - start_time
        print(f"Inserted {total_records} records into MongoDB in {total_time:.2f} seconds using bulk size {bulk_size}.")
        return total_time, individual_times

    def ensure_empty(self, collection_name="reviews"):
        """Ensure that the MongoDB collection is empty."""
        if not self.handler.is_empty(collection_name):
            print(f"MongoDB collection '{collection_name}' is not empty. Reinitializing...")
            self.handler.initialize_collection(collection_name)
            print(f"MongoDB collection '{collection_name}' has been reinitialized.")
