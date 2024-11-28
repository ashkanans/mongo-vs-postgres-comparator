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

        # Use tqdm to create a progress bar
        for record in tqdm(records, desc="Inserting Records", unit="record"):
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

        end_time = time.time()
        total_time = end_time - start_time
        print(f"Inserted {len(records)} records into MongoDB in {total_time:.2f} seconds.")
        return total_time

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
