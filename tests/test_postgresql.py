import unittest

from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from db.postgres_handler import PostgresDBHandler
from utils.config_loader import load_config


class TestPostgresDBHandler(unittest.TestCase):
    def setUp(self):
        """Set up the database handler and ensure the `reviews` table exists."""
        config = load_config('../config/postgres_config.json')
        self.db_handler = PostgresDBHandler(config, use_persistent_connection=True)
        self.db_handler.create_reviews_table()  # Ensure the table exists

    def tearDown(self):
        """Clean up by deleting all rows in the `reviews` table."""
        conn = self.db_handler._get_connection()
        cursor = conn.cursor()
        # cursor.execute("DELETE FROM reviews;")
        conn.commit()
        cursor.close()

    def test_insert_and_verify_row(self):
        """Test inserting a row into the `reviews` table and verifying its presence."""
        # Define the test row
        test_row = {
            "product/productId": "B001E4KFG0",
            "review/userId": "A1E5YZGEUSK7F2",
            "review/profileName": "Delightful Reviewersss",
            "review/helpfulness": "2/3",
            "review/score": 4.5,
            "review/time": 1234567890,
            "review/summary": "Great product!",
            "review/text": "I loved this product. It exceeded expectations!"
        }

        # Insert the row into the `reviews` table
        self.db_handler.insert_one(test_row)

        # Verify the row was inserted correctly
        conn = self.db_handler._get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            sql.SQL("SELECT * FROM reviews WHERE product_id = %s AND user_id = %s;"),
            [test_row["product/productId"], test_row["review/userId"]]
        )
        result = cursor.fetchone()
        cursor.close()

        # Assert that the row exists and matches the test row
        self.assertIsNotNone(result, "Inserted row not found in the database.")
        self.assertEqual(result["product_id"], test_row["product/productId"])
        self.assertEqual(result["user_id"], test_row["review/userId"])
        self.assertEqual(result["profile_name"], test_row["review/profileName"])
        self.assertEqual(result["helpfulness"], test_row["review/helpfulness"])
        self.assertEqual(result["score"], test_row["review/score"])
        self.assertEqual(result["review_time"], test_row["review/time"])
        self.assertEqual(result["summary"], test_row["review/summary"])
        self.assertEqual(result["review_text"], test_row["review/text"])


if __name__ == "__main__":
    unittest.main()
