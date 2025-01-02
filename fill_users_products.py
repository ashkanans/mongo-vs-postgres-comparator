# import random
# import hashlib
# import datetime
#
# from pymongo import MongoClient
#
# from db.handler.mongodb_handler import MongoDBHandler
# from utils.config_loader import load_config
#
# mongo_config = load_config('config/mongo_config.json')
# mongo_handler = MongoDBHandler(mongo_config)
# user_ids = mongo_handler.get_all_user_ids("reviews", field_name="user_id")
#
#
# def generate_random_user(user_id):
#     random_name = "User_" + hashlib.md5(str(random.random()).encode()).hexdigest()[:8]
#     random_email = random_name.lower() + "@example.com"
#     random_date = datetime.date(2020, 1, 1) + datetime.timedelta(days=random.randint(0, 365))
#     user_type = "regular" if random.random() < 0.5 else "premium"
#     return {
#         "user_id": user_id,
#         "name": random_name,
#         "email": random_email,
#         "join_date": random_date.isoformat(),
#         "user_type": user_type,
#     }
#
#
# # Generate and insert random user data into the 'users' collection
# # MongoDB Configuration
# config = {
#     "host": "localhost",
#     "port": 27017,
#     "database": "benchmark_db"
# }
#
# # Connect to MongoDB
# client = MongoClient(host=config["host"], port=config["port"])
# db = client[config["database"]]
# users_collection = db['users']
#
# users = [generate_random_user(user_id) for user_id in user_ids]
# users_collection.insert_many(users)
#
# print(f"Inserted {len(users)} users into the 'users' collection in the '{config['database']}' database.")

import random
import hashlib
import datetime
from pymongo import MongoClient
from db.handler.mongodb_handler import MongoDBHandler
from utils.config_loader import load_config

# Load configuration and connect to MongoDB
mongo_config = load_config('config/mongo_config.json')
mongo_handler = MongoDBHandler(mongo_config)
ids = mongo_handler.get_all_user_ids("reviews", field_name="product_id")  # Fetch all product IDs

# MongoDB Configuration
config = {
    "host": "localhost",
    "port": 27017,
    "database": "benchmark_db"
}

# Connect to MongoDB
client = MongoClient(host=config["host"], port=config["port"])
db = client[config["database"]]
products_collection = db['products']


# Function to generate random product data
def generate_random_product(product_id):
    random_name = "Product_" + hashlib.md5(str(random.random()).encode()).hexdigest()[:6]
    category = random.choice(["Electronics", "Clothing", "Books", "Home", "Toys", "Sports"])
    price = round(random.uniform(5.0, 500.0), 2)  # Random price between $5 and $500
    stock = random.randint(10, 1000)  # Random stock between 10 and 1000
    return {
        "product_id": product_id,
        "product_name": random_name,
        "category": category,
        "price": price,
        "stock": stock
    }


# Generate and insert random product data into the 'products' collection
products = [generate_random_product(product_id) for product_id in ids if product_id is not None]
products_collection.insert_many(products)

print(f"Inserted {len(products)} products into the 'products' collection in the '{config['database']}' database.")
