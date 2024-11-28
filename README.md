Here’s the updated markdown file with embedded images and an explanation of why MongoDB outperforms PostgreSQL in both
persistent and non-persistent connection scenarios.

---

# MongoDB vs. PostgreSQL

## Table of Contents

1. [Introduction](#introduction)
2. [Setup Instructions](#setup-instructions)
3. [Database Creation and Management](#database-creation-and-management)
    - [PostgreSQL Database Creation and Deletion](#postgresql-database-creation-and-deletion)
    - [MongoDB Database Creation and Deletion](#mongodb-database-creation-and-deletion)
4. [Simulations](#simulations)
    - [Insert Simulation](#insert-simulation)
5. [Comparison Metrics](#comparison-metrics)
    - [Insertion Speed](#insertion-speed)
6. [Results](#results)
7. [Conclusion](#conclusion)

---

## Introduction

The goal of this project was to compare MongoDB and PostgreSQL in terms of key performance metrics, starting with the
insertion operation. The objective is to identify how each database handles different workloads and configurations to
help determine optimal use cases.

---

## Setup Instructions

1. **PostgreSQL**: Ensure PostgreSQL is installed and configured locally. Use `psycopg2` for Python connectivity.
2. **MongoDB**: Install MongoDB and ensure `pymongo` is available for Python.
3. **Configuration**: Use `postgres_config.json` and `mongo_config.json` to define connection parameters for each
   database.

---

## Database Creation and Management

### PostgreSQL Database Creation and Deletion

The PostgreSQL setup involves creating a new database (`benchmark_db`) and a table (`reviews`) before each simulation.
Existing databases and tables are dropped before reinitialization.

### MongoDB Database Creation and Deletion

For MongoDB, the database (`benchmark_db`) and the collection (`reviews`) are dropped and recreated at the beginning of
each simulation to ensure a clean environment.

---

## Simulations

### Insert Simulation

Two insertion simulations were conducted:

1. **Without Persistent Connections**:
    - 3000 records were inserted.
    - Each database closed its connection after every insertion.
2. **With Persistent Connections**:
    - 100,000 records were inserted.
    - A persistent connection was maintained throughout.

---

## Comparison Metrics

### Insertion Speed

Insertion speed is a critical metric for evaluating database performance under high write loads. Here are the results:

1. **Without Persistent Connections** (3000 records):
    - MongoDB: **84.26 seconds** (35.60 records/second)
    - PostgreSQL: **141.66 seconds** (21.22 records/second)

   ![Database Insertion Time Comparison (Persistent connection: False)](files/insertion/Database%20Insertion%20Time%20Comparison%20(PC_False).png)

   **Interpretation**:
    - MongoDB outperformed PostgreSQL significantly in this scenario. The high overhead of establishing a connection for
      every record contributed to PostgreSQL's slower performance.

2. **With Persistent Connections** (100,000 records):
    - MongoDB: **33.45 seconds** (2989.53 records/second)
    - PostgreSQL: **42.27 seconds** (2366.52 records/second)

   ![Database Insertion Time Comparison (Persistent connection: True)](files/insertion/Database%20Insertion%20Time%20Comparison%20(PC_True).png)

   **Interpretation**:
    - Persistent connections significantly improved both databases' performance. MongoDB retained its edge due to its
      schema-less nature and lighter write operations.

---

## Results

### Individual Record Insertion Times

Analyzing individual record insertion times provided further insights into performance variability:

1. **MongoDB Without Persistent Connections**:

   ![Individual Record Insertion Times for MongoDB (Persistent connection: False)](files/insertion/Individual%20Record%20Insertion%20Times%20for%20MongoDB%20(PC_False).png)

   **Interpretation**:
    - Consistent performance for most records, with slight spikes likely due to intermittent network or I/O delays.

2. **MongoDB With Persistent Connections**:

   ![Individual Record Insertion Times for MongoDB (Persistent connection: True)](files/insertion/Individual%20Record%20Insertion%20Times%20for%20MongoDB%20(PC_True).png)

   **Interpretation**:
    - Overall lower times with minimal variability. Outliers at the beginning and end might be due to initial connection
      setup and cleanup.

3. **PostgreSQL Without Persistent Connections**:

   ![Individual Record Insertion Times for PostgreSQL (Persistent connection: False)](files/insertion/Individual%20Record%20Insertion%20Times%20for%20PostreSQL%20(PC_False).png)

   **Interpretation**:
    - Higher variability and frequent spikes due to connection overhead and transaction handling.

4. **PostgreSQL With Persistent Connections**:

   ![Individual Record Insertion Times for PostgreSQL (Persistent connection: True)](files/insertion/Individual%20Record%20Insertion%20Times%20for%20PostreSQL%20(PC_True).png)

   **Interpretation**:
    - More stable performance and significantly reduced variability, with occasional spikes caused by background
      transaction commits.

---

## Why MongoDB Is Faster in Both Cases

### Technical Reasons for MongoDB's Performance Edge

1. **Schema Flexibility**:
    - MongoDB does not enforce a strict schema, avoiding additional validation overhead during insertion.
    - PostgreSQL, on the other hand, checks for schema constraints, which adds time to every insert.

2. **Connection Overhead**:
    - MongoDB uses lighter protocols (e.g., BSON and JSON-like data structures) that are optimized for flexibility.
    - PostgreSQL relies on heavier transaction-based protocols, which can slow down individual operations.

3. **Write Optimization**:
    - MongoDB's write operations are optimized for high throughput, leveraging features like memory-mapped storage for
      rapid writes.
    - PostgreSQL's write operations include more strict ACID compliance (e.g., ensuring immediate durability of
      transactions), adding latency.

4. **Persistent Connections**:
    - Both databases benefit from persistent connections, but the difference is more pronounced for PostgreSQL because
      establishing and tearing down a connection is expensive.

---

## Conclusion

1. **MongoDB**:
    - Outperforms PostgreSQL in write-heavy operations, especially without persistent connections.
    - Ideal for workloads that prioritize write speed and scalability over strict relational integrity.

2. **PostgreSQL**:
    - Better suited for applications requiring ACID compliance and complex relational queries.
    - Connection persistence significantly improves performance, narrowing the gap with MongoDB.

These results demonstrate that choosing the right database depends on the workload. MongoDB is advantageous for agile,
schema-less applications, while PostgreSQL excels in structured, transactional systems.