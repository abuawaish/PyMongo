# MongoDB Operations Manager with PyMongo

![MongoDB](https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=for-the-badge&logo=mongodb&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)

A complete MongoDB management system implementing CRUD operations, aggregation pipelines, schema validation, and index management using PyMongo.

## Features

### Core Operations
- **Database Management**
  - Create/Drop databases
  - List all databases
- **Collection Operations**
  - Create/Drop collections
  - List collection names
  - Schema validation/modification
- **Document Operations**
  - Insert (single/bulk)
  - Update (single/multiple)
  - Delete (single/multiple)
  - Fetch documents

### Advanced Features
- **Aggregation Framework**
  - 10 predefined pipelines
  - Join operations between collections
  - Complex data transformations
- **Index Management**
  - Create indexes
  - List existing indexes
  - Drop indexes
- **Validation System**
  - JSON schema validation
  - Detailed error reporting
  - Schema modification for existing collections

### Operational Safety
- Connection pooling management
- Comprehensive error handling
- Type hinting throughout
- Input validation for all operations
- Clean resource cleanup

### Predefined Aggregation Pipelines

- **Sample Datasets**
  - Cars data (15 documents)
  - Users & Orders data (5 users, 5 orders)

## Project Structure
```text
PyMongo/
├── pymongo_tutorial.py    # Main MongoDB operations class
├── pymongo_pipelines.py   # Aggregation pipelines and sample data
├── .gitignore             # Ignore tracked files.
├── LICENSE                # Grants rights to users
└── README.md              # This documentation
```

## Prerequisites

- Python 3.10+
- PyMongo (4.6.0+)
- MongoDB Atlas URI

## Installation

1. Install PyMongo:

```bash
pip install pymongo
```
2. Add your MongoDB Atlas URI:

```text
# In pymongo_tutorial.py
uri: str = "mongodb+srv://<username>:<password>@cluster.x.mongodb.net/"
```

## Usage

1. Aggregation Pipelines

```text
# Execute car analysis pipeline
MongoDbOperation.execute_aggregate_pipeline(
    pipeline_=Pipelines.pipeline_1()
)

# Perform collection join
MongoDbOperation.aggregate_join_collection(
    pipeline_=Pipelines.join_pipeline()
)
```

2. Database Management

```text
# Create database
MongoDbOperation.create_database('new_db')

# List databases
MongoDbOperation.get_database_names()
```

3. Collection Operations

```text
# Create collection with validation
validation = Pipelines.validator()
MongoDbOperation.create_collection(
    database_name='store_db',
    collection_name='users',
    validator=validation
)

# Drop collection
MongoDbOperation.drop_collection('store_db', 'temp_collection')
```

4. Document Operations

```text
# Insert document
MongoDbOperation.insert_document(
    database_name='store_db',
    collection_name='users',
    document={
        "name": "John Doe",
        "age": 30,
        "email": "john@example.com"
    }
)

# Update document
MongoDbOperation.update_document(
    database_name='store_db',
    collection_name='users',
    filter_condition={"name": "John Doe"},
    update_values={"age": 31},
    update_type="one"
)
```

5. Index Management

```text
# Create index
MongoDbOperation.create_index(
    database_name='store_db',
    collection_name='users',
    index_name='email'
)

# List indexes
MongoDbOperation.show_indexes(
    database_name='store_db',
    collection_name='users'
)

# Drop index
MongoDbOperation.drop_index(
    database_name='store_db',
    collection_name='users',
    index_name='email_1'
)
```

## Predefined Pipelines (pymongo_pipelines.py)

1. **Basic Aggregation**
    - Group by fuel type
    - Calculate car statistics

```text
Pipelines.pipeline_1()
```

2. **Data Transformation**
    - Add computed fields
    - Format prices

```text
Pipelines.pipeline_6()
```

3. **Joins**
    - Users with Orders

```text
Pipelines.join_pipeline()
```

4. **Validation Schema**
    - Name (required string)
    - Age (minimum 18)
    - Email format validation

```text
Pipelines.validator()
```

## Sample Data Structures

### **Cars Collection**

```doctest
{
    "maker": "Hyundai",
    "model": "Creta",
    "fuel_type": "Diesel",
    "price": 1500000,
    "features": ["Sunroof", "Leather Seats"],
    "owners": [
        {"name": "Raju", "location": "Mumbai"}
    ]
}
```

### **Users Collection**

```doctest
{
    "_id": "user1",
    "name": "Amit Sharma",
    "email": "amit.sharma@example.com",
    "phone": "+91-987654210"
}
```

### **Orders Collection**

```doctest
{
    "_id": "order1",
    "user_id": "user1",
    "product": "Laptop",
    "amount": 50000
}
```

## Error Handling

**The implementation includes comprehensive error handling for:**

- Connection failures

- Invalid operations

- Schema validation errors

- Write conflicts

- Collection/Database existence checks

**Example validation error output:**

## Index Management
**Supported Index Operations**

1. Creation
   - Single field indexes
   - Unique constraints

2. Inspection
   - List all indexes
   - View index specifications

3. Removal
   - Drop by index name
   - Safety checks before removal

```text
❌ WriteError: Document failed validation!
Field: age
Description: must be greater than or equal to 18
  Operator: $gte
  Specified: {"minimum": 18}
  Reason: "consideredValue" '17' is not numeric
```

# MongoDB Workflow Overview

## 🚀 Workflow Summary

This project provides a robust framework for managing MongoDB operations with validation, aggregation, and error handling. Below is a detailed breakdown of the workflow:

---

## 🔌 Connection Setup

- Establish secure MongoDB connection using `MongoDbOperation` class.
- Automatic connection timeout handling (5 seconds) and verification.
- Graceful handling of connection failures.

---

## 🛠️ Database/Collection Setup

Create databases and collections with strict schema validation rules.
- **Predefined collections**:
  - `cars` (in `Test` database)
  - `users`, `orders` (in `store_db` database)
- **Validation rules**:
  - `age` field must be ≥18.
  - Proper email format enforcement.
  - Required fields must be present.

---

## 📂 Data Operations

### Insert
- Load sample datasets:
  - 15 cars
  - 5 users
  - 5 orders
- Validation performed during data insertion.

### Update/Delete
- Modify documents with:
  - Single document operations
  - Multiple document operations

### Fetch
- Retrieve documents with:
  - Collection/database existence checks
  - Structured error handling if not found

---

## 📊 Aggregation & Analysis

Execute 10 predefined aggregation pipelines including:

- **Fuel-Type Statistics** (`pipeline_1`)
- **Price Categorization** (`pipeline_9`)
- **Service Cost Analysis** (`pipeline_7`)
- **Collection Joins** (`join_pipeline`)

**Aggregation features**:
- Field projections
- Grouping operations
- Computed fields
- Output to collections

---

## 🛡️ Validation & Error Handling

- Structured validation errors with detailed, field-specific diagnostics.
- Connection failure recovery logic.
- Write conflict resolution handling.
- Guaranteed resource cleanup via `finally` blocks.

---

## 🛠️ Maintenance

- List all databases and collections.
- Drop specific databases or collections.
- Modify schema validation for existing collections.
- Create temporary collections for database initialization.

---

## 🔑 Key Flow

```plaintext
Connection → Setup → Data Insertion → Aggregation → CRUD → Validation → Cleanup
