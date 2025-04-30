from pymongo import MongoClient, ASCENDING

from pymongo.results import DeleteResult, UpdateResult, InsertOneResult, InsertManyResult
from pymongo.synchronous.command_cursor import CommandCursor

from pymongo_pipelines import Pipelines
from pymongo.errors import ConnectionFailure, ConfigurationError, CollectionInvalid, PyMongoError, WriteError, OperationFailure
import logging
from typing import Any, MutableMapping, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class MongoDbOperation:
    @classmethod
    def __connect(cls) -> Optional[MongoClient]:
        client: Optional[MongoClient] = None
        try:
            uri: str = "PASTE YOUR URI HERE" # get it from MongoDB Atlas or get it from your local MongoDb
            client: MongoClient = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            logging.info("Connected to MongoDB successfully!")
            return client
        except ConnectionFailure as cf:
            logging.exception(f"Could not connect to MongoDB: {cf}")
            if client is not None:
                client.close()
            return None

    @staticmethod
    def execute_aggregate_pipeline(pipeline_: list[dict[str, Any]]) -> None:
        """Run aggregation pipeline on the 'cars' collection in 'Test' DB."""
        if not pipeline_:
            raise ValueError("Aggregation pipeline must not be empty.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()

        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            db = client.get_database('Test')
            my_collection = db.get_collection('cars')

            logging.info("Executing aggregation pipeline...")

            result = my_collection.aggregate(pipeline_)
            for document in result:
                for key, value in document.items():
                    print(f'{key} : {value}')
                print("=" * 30)

        except PyMongoError as ex:
            logging.exception(f"Aggregation failed: {ex}")

        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def aggregate_join_collection(pipeline_: list[dict[str, Any]]) -> None:
        """Run aggregation join pipeline on the 'users' collection in 'store_db'."""
        if not pipeline_:
            raise ValueError("Aggregation pipeline must not be empty.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()

        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            db = client.get_database('store_db')
            my_collection = db.get_collection('users')

            logging.info("Executing aggregation join pipeline...")
            result = my_collection.aggregate(pipeline_)

            for document in result:
                for key, value in document.items():
                    if key == "orders" and isinstance(value, list):
                        if not value:
                            print("Orders: None")
                        else:
                            print("Orders:")
                            for order in value:
                                for k, v in order.items():
                                    print(f'  {k} : {v}')
                    elif key != "orders":
                        print(f'{key} : {value}')
                print("=" * 30)

        except PyMongoError as ex:
            logging.exception(f"Aggregation failed: {ex}")

        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def get_database_names() -> None:
        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            databases: list[str] = client.list_database_names()

            if not databases:
                print("No databases found.")
            else:
                print("List of databases:")
                for db in databases:
                    print(f" - {db}")

        except PyMongoError as ex:
            logging.exception(f"An error occurred while fetching database names: {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def get_collection_names(database_name: str) -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            if database_name not in client.list_database_names():
                print(f"The database '{database_name}' does not exist.")
                return

            db = client[database_name]
            collections: list[str] = db.list_collection_names()

            if not collections:
                print(f"No collections found in '{database_name}'.")
            else:
                print(f"Collections in '{database_name}':")
                for name in collections:
                    print(f" - {name}")

        except PyMongoError as ex:
            logging.exception(f"An error occurred while listing collections in '{database_name}': {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def create_collection(database_name: str, collection_name: str, validator: dict[str, Any] | None = None) -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")
        if not collection_name:
            raise ValueError("Collection name must not be empty.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            if database_name not in client.list_database_names():
                print(f"The database '{database_name}' does not exist.")
                return

            db = client[database_name]

            if validator:
                db.create_collection(collection_name, validator = validator)
            else:
                db.create_collection(collection_name)

            print(f"The collection '{collection_name}' was created successfully.")

        except CollectionInvalid:
            logging.warning(f"The collection '{collection_name}' already exists")
        except PyMongoError as ex:
            logging.exception(f"An error occurred while creating the collection '{collection_name}': {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def drop_collection(database_name: str, collection_name: str) -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")
        if not collection_name:
            raise ValueError("Collection name must not be empty.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            if database_name not in client.list_database_names():
                print(f"The database '{database_name}' does not exist.")
                return

            db = client[database_name]

            if collection_name not in db.list_collection_names():
                print(f"The collection '{collection_name}' does not exist in database '{database_name}'.")
                return

            db.drop_collection(collection_name)
            print(f"The collection '{collection_name}' was dropped successfully.")

        except PyMongoError as ex:
            logging.exception(f"An error occurred while dropping the collection '{collection_name}': {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def fetch_document(database_name: str, collection_name: str) -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")
        if not collection_name:
            raise ValueError("Collection name must not be empty.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            if database_name not in client.list_database_names():
                print(f"The database '{database_name}' does not exist.")
                return

            db = client[database_name]

            if collection_name not in db.list_collection_names():
                print(f"The collection '{collection_name}' does not exist in database '{database_name}'.")
                return

            collection = db.get_collection(collection_name)
            documents = collection.find()

            found = False
            for doc in documents:
                found = True
                for k, v in doc.items():
                    print(f"{k}: {v}")
                print("-" * 30)

            if not found:
                print(f"No documents found in collection '{collection_name}'.")

        except PyMongoError as ex:
            logging.exception(f"An error occurred while fetching documents from '{collection_name}': {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def create_database(database_name: str) -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")
        try:
            if database_name in client.list_database_names():
                print(f"The database '{database_name}' already exists.")
                return

            db = client[database_name]
            # Trigger actual creation by inserting a dummy doc
            db['__temp_collection__'].insert_one({'created': True})
            db.drop_collection('__temp_collection__')  # Clean it up

            if database_name in client.list_database_names():
                print(f"The database '{database_name}' was created successfully.")
            else:
                print(f"Failed to verify creation of the database '{database_name}'.")
        except PyMongoError as ex:
            logging.exception(f"An error occurred while creating the database: {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def drop_database(database_name: str) -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")

        client: MongoClient | None = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            if database_name not in client.list_database_names():
                logging.info(f"The database '{database_name}' does not exist.")
                return

            logging.info(f"Trying to drop the database: {database_name}")
            client.drop_database(database_name)
            print(f"The database '{database_name}' was dropped successfully.")
        except PyMongoError as ex:
            logging.exception(f"An error occurred while dropping the database '{database_name}': {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def insert_document(database_name: str, collection_name: str, document: dict[str, Any] | list[dict[str, Any]]) -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")
        if not collection_name:
            raise ValueError("Collection name must not be empty.")
        if not document:
            raise ValueError("Document must be a dictionary or a list of dictionaries and Document must not be empty.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            if database_name not in client.list_database_names():
                print(f"The database '{database_name}' does not exist.")
                return

            db = client[database_name]

            if collection_name not in db.list_collection_names():
                print(f"The collection '{collection_name}' does not exist in database '{database_name}'.")
                return

            collection = db[collection_name]

            if isinstance(document, dict):
                result_1: InsertOneResult = collection.insert_one(document)
                print(f"Document inserted with _id: {result_1.inserted_id}")
            elif isinstance(document, list):
                result_2: InsertManyResult = collection.insert_many(document)
                for inserted_id in result_2.inserted_ids:
                    print(f"Document inserted with _id: {inserted_id}")
            else:
                raise ValueError("Document must be a dictionary or a list of dictionaries.")

        except WriteError as we:
            print("\n❌ WriteError: Document failed validation!")
            print("Error Details:")

            if we.details is not None:
                for key, value in we.details.items():
                    if key == "errInfo":
                        print("\nError Info:")
                        failing_doc_id = value.get("failingDocumentId", None)
                        if failing_doc_id:
                            print(f"  Failing Document ID: {failing_doc_id}")
                        # Dive into the 'details' part
                        details = value.get("details", {})
                        if details:
                            schema_rules = details.get("schemaRulesNotSatisfied", [])
                            for rule in schema_rules:
                                if rule.get("operatorName") == "properties":
                                    properties_not_satisfied = rule.get("propertiesNotSatisfied", [])
                                    for prop in properties_not_satisfied:
                                        property_name = prop.get("propertyName", "Unknown")
                                        description = prop.get("description", "No description")
                                        print(f"\n  Field: {property_name}")
                                        print(f"  Description: {description}")

                                        # Further go inside "details"
                                        prop_details = prop.get("details", [])
                                        for detail in prop_details:
                                            operator_name = detail.get("operatorName", "Unknown")
                                            specified = detail.get("specifiedAs", "Unknown")
                                            reason = detail.get("reason", "Unknown reason")
                                            considered_value = detail.get("consideredValue", "Unknown value")
                                            considered_type = detail.get("consideredType", "Unknown type")
                                            print(f"    Operator: {operator_name}")
                                            print(f"    Specified: {specified}")
                                            print(f"    Reason: {reason}")
                                            print(f"    Considered Value: {considered_value}")
                                            print(f"    Considered Type: {considered_type}")

                                elif rule.get("operatorName") == "required":
                                    # Handling missing required fields
                                    missing_properties = rule.get("missingProperties", [])
                                    if missing_properties:
                                        print("\n  Missing Required Fields:")
                                        for field in missing_properties:
                                            print(f"    - {field}")
                    else:
                        print(f"{key}: {value}")
            print()

        except PyMongoError as ex:
            logging.exception(f"An error occurred while inserting the document(s): {ex}")
            print(f"Failed to insert document(s): {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def update_document(database_name: str, collection_name: str, filter_condition: dict[str, Any], update_values: dict[str, Any], update_type: str = "one") -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")
        if not collection_name:
            raise ValueError("Collection name must not be empty.")
        if not filter_condition or not isinstance(filter_condition, dict):
            raise ValueError("Filter condition must be a non-empty dictionary.")
        if not update_values or not isinstance(update_values, dict):
            raise ValueError("Update values must be a non-empty dictionary.")
        if update_type.strip().lower() not in ("one", "many"):
            raise ValueError("Invalid input. Please enter 'one' or 'many' for update_type.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            if database_name not in client.list_database_names():
                print(f"The database '{database_name}' does not exist.")
                return

            db = client[database_name]

            if collection_name not in db.list_collection_names():
                print(f"The collection '{collection_name}' does not exist in database '{database_name}'.")
                return

            collection = db[collection_name]
            update_operation = {"$set": update_values}

            if update_type.strip().lower() == "one":
                result_1: UpdateResult = collection.update_one(filter_condition, update_operation)
                print(f"Acknowledged {result_1.acknowledged}, Matched {result_1.matched_count}, Modified {result_1.modified_count} (single document).")
            elif update_type.strip().lower() == "many":
                result_2: UpdateResult = collection.update_many(filter_condition, update_operation)
                print(f"Acknowledged {result_2.acknowledged}, Matched {result_2.matched_count}, Modified {result_2.modified_count} (multiple documents).")

        except PyMongoError as ex:
            logging.exception(f"An error occurred while updating documents in collection '{collection_name}': {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def delete_document(database_name: str, collection_name: str, filter_query: dict[str, Any], delete_type: str = "one") -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")
        if not collection_name:
            raise ValueError("Collection name must not be empty.")
        if not filter_query or not isinstance(filter_query, dict):
            raise ValueError("Filter query must be a non-empty dictionary.")
        if delete_type.strip().lower() not in ("one", "many"):
            raise ValueError("Invalid input. Please enter 'one' or 'many' for update_type.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            if database_name not in client.list_database_names():
                print(f"The database '{database_name}' does not exist.")
                return

            db = client[database_name]

            if collection_name not in db.list_collection_names():
                print(f"The collection '{collection_name}' does not exist in database '{database_name}'.")
                return

            collection = db[collection_name]

            if delete_type.strip().lower() == "one":
                result_1: DeleteResult = collection.delete_one(filter_query)
                print(f"Acknowledged {result_1.acknowledged}, Deleted {result_1.deleted_count} document.")
            elif delete_type.strip().lower() == "many":
                result_2: DeleteResult = collection.delete_many(filter_query)
                print(f"Acknowledged {result_2.acknowledged}, Deleted {result_2.deleted_count} documents.")

        except PyMongoError as ex:
            logging.exception(f"An error occurred during deletion from '{collection_name}': {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def modify_existing_collection_schema(database_name: str, collection_name: str, validator: dict[str, Any]) -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")
        if not collection_name:
            raise ValueError("Collection name must not be empty.")
        if not validator or not isinstance(validator, dict):
            raise ValueError("Validator must be a non-empty dictionary.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            if database_name not in client.list_database_names():
                print(f"The database '{database_name}' does not exist.")
                return

            db = client[database_name]

            if collection_name not in db.list_collection_names():
                print(f"The collection '{collection_name}' does not exist in database '{database_name}'.")
                return

            db.command({
                "collMod": collection_name,
                "validator": validator,
                "validationLevel": "moderate",   # Can also be "strict"
                "validationAction": "warn"       # Or "error"
            })
            print(f"Schema modified successfully for collection '{collection_name}'.")

        except PyMongoError as ex:
            logging.exception(f"An error occurred during schema modification in collection '{collection_name}': {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def create_index(database_name: str, collection_name: str, index_name: str) -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")
        if not collection_name:
            raise ValueError("Collection name must not be empty.")
        if not index_name:
            raise ValueError("Index name must not be empty.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            if database_name not in client.list_database_names():
                print(f"The database '{database_name}' does not exist.")
                return

            db = client[database_name]

            if collection_name not in db.list_collection_names():
                print(f"The collection '{collection_name}' does not exist in database '{database_name}'.")
                return

            my_collection = db.get_collection(collection_name)
            index_result: str = my_collection.create_index([(index_name, ASCENDING)], unique=True)
            print(f"The index '{index_result}' was created successfully in collection '{collection_name}'.")

        except OperationFailure as ex:
            logging.exception(f"Index creation failed for collection '{collection_name}' with index '{index_name}': {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def show_indexes(database_name: str, collection_name: str) -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")

        if not collection_name:
            raise ValueError("Collection name must not be empty.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            if database_name not in client.list_database_names():
                print(f"The database '{database_name}' does not exist.")
                return

            db = client[database_name]

            if collection_name not in db.list_collection_names():
                print(f"The collection '{collection_name}' does not exist in database '{database_name}'.")
                return

            my_collection = db.get_collection(collection_name)
            index_cursor: CommandCursor[MutableMapping[str, Any]] = my_collection.list_indexes()

            print(f"The list of indexes for collection `{collection_name}`:")
            for index in index_cursor:
                print("\nIndex:")
                for key, value in index.items():
                    print(f"  {key}: {value}")

        except OperationFailure as of:
            logging.exception(f"Failed to retrieve the indexes from the collection `{collection_name}`: {of}")

        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def drop_index(database_name: str, collection_name: str, index_name: str) -> None:
        if not database_name:
            raise ValueError("Database name must not be empty.")
        if not collection_name:
            raise ValueError("Collection name must not be empty.")
        if not index_name:
            raise ValueError("Index name must not be empty.")

        client: Optional[MongoClient] = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            if database_name not in client.list_database_names():
                print(f"The database '{database_name}' does not exist.")
                return

            db = client[database_name]

            if collection_name not in db.list_collection_names():
                print(f"The collection '{collection_name}' does not exist in database '{database_name}'.")
                return

            my_collection = db.get_collection(collection_name)

            existing_indexes = [index['name'] for index in my_collection.list_indexes()]
            if index_name not in existing_indexes:
                print(f"Index '{index_name}' does not exist in collection '{collection_name}'.")
                return

            my_collection.drop_index(index_or_name = index_name)
            print(f"The index '{index_name}' was dropped successfully from collection '{collection_name}'.")

        except OperationFailure as ex:
            logging.exception(f"Failed to delete index '{index_name}' from the collection '{collection_name}': {ex}")

        finally:
            client.close()
            logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    try:
        validation_rule = Pipelines.validator()
        # MongoDbOperation.modify_existing_collection_schema(database_name = 'store_db', collection_name = 'users_1', validator = validation_rule)
        # MongoDbOperation.create_index(database_name='store_db', collection_name='users_1', index_name='name')
        # MongoDbOperation.show_indexes(database_name='store_db', collection_name='users_1')
        # MongoDbOperation.drop_index(database_name='store_db', collection_name='users_1', index_name='name_1')
        # MongoDbOperation.update_document(database_name='store_db', collection_name='users_1', filter_condition={"name":"sufiyan"}, update_values={"name": "serazi"}, update_type="one")

    except ConfigurationError:
        logging.error("Could not configure MongoDB client. Check your internet connection.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

