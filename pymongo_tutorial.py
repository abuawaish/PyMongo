from pymongo import MongoClient
from pymongo_pipelines import Pipelines
from pymongo.errors import ConnectionFailure, ConfigurationError, CollectionInvalid, PyMongoError, WriteError
import logging
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
class MongoDbOperation:
    @classmethod
    def __connect(cls) -> MongoClient | None:

        try:
            uri: str = "PASTE YOUR URI HERE" # get it from MongoDB Atlas
            client: MongoClient = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            logging.info("Connected to MongoDB successfully!")
            return client
        except ConnectionFailure as cf:
            logging.error(f"Could not connect to MongoDB: {cf}")
            return None

    @staticmethod
    def execute_aggregate_pipeline(pipeline_: list[dict[str, Any]]) -> None:
        """Run aggregation pipeline on the 'cars' collection in 'Test' DB."""
        client: MongoClient | None = MongoDbOperation.__connect()

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

        except Exception as e_:
            raise RuntimeError(f"Aggregation failed: {e_}") from e_

        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def aggregate_join_collection(pipeline_: list[dict[str, Any]]) -> None:
        """Run aggregation join pipeline on the 'users' collection in 'store_db'."""
        client: MongoClient | None = MongoDbOperation.__connect()

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

        except Exception as e_:
            raise RuntimeError(f"Aggregation failed: {e_}") from e_

        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def get_database_names() -> None:
        client: MongoClient | None = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")
        try:
            databases = client.list_database_names()
            if not databases:
                print('No databases found')
            else:
                print(f'List of databases: {databases}')
        except Exception as ex:
            logging.exception(f'An error occurred while fetching database names: {ex}')
        finally:
            client.close()
            logging.info("MongoDB connection closed")

    @staticmethod
    def get_collection_names(database_name: str) -> None:
        client: MongoClient | None = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")
        try:
            db = client[database_name]
            collections = db.list_collection_names()
            if not collections:
                print("No collections found.")
            else:
                print(f"Collections in '{database_name}': {collections}")
        except Exception as ex:
            logging.exception(f"An error occurred: {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def create_collection(database_name: str, collection_name: str, validator: dict[str, Any] | None = None) -> None:
        client: MongoClient | None = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")

        try:
            db = client[database_name]

            if validator:
                db.create_collection(collection_name, validator = validator)
                print(f"The collection '{collection_name}' was created successfully.")
            else:
                db.create_collection(collection_name)
                print(f"The collection '{collection_name}' was created successfully.")

        except CollectionInvalid:
            logging.warning(f"The collection '{collection_name}' already exists.")

        except Exception as ex:
             logging.exception(f"An error occurred while creating the collection: {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def drop_collection(database_name: str, collection_name: str) -> None:
        client: MongoClient | None = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")
        try:
            db = client[database_name]

            # Check if the collection exists before attempting to drop
            if collection_name in db.list_collection_names():
                db.drop_collection(collection_name)
                print(f"The collection '{collection_name}' was dropped successfully.")
            else:
                print(f"The collection '{collection_name}' does not exist.")
        except Exception as ex:
            logging.exception(f"An error occurred while dropping the collection: {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def fetch_document(database_name: str, collection_name: str) -> None:
        client: MongoClient | None = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")
        try:
            # Check if the database exists first
            if database_name not in client.list_database_names():
                print(f"The database '{database_name}' does not exist.")
                return

            db = client[database_name]

            # Then check if the collection exists
            if collection_name not in db.list_collection_names():
                print(f"The collection '{collection_name}' does not exist in database '{database_name}'.")
                return

            # Fetch documents
            my_collection = db.get_collection(collection_name)
            documents = my_collection.find()

            found = False
            for doc in documents:
                found = True
                for k, v in doc.items():
                    print(f"{k}: {v}")
                print("-" * 30)

            if not found:
                print(f"No documents found in the collection '{collection_name}'.")

        except Exception as ex:
            logging.exception(f"An error occurred while fetching documents: {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def create_database(database_name: str) -> None:
        client: MongoClient | None = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")
        try:
            db = client[database_name]
            # Trigger actual creation by inserting a dummy doc
            db['__temp_collection__'].insert_one({'created': True})
            db.drop_collection('__temp_collection__')  # Clean it up

            if database_name in client.list_database_names():
                print(f"The database '{database_name}' was created successfully.")
            else:
                print(f"Failed to create the database '{database_name}'.")
        except Exception as ex:
            logging.exception(f"An error occurred while creating the database: {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def drop_database(database_name: str) -> None:
        client: MongoClient | None = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")
        try:
            logging.info(f"Trying to drop the database: {database_name}")
            if client:
                client.drop_database(database_name)
                print(f"The database '{database_name}' was dropped successfully.")
        except Exception as ex:
            logging.exception(f"An error occurred while dropping the database '{database_name}': {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def insert_document(database_name: str, collection_name: str, document: dict[str, Any] | list[dict[str, Any]]) -> None:
        client: MongoClient | None = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")
        try:
            db = client[database_name]
            collection = db[collection_name]

            if isinstance(document, dict):
                result_1 = collection.insert_one(document)
                print(f"Document inserted successfully with _id: {result_1.inserted_id}")
            elif isinstance(document, list):
                result_2 = collection.insert_many(document)
                for inserted_id in result_2.inserted_ids:
                    print(f"Document inserted successfully with _id: {inserted_id}")
            else:
                raise ValueError("Document must be a dictionary or a list of dictionaries.")

        except WriteError as we:
            print("\n❌ WriteError: Document failed validation!")
            print("Error Details:")

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
    def update_document(database_name: str, collection_name: str, filter_condition: dict, update_values: dict, update_type: str = "one") -> None:
        client: MongoClient | None = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")
        try:
            db = client[database_name]
            collection = db[collection_name]

            update_operation = {"$set": update_values}

            if update_type.strip().lower() == "one":
                result = collection.update_one(filter_condition, update_operation)
                print(f"Acknowledged {result.acknowledged}, Matched {result.matched_count}, Modified {result.modified_count} (single document).")
            elif update_type.strip().lower() == "many":
                result = collection.update_many(filter_condition, update_operation)
                print(f"Acknowledged {result.acknowledged}, Matched {result.matched_count}, Modified {result.modified_count} (multiple documents).")
            else:
                raise ValueError("Invalid update_type. Use 'one' or 'many'.")


        except PyMongoError as ex:
            logging.exception(f"An error occurred while updating documents: {ex}")
            print(f"Failed to update documents: {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def delete_document(database_name: str, collection_name: str, filter_query: dict[str, Any], update_type: str = "one") -> None:
        client: MongoClient | None = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")
        try:
            db = client[database_name]
            collection = db[collection_name]

            if update_type.strip().lower() == "one":
                result_1 = collection.delete_one(filter_query)
                print(f"Acknowledged {result_1.acknowledged}, Deleted {result_1.deleted_count} document.")
            elif update_type.strip().lower() == "many":
                result_2 = collection.delete_many(filter_query)
                print(f"Acknowledged {result_2.acknowledged}, Deleted {result_2.deleted_count} documents.")
            else:
                raise ValueError("Invalid input. Please enter 'one' or 'many'.")

        except PyMongoError as ex:
            logging.exception(f"An error occurred during deletion: {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

    @staticmethod
    def modify_existing_collection_schema(database_name: str, collection_name: str, validator: dict[str, Any]) -> None:
        client: MongoClient | None = MongoDbOperation.__connect()
        if client is None:
            raise ConnectionError("MongoDB client is None. Could not establish connection.")
        try:
            db = client[database_name]
            db.command({
                "collMod": collection_name,
                "validator": validator,
                "validationLevel": "moderate",
                "validationAction": "warn"
            })
            print(f"Schema modified successfully for collection '{collection_name}'.")
        except PyMongoError as ex:
            logging.exception(f"An error occurred during schema modification: {ex}")
        finally:
            client.close()
            logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    try:
        validation_rule = Pipelines.validator()

        # MongoDbOperation.aggregate_join_collection(pipeline_ = pipeline)
        # MongoDbOperation.fetch_document(database_name='store_db', collection_name='users')
        # MongoDbOperation.insert_document(database_name = 'store_db', collection_name = 'users_1', document = {"name": "awaish", "age": 22, "email": "awaish7@gmail.com"})
        # MongoDbOperation.update_document(database_name='store_db', collection_name='users_1', filter_condition = {"name": "awaish"}, update_values = { "email": "awaish7@gmail.com"}, update_type = 'one')
        # MongoDbOperation.delete_document(database_name='store_db',collection_name='users_1', filter_query={"name": "awaish"}, update_type="one")
        # MongoDbOperation.modify_existing_collection_schema(database_name = 'store_db', collection_name = 'users_1', validator = validation_rule)
        MongoDbOperation.insert_document(database_name='store_db',collection_name='users_1',document={"name":"ayan","age": '17',"email":"sufiyan@123gmail.com"})
    except ConfigurationError:
        logging.error("Could not configure MongoDB client. Check your internet connection.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

