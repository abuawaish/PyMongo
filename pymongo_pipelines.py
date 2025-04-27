from typing import Any
class Pipelines:
    @staticmethod
    def pipeline_1() -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = [
            {
                "$group": {
                    "_id": "$fuel_type",
                    "total_cars": {"$sum": 1},
                    "engine_over_1000_cc": {
                        "$sum": {
                            "$cond": [{"$gt": ["$engine.cc", 1000]}, 1, 0]
                        }
                    }
                }
            },
            {
                "$sort": {"_id": 1}
            },
            {
                "$project": {
                    "_id": 0,
                    "fuel_type": {"$toUpper": "$_id"},
                    "total_cars": 1,
                    "engine_over_1000_cc": 1
                }
            }
        ]

        return pipeline

    @staticmethod
    def pipeline_2() -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = [
            {
                "$project": {
                    "_id": 0,
                    "model": 1,
                    "is_diesel": {
                        "$regexMatch": {
                            "input": "$fuel_type",
                            "regex": "Dies"
                        }
                    }
                }
            }
        ]
        return pipeline

    @staticmethod
    def get_cars_data() -> list[dict[str, Any]]:
        cars: list[dict[str, Any]] = [
            {
                "maker": "Hyundai",
                "model": "Creta",
                "fuel_type": "Diesel",
                "transmission": "Manual",
                "engine": {
                    "type": "Naturally Aspirated",
                    "cc": 1493,
                    "torque": "250 Nm"
                },
                "features": ["Sunroof", "Leather Seats", "Wireless Charging", "Ventilated Seats", "Bluetooth"],
                "sunroof": True,
                "airbags": 6,
                "price": 1500000,
                "owners": [
                    {"name": "Raju", "purchase_date": "2021-03-15", "location": "Mumbai"},
                    {"name": "Shyam", "purchase_date": "2023-01-10", "location": "Delhi"}
                ],
                "service_history": [
                    {"date": "2022-04-10", "service_type": "Oil Change", "cost": 5000},
                    {"date": "2023-07-18", "service_type": "Brake Replacement", "cost": 12000}
                ]
            },
            {
                "maker": "Maruti Suzuki",
                "model": "Baleno",
                "fuel_type": "Petrol",
                "transmission": "Automatic",
                "engine": {
                    "type": "Naturally Aspirated",
                    "cc": 1197,
                    "torque": "113 Nm"
                },
                "features": ["Projector Headlamps", "Apple CarPlay", "ABS"],
                "sunroof": False,
                "airbags": 2,
                "price": 800000,
                "owners": [
                    {"name": "Baburao", "purchase_date": "2020-08-22", "location": "Pune"}
                ],
                "service_history": [
                    {"date": "2021-05-12", "service_type": "Tire Rotation", "cost": 2000},
                    {"date": "2022-11-05", "service_type": "Battery Replacement", "cost": 7000}
                ]
            },
            {
                "maker": "Mahindra",
                "model": "XUV500",
                "fuel_type": "Diesel",
                "transmission": "Manual",
                "engine": {
                    "type": "Turbocharged",
                    "cc": 2179,
                    "torque": "360 Nm"
                },
                "features": ["All-Wheel Drive", "Navigation System", "Cruise Control"],
                "sunroof": True,
                "airbags": 6,
                "price": 1800000,
                "owners": [
                    {"name": "Raju", "purchase_date": "2019-11-30", "location": "Bangalore"},
                    {"name": "Shyam", "purchase_date": "2022-02-15", "location": "Hyderabad"}
                ],
                "service_history": [
                    {"date": "2021-02-25", "service_type": "Transmission Repair", "cost": 35000},
                    {"date": "2023-03-10", "service_type": "Tire Replacement", "cost": 15000}
                ]
            },
            {
                "maker": "Honda",
                "model": "City",
                "fuel_type": "Petrol",
                "transmission": "Automatic",
                "engine": {
                    "type": "Naturally Aspirated",
                    "cc": 1498,
                    "torque": "145 Nm"
                },
                "features": ["Keyless Entry", "Auto AC", "Multi-angle Rearview Camera"],
                "sunroof": False,
                "airbags": 4,
                "price": 1200000,
                "owners": [
                    {"name": "Baburao", "purchase_date": "2020-05-20", "location": "Chennai"}
                ],
                "service_history": [
                    {"date": "2021-08-10", "service_type": "Oil Change", "cost": 5000},
                    {"date": "2022-10-25", "service_type": "Brake Replacement", "cost": 10000}
                ]
            },
            {
                "maker": "Tata",
                "model": "Nexon",
                "fuel_type": "Petrol",
                "transmission": "Automatic",
                "engine": {
                    "type": "Turbocharged",
                    "cc": 1199,
                    "torque": "170 Nm"
                },
                "features": ["Touchscreen", "Reverse Camera", "Bluetooth Connectivity"],
                "sunroof": False,
                "airbags": 2,
                "price": 1100000,
                "owners": [
                    {"name": "Raju", "purchase_date": "2021-12-05", "location": "Kolkata"}
                ],
                "service_history": [
                    {"date": "2022-12-01", "service_type": "Oil Change", "cost": 6000},
                    {"date": "2023-06-15", "service_type": "Tire Rotation", "cost": 3000}
                ]
            },
            {
                "maker": "Hyundai",
                "model": "Venue",
                "fuel_type": "Petrol",
                "transmission": "Automatic",
                "engine": {
                    "type": "Turbocharged",
                    "cc": 998,
                    "torque": "172 Nm"
                },
                "features": ["Sunroof", "Touchscreen Infotainment", "Keyless Entry", "Rear Camera", "Cruise Control"],
                "sunroof": True,
                "airbags": 4,
                "price": 1200000,
                "owners": [
                    {"name": "Amit", "purchase_date": "2020-05-20", "location": "Bangalore"},
                    {"name": "Priya", "purchase_date": "2022-11-05", "location": "Chennai"}
                ],
                "service_history": [
                    {"date": "2021-07-15", "service_type": "Oil Change", "cost": 4000},
                    {"date": "2023-03-22", "service_type": "Tire Replacement", "cost": 8000}
                ]
            },
            {
                "maker": "Hyundai",
                "model": "i20",
                "fuel_type": "Petrol",
                "transmission": "Manual",
                "engine": {
                    "type": "Naturally Aspirated",
                    "cc": 1197,
                    "torque": "114 Nm"
                },
                "features": ["Apple CarPlay", "ABS", "Projector Headlamps", "Wireless Charging"],
                "sunroof": False,
                "airbags": 2,
                "price": 900000,
                "owners": [
                    {"name": "Rohit", "purchase_date": "2021-06-15", "location": "Delhi"}
                ],
                "service_history": [
                    {"date": "2022-09-10", "service_type": "Battery Replacement", "cost": 7000},
                    {"date": "2023-05-25", "service_type": "Tire Rotation", "cost": 2500}
                ]
            },
            {
                "maker": "Maruti Suzuki",
                "model": "Swift",
                "fuel_type": "Petrol",
                "transmission": "Manual",
                "engine": {
                    "type": "Naturally Aspirated",
                    "cc": 1198,
                    "torque": "113 Nm"
                },
                "features": ["Touchscreen Infotainment", "ABS", "Keyless Entry", "Rear Parking Sensors"],
                "sunroof": False,
                "airbags": 2,
                "price": 750000,
                "owners": [
                    {"name": "Vijay", "purchase_date": "2019-03-20", "location": "Hyderabad"}
                ],
                "service_history": [
                    {"date": "2020-05-18", "service_type": "Oil Change", "cost": 3000},
                    {"date": "2022-08-10", "service_type": "Brake Replacement", "cost": 5000}
                ]
            },
            {
                "maker": "Tata",
                "model": "Harrier",
                "fuel_type": "Diesel",
                "transmission": "Automatic",
                "engine": {
                    "type": "Turbocharged",
                    "cc": 1956,
                    "torque": "350 Nm"
                },
                "features": ["Panoramic Sunroof", "Leather Upholstery", "Terrain Response System", "Auto-Dimming IRVM"],
                "sunroof": True,
                "airbags": 6,
                "price": 2000000,
                "owners": [
                    {"name": "Deepak", "purchase_date": "2022-01-10", "location": "Mumbai"}
                ],
                "service_history": [
                    {"date": "2022-10-15", "service_type": "Transmission Repair", "cost": 45000},
                    {"date": "2023-04-20", "service_type": "Brake Replacement", "cost": 15000}
                ]
            },
            {
                "maker": "Honda",
                "model": "Amaze",
                "fuel_type": "Diesel",
                "transmission": "Manual",
                "engine": {
                    "type": "Naturally Aspirated",
                    "cc": 1498,
                    "torque": "200 Nm"
                },
                "features": ["Keyless Entry", "Auto AC", "Rear Parking Camera", "Cruise Control"],
                "sunroof": False,
                "airbags": 4,
                "price": 1000000,
                "owners": [
                    {"name": "Anil", "purchase_date": "2020-11-25", "location": "Kolkata"}
                ],
                "service_history": [
                    {"date": "2021-12-10", "service_type": "Oil Change", "cost": 4500},
                    {"date": "2022-08-15", "service_type": "Tire Rotation", "cost": 2500}
                ]
            },
            {
                "maker": "Tata",
                "model": "Nexon EV",
                "fuel_type": "Electric",
                "transmission": "Automatic",
                "engine": {
                    "type": "Electric Motor",
                    "battery_capacity": "30.2 kWh",
                    "torque": "245 Nm"
                },
                "features": ["Touchscreen Infotainment", "Wireless Charging", "Connected Car Tech", "Sunroof"],
                "sunroof": True,
                "airbags": 6,
                "price": 1400000,
                "owners": [
                    {"name": "Vikas", "purchase_date": "2021-05-20", "location": "Bangalore"}
                ],
                "service_history": [
                    {"date": "2022-06-10", "service_type": "Battery Check", "cost": 0},
                    {"date": "2023-03-15", "service_type": "Tire Rotation", "cost": 3000}
                ]
            },
            {
                "maker": "Hyundai",
                "model": "Kona Electric",
                "fuel_type": "Electric",
                "transmission": "Automatic",
                "engine": {
                    "type": "Electric Motor",
                    "battery_capacity": "39.2 kWh",
                    "torque": "395 Nm"
                },
                "features": ["Wireless Charging", "Ventilated Seats", "Sunroof", "Auto AC"],
                "sunroof": True,
                "airbags": 6,
                "price": 2300000,
                "owners": [
                    {"name": "Sneha", "purchase_date": "2022-01-15", "location": "Mumbai"}
                ],
                "service_history": [
                    {"date": "2022-09-10", "service_type": "Battery Check", "cost": 0},
                    {"date": "2023-06-05", "service_type": "Brake Replacement", "cost": 8000}
                ]
            },
            {
                "maker": "Maruti Suzuki",
                "model": "WagonR",
                "fuel_type": "CNG",
                "transmission": "Manual",
                "engine": {
                    "type": "Naturally Aspirated",
                    "cc": 998,
                    "torque": "90 Nm"
                },
                "features": ["Manual AC", "ABS", "Power Windows"],
                "sunroof": False,
                "airbags": 2,
                "price": 600000,
                "owners": [
                    {"name": "Rahul", "purchase_date": "2019-07-22", "location": "Delhi"}
                ],
                "service_history": [
                    {"date": "2020-11-10", "service_type": "CNG Kit Checkup", "cost": 2000},
                    {"date": "2021-08-15", "service_type": "Tire Rotation", "cost": 1500}
                ]
            },
            {
                "maker": "Honda",
                "model": "Amaze",
                "fuel_type": "CNG",
                "transmission": "Manual",
                "engine": {
                    "type": "Naturally Aspirated",
                    "cc": 1199,
                    "torque": "110 Nm"
                },
                "features": ["Keyless Entry", "Auto AC", "Rear Parking Camera", "Cruise Control"],
                "sunroof": False,
                "airbags": 4,
                "price": 800000,
                "owners": [
                    {"name": "Sanjay", "purchase_date": "2021-03-18", "location": "Pune"}
                ],
                "service_history": [
                    {"date": "2021-09-10", "service_type": "CNG Kit Checkup", "cost": 2500},
                    {"date": "2022-05-15", "service_type": "Oil Change", "cost": 3500}
                ]
            }
        ]
        return  cars

    @staticmethod
    def pipeline_3() -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = [
            {
                "$group": {
                    "_id": "$model",
                    "average_price": { "$avg": "$price" }
                }
            },
            {
                "$sort": {"_id": 1}
            },
            {
                "$project": {
                    "_id": 0,
                    "model": "$_id",
                    "average_price": 1
                }
            }
        ]
        return pipeline

    @staticmethod
    def pipeline_4() -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = [
            {
                "$match": {
                    "maker": "Hyundai"
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "car_name": {
                        "$toUpper": {
                            "$concat": ["maker", " ", "model"]
                        }
                    }
                }
            },
            {
                "$out": "hyundai_cars"
            }
        ]
        return pipeline

    @staticmethod
    def pipeline_5() -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = [
            {
                "$project": {
                    "_id": 0,
                    "model": 1,
                    "new_price": {
                        "$add":["$price", 55000]
                    }
                }
            }
        ]
        return pipeline

    @staticmethod
    def pipeline_6() -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = [
            {
                "$project":{
                    "_id": 0,
                    "model": 1,
                    "price": 1
                }
            },

            {
                "$addFields":{
                    "price_in_lakhs":{
                        "$concat":[
                            {
                                "$toString":{
                                    "$divide": ["$price", 100000]
                                }
                            },
                            " lakhs"
                        ]
                    }
                }
            }
        ]
        return pipeline

    @staticmethod
    def pipeline_7() -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = [
            {
                "$match": {
                    "maker": "Hyundai"
                }
            },

            {
                "$set":{
                    "total_service_cost": {
                        "$sum": "$service_history.cost"
                    }
                }
            },

            {
                "$project": {
                    "_id": 0,
                    "model": 1,
                    "total_service_cost": 1
                }
            }
        ]
        return pipeline

    @staticmethod
    def pipeline_8() -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = [
            {
                "$project":{
                    "_id": 0,
                    "maker": 1,
                    "model": 1,
                    "fuel_cat":{
                        "$cond":{
                            "if": {"$eq": ["$fuel_type","Petrol"]},
                            "then": "Petrol_car",
                            "else": "Non_petrol_car"
                        }
                    }
                }
            }
        ]
        return pipeline

    @staticmethod
    def pipeline_9() -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = [
            {
                "$project": {
                    "_id": 0,
                    "maker": 1,
                    "model": 1,
                    "budget_cat": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$lt": ["$price", 500000]},
                                        "then": "Budget"
                                },

                                {
                                    "case": {"$and": [{"$gte": ["$price", 500000]}, {"$lt": ["$price", 1000000]}]},
                                        "then": "Mid_range"
                                },

                                {
                                    "case": {"$gte": ["$price", 1000000]},
                                    "then": "Premium"
                                }
                            ],

                            "default": "Unknown"
                        }
                    }
                }
            }
        ]
        return pipeline

    @staticmethod
    def pipeline_10() -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = [
            {
                "$match" : {"maker": "Hyundai"}
            },

            {
                "$set": {
                    "total_service_cost": {
                        "$sum": "$service_history.cost"
                    }
                }
            },

            {
                "$project":{
                    "maker": 1,
                    "model": 1,
                    "_id": 0,
                    "total_service_cost": 1,
                    "cost_status": {
                        "$let": {
                            "vars": {"total_cost": "$total_service_cost"}, # total_cost is user define variable
                            "in": {
                                "$cond": {
                                    "if": {"$gte": ["$$total_cost", 10000]},
                                    "then": "High",
                                    "else": "Low"
                                }
                            }
                        }
                    }
                }
            }
        ]
        return pipeline

    @staticmethod
    def get_users_data() -> list[dict[str, Any]]:
        users: list[dict[str, Any]] = [
            {
                "_id": "user1",
                "name": "Amit Sharma",
                "email": "amit.sharma@example.com",
                "phone": "+91-987654210",
                "address": "MG Road, Mumbai, Maharashtra"
            },
            {
                "_id": "user2",
                "name": "Priya Verma",
                "email": "priya.verma@example.com",
                "phone": "+91-987654211",
                "address": "Nehru Place, New Delhi, Delhi"
            },
            {
                "_id": "user3",
                "name": "Rahul Singh",
                "email": "rahul.singh@example.com",
                "phone": "+91-987654212",
                "address": "Sector 18, Noida, Uttar Pradesh"
            },
            {
                "_id": "user4",
                "name": "Anjali Nair",
                "email": "anjali.nair@example.com",
                "phone": "+91-987654213",
                "address": "Marine Drive, Kochi, Kerala"
            },
            {
                "_id": "user5",
                "name": "Vikram Desai",
                "email": "vikram.desai@example.com",
                "phone": "+91-987654214",
                "address": "Park Street, Kolkata, West Bengal"
            }
        ]
        return users

    @staticmethod
    def get_orders_data() -> list[dict[str, Any]]:
        orders: list[dict[str, Any]] = [
            {
                "_id": "order1",
                "user_id": "user1",
                "product": "Laptop",
                "amount": 50000,
                "order_date": "2024-08-01"
            },
            {
                "_id": "order2",
                "user_id": "user2",
                "product": "Mobile Phone",
                "amount": 15000,
                "order_date": "2024-08-05"
            },
            {
                "_id": "order3",
                "user_id": "user1",
                "product": "Headphones",
                "amount": 2000,
                "order_date": "2024-08-10"
            },
            {
                "_id": "order4",
                "user_id": "user3",
                "product": "Tablet",
                "amount": 25000,
                "order_date": "2024-08-12"
            },
            {
                "_id": "order5",
                "user_id": "user4",
                "product": "Smart Watch",
                "amount": 8000,
                "order_date": "2024-08-15"
            }
        ]
        return orders

    @staticmethod
    def join_pipeline() -> list[dict[str, Any]]:
        pipeline: list[dict[str, Any]] = [
            {
                "$lookup": {
                    "from": "orders",
                    "localField": "_id",
                    "foreignField": "user_id",
                    "as": "orders"
                }
            }
        ]
        return pipeline

    @staticmethod
    def validator() -> dict[str, Any]:
        validation_rule: dict[str, Any] = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name", "age"],
                "properties": {
                    "name": {
                        "bsonType": "string",
                        "description": "must be a string and is required"
                    },
                    "age": {
                        "bsonType": "int",
                        "minimum": 18,
                        "description": "must be greater than or equal to 18 and is required"
                    },
                    "email": {
                        "bsonType": "string",
                        "pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$",
                        "description": "must be a valid email format"
                    }
                }
            }
        }
        return validation_rule
