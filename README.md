# ReadMe for CaseStudy

## Overview

This project aims to create a data pipeline for migrating and processing JSON data into a PostgreSql database. The pipeline is designed to read raw data from local JSON file, 
parse and transform it into structed Category, Chain, Hotel objects. After this, insert these obejcts to relational databse tables for further analysis.

## Architecture

The data pipeline has 7 class. 3 of them are data models for keeping data, the other classes used for designing the pipeline.

- Category represents the category of a hotel.
- Chain represents a hotel chain.
- Hotel represents a hotel with a name, category, chain, and location.

I desisgned JsonReader, ObjectCreater, DbProcesser classes as data pipeline components and DataPipeline class represent data pipeline.

JsonReader is a data input layer. It is responsible to read JSON file and return values in the file.
ObjectCreater is a processing layer. It is responsible to transform json values into Python objects using data model classes.
DbProcesser is a database layer. It is responsible to create session for database connection and insert data to database.
DataPipeline is a orchestrator. It is responsible for data flow.

## Design Decisions

- Each component in the pipeline is designed with sole responsibility for SRM(Single Responsibility Principle)
- SQLAlchemy ORM provides an abstraction layer for data models and it provides flexibility for performing database actions.
- Python dataclass decorator is used to simplify model definitions and improve readability.
- The category and chain models are linked to the Hotel model through SQLAlchemy’s relationship and back_populates functions. They provides a two-way navigaiton between these models.
- Python logging used for logging meaningful error messages. This provides easier troubleshoot issues without interrupting the entire pipeline.
- config.ini file used to manage settings for database connection, logging, path inputs. This provides security of code and flexibility.

## Implementation Details

**Data Models**: These data models represent the structure of the tables in the database. Their relationships are defined to ensure data integrity and consistency.
  - Category: category_id is a primary key and it has one-to-many relationship with Hotel.
  - Chanin: chain_id is a primary key and it has one-to-many relationship with Hotel.
  - Hotel: hotel_id(property_id) is a primary key. category_id and chain_id are foreign keys. It has many-to-one relationship with Chain and Category.

**JsonReader**: It handles reading and validating raw data from Json file.
  - It performs error handling to ensure file opened correctly, the json data is not empty.
  - If any errors occured, that error will be logged and raise a meaningfull error.

**ObjectCreater**: It handles transforming row data into Python objects (Category, Chain, Hotel)
  - It extracts fields from the row and maps them to the corresponding data model.
  - It ensures that primary key values are valid integers, the IDs are not negative and other neccessary fields are present and valid for hotel model.
  - It has special logic for location handling. The location data includes coordinates, which may be obfuscated. It checks whether obfuscation is required.

**DbProcesssor**: It handles all actions with the PostgreSQL databse via SQLAlchemy. It establishes a connection using details from config.ini file.
  - With the SQLAlchemy ORM usage, it can add the python class directly to database.
  - It has the session.add_all method in insert_data method that is used to batch insert to avoid partial commits and multiple request to database.

**DataPipeline**: It is the orchestrator of the entire process.
  - It brings together the JsonReader, ObjectCreater, DbProcesssor to run pipeline end-to-end.
  - Reading data with JsonReader.
  - Transform and creating objects with ObjcetCreater.
  - Once objects are created, they are added to a list. Before adding to list, each object are checked for validation and if it is existing in the list.
  - All object in the list are inserted into database in a single commit operation. This minimizes the number of transactions and ensures efficient database operations.

Note: Docker desktop was used to stand up the PostgreSQL database.
  
  
  





