/*
============================================================
Create Database and Schemas
============================================================

Script Purpose:
    This script first drops the database (if exist) and creates a new database named 'DataWareHouse'.
    Additionally, the script sets up 3 schemas with-in the database: 'gold', 'silver', and 'bronze'.

Note:
    This script will drop the entire database 'DataWareHouse' if exists.
*/



--- Create the 'DataWareHouse' database
DROP DATABASE IF EXISTS DataWareHouse;
CREATE DATABASE DataWareHouse;

--- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;