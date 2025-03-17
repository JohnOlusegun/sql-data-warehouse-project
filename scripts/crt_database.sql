/*

=========================================================
Create Database and Schamss
=========================================================

Scripts Purpose:
  This Scripts will first check if a database with the name "DataWarehous" exists. after checking if it does exists,
  it then drop and recreate a new one. it wuill also create 3 new schemas for better organization and smooth workflow: "Bronze", "Silver", "Gold".

Note:
  Running this script directly will drop the entire "DataWarehous" database if exists.
  All data will be lost. Kindly proceed carefully and ensure you have a proper backups before running this scripts. Thanks.

======================================================
*/


--- Create Database "DataWareHouse"
USE master;
go

-- drop and recreate the 'DataWarehous' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehous')
BEGIN
	ALTER DATABASE DataWarehous SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehous;
END;
GO

-- Create the 'DataWarehous' database
CREATE DATABASE DataWarehous;
GO

Use DataWarehous;
GO

-- Create Bronze Schema
CREATE SCHEMA bronze;
GO

--Create Silver Schema
CREATE SCHEMA silver;
GO

-- Create Gold  Schema
CREATE SCHEMA gold;
GO

