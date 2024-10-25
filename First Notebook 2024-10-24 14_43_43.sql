-- Databricks notebook source
-- MAGIC %md
-- MAGIC #First databricks program
-- MAGIC ##By Amit Sharma

-- COMMAND ----------

-- Select * from factinternetsales_csv limit 10;
select count(*) as total from factinternetsales_csv;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # File location and type
-- MAGIC file_location = "/FileStore/tables/FactInternetSales-1.csv"
-- MAGIC file_type = "csv"
-- MAGIC
-- MAGIC # CSV options
-- MAGIC infer_schema = "false"
-- MAGIC first_row_is_header = "false"
-- MAGIC delimiter = ","
-- MAGIC
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC print(df.count())
-- MAGIC # display(df)
-- MAGIC print(df.show())

-- COMMAND ----------

create database if not exists awsproject;

-- COMMAND ----------

use awsproject;
show tables;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.help()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('/FileStore/tables/FactInternetSales.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #File location and type
-- MAGIC file_location = "dbfs:/FileStore/tables/DimDate.csv"
-- MAGIC file_type = "csv"
-- MAGIC
-- MAGIC #option field
-- MAGIC header = "true"
-- MAGIC infer_schema="true"
-- MAGIC delimiter=","
-- MAGIC
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC     .option("header",header) \
-- MAGIC     .option("inferSchema",infer_schema) \
-- MAGIC     .load(file_location)
-- MAGIC
-- MAGIC # display(df)
-- MAGIC # print(df.count())
-- MAGIC
-- MAGIC #create table from df
-- MAGIC sqlCode = df.createOrReplaceTempView('DimData')
-- MAGIC
-- MAGIC #Sql code 
-- MAGIC

-- COMMAND ----------

select * from DimData limit 2;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format('delta').saveAsTable('Dimdate');

-- COMMAND ----------

create table dimcustomer using csv options(path "/FileStore/tables/DimCustomer.csv", header "true", inferSchema="true");

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #File location and type
-- MAGIC file_location = "dbfs:/FileStore/tables/FactInternetSalesReason.csv"
-- MAGIC file_type = "csv"
-- MAGIC
-- MAGIC #File option
-- MAGIC header_value = "true"
-- MAGIC infer_schema = "true"
-- MAGIC delimiter = ","
-- MAGIC
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC     .option("header",header_value)\
-- MAGIC     .option("inferSchema",infer_schema)\
-- MAGIC     .load(file_location)
-- MAGIC # display(df)
-- MAGIC print(df.show(10))

-- COMMAND ----------

show databases;
use awsproject;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sqlCode = df.createOrReplaceTempView("FactInternetSalesReason")
-- MAGIC # spark.sql("select * from dimCustomer limit 10").show();
-- MAGIC df.write.format('delta').saveAsTable('FactInternetSalesReason')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - ProductCategorykye from dimproductcategory
-- MAGIC - produtsubcategorykey from dimproductsubcategory
-- MAGIC - productkey from dimproduct
-- MAGIC - Englishproductcategoryname renamed as category from dimproductcategory
-- MAGIC - Englishproductsubcategoryname renamed as subcategory from dimproductsubcategory
-- MAGIC - Modelname as Model from dimproduct

-- COMMAND ----------

select * from dimproductsubcategory limit 10;

-- COMMAND ----------

select p.productkey,
s.productsubcategorykey,
c.productcategorykey,
EnglishProductCategoryName as category,
englishproductsubcategoryname as subategory,
modelname as model
from dimproduct as p 
inner join dimproductsubcategory as s
on (p.ProductSubcategoryKey = s.ProductCategoryKey)
inner join dimproductcategory as c
on (s.ProductCategoryKey = c.productcategorykey)
where p.Status = 'Current' Or p.Status = 'NULL'


-- COMMAND ----------

drop table if exists f_productinfi;

-- COMMAND ----------

create table f_productinfo as 
select p.productkey,
s.productsubcategorykey,
c.productcategorykey,
EnglishProductCategoryName as category,
englishproductsubcategoryname as subategory,
modelname as model
from dimproduct as p 
inner join dimproductsubcategory as s
on (p.ProductSubcategoryKey = s.ProductCategoryKey)
inner join dimproductcategory as c
on (s.ProductCategoryKey = c.productcategorykey)
where p.Status = 'Current' Or p.Status = 'NULL'

-- COMMAND ----------

select distinct model,subategory,category,count(*) as numproducts
from f_productinfo
group by model,subategory,category
order by numproducts desc;

-- COMMAND ----------

show create table dimcustomer

-- COMMAND ----------

create or replace view v_customer as 
select CustomerKey,GeographyKey,gender,yearlyincome,BirthDate,TotalChildren,
NumberChildrenAtHome,
EnglishEducation as education,HouseOwnerFlag,
NumberCarsOwned,DateFirstPurchase,
EnglishOccupation as occuption,
CommuteDistance,
int(datediff(current_date(), BirthDate)/365) as age
from dimcustomer

-- COMMAND ----------

select * from v_customer limit 2;

-- COMMAND ----------


