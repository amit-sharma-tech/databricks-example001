-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Demonstrate use of SQL Data Definition Language (DDL) Statements

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Use SQL Data Definition langauge(DDL)

-- COMMAND ----------

create database mydb;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database mydb;

-- COMMAND ----------

describe database extended mydb;

-- COMMAND ----------

alter database mydb set dbproperties (Production=True)

-- COMMAND ----------

create table myTable(name string,age int ,city string)
tblproperties ('created.by.user'='Amit','created.date'='10-24-2024');

-- COMMAND ----------

show tblproperties myTable;

-- COMMAND ----------

show tblproperties myTable (created.by.user);

-- COMMAND ----------

describe table extended myTable;

-- COMMAND ----------

alter table myTable rename to mytable2;

-- COMMAND ----------

alter table mytable2 add columns (title string,DOB timestamp)

-- COMMAND ----------

describe table mytable2;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(_sqldf)

-- COMMAND ----------

alter table mytable2 alter column city comment "address city";

-- COMMAND ----------

describe table mytable2;

-- COMMAND ----------

alter table mytable2 rename to mytable;

-- COMMAND ----------

desc table mytable;

-- COMMAND ----------

show partitions mytable;

-- COMMAND ----------

show databases;

-- COMMAND ----------

show functions;

-- COMMAND ----------

drop database if exists mydb;

-- COMMAND ----------

show databases;

-- COMMAND ----------

create database if not exists mydb;

-- COMMAND ----------

show databases;

-- COMMAND ----------

use mydb;

-- COMMAND ----------

describe database mydb;

-- COMMAND ----------


