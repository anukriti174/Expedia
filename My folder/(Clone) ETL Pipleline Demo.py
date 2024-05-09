# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ## ETL Pipeline Demo

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import date
from pyspark.sql import functions as F 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Extracting the Data from an excel file

# COMMAND ----------

# Read the Excel file
path = "/Volumes/zenogroup/default/fileupload/ETLDemoPipeline/2024/"

# Loading the dataset Employee Data
Ed_spark = spark.read.format("csv").option("inferSchema", "true").option("Header", "true").load(path + "2024_05_04_EmployeeData.csv")

# COMMAND ----------

#Ed_spark = spark.read.format("csv").option("inferSchema", "true").option("Header", "true").load(path + "2024_05_04_EmployeeData.csv")

# COMMAND ----------

display(Ed_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transforming data by adding a 'LoadDate' column

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

#Ed['LoadDate'] = date.today()

Ed_spark = (Ed_spark
            .withColumn("LoadDate", F.current_date())
            .withColumnRenamed('Employee ID', 'Emp_id')
            .withColumnRenamed('Role ', 'Role')
            .withColumn("StartDate", F.to_date(F.col("StartDate"), 'M/d/yy'))
            )
display(Ed_spark)

# COMMAND ----------

Ed_spark.columns

# COMMAND ----------

Ed_spark.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("anukriti_saxena.test_ETL_spark")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from anukriti_saxena.test_etl_spark

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail anukriti_saxena.test_etl_spark

# COMMAND ----------

dbutils.fs.ls("dbfs:/")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load the data into delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC create database anukriti_saxena

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Test SQL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from default.expedia_dataset_expanded

# COMMAND ----------


