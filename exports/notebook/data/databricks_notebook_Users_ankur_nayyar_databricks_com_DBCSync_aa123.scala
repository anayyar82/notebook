// Databricks notebook source
----------------------------------------------------------------
Tue Aug 28 16:58:59 UTC 2018:
Booting Derby version The Apache Software Foundation - Apache Derby - 10.12.1.1 - (1704137): instance a816c00e-0165-8179-5ee0-000436e0a430 
on database directory memory:/databricks/driver with class loader sun.misc.Launcher$AppClassLoader@18b4aac2 
Loaded from file:/databricks/jars/spark--maven-trees--spark_2.3--org.apache.derby--derby--org.apache.derby__derby__10.12.1.1.jar
java.vendor=Oracle Corporation
java.runtime.version=1.8.0_162-8u162-b12-0ubuntu0.16.04.2-b12
user.dir=/databricks/driver
os.name=Linux
os.arch=amd64
os.version=4.4.0-1062-aws
derby.system.home=null
Database Class Loader started - derby.database.classpath=''

// COMMAND ----------

// MAGIC %python
// MAGIC # Import python packages/libraries
// MAGIC from pyspark.sql.functions import *
// MAGIC from pyspark.sql.types import *
// MAGIC from pyspark.sql import *
// MAGIC row_object = Row('document_text', 'document_properties', 'link', 'pt_batch_id', 'pt_file_id')
// MAGIC document_properties = str({"CreationDate": "D:20170915155236Z", "Producer": "iText 4.2.0 by 1T3XT", "ModDate": "D:20170915155236Z"})
// MAGIC row_list_for_df = [row_object("""\n5 Spaker1: Okay. Uh, \n\nyour height is 6'1", weight 240, which \n\nis a bit heavy and you work full time, don't\n\nyou?""", document_properties, "s3a://amgen-edl-gco-us-analytics-development/cdl/landing/verilogue/verilogue_daily_patient_interactions/120406.pdf", "20180926024542522", "76974")]
// MAGIC d_patient_interactions_verilogue_df = spark.createDataFrame(row_list_for_df)
// MAGIC d_patient_interactions_verilogue_df = d_patient_interactions_verilogue_df.withColumn("pt_file_id", d_patient_interactions_verilogue_df['pt_file_id'].cast(IntegerType()))
// MAGIC d_patient_interactions_verilogue_df.coalesce(1).write.mode("overwrite").option("header", "false").option("sep", "|").option('quote', '"').option('escape', '"').option("multiLine", 'true').csv("/mnt/ankur/pt_file_id=10001")

// COMMAND ----------

// MAGIC %python 
// MAGIC display(d_patient_interactions_verilogue_df)

// COMMAND ----------

