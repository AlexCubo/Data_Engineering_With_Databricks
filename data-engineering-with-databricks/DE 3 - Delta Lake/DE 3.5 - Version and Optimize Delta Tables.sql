-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Version, Optimize, Vacuum in Delta Lake
-- MAGIC
-- MAGIC Now that you feel comfortable performing basic data tasks with Delta Lake, we can discuss a few features unique to Delta Lake.
-- MAGIC
-- MAGIC Note that while some of the keywords used here aren't part of standard ANSI SQL, all Delta Lake operations can be run on Databricks using SQL
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC * Use **`OPTIMIZE`** to compact small files
-- MAGIC * Use **`ZORDER`** to index tables
-- MAGIC * Describe the directory structure of Delta Lake files
-- MAGIC * Review a history of table transactions
-- MAGIC * Query and roll back to previous table version
-- MAGIC * Clean up stale data files with **`VACUUM`**
-- MAGIC
-- MAGIC **Resources**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Databricks Docs</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Delta Vacuum - Databricks Docs</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Run Setup
-- MAGIC The first thing we're going to do is run a setup script. It will define a username, userhome, and schema that is scoped to each user.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Creating a Delta Table with History
-- MAGIC
-- MAGIC As you're waiting for this query to run, see if you can identify the total number of transactions being executed.

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     spark.sql(
-- MAGIC         f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000000.json`"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### LOG
-- MAGIC {
-- MAGIC     "createdTime": 1731310950271,
-- MAGIC     "format": {
-- MAGIC         "provider": "parquet"
-- MAGIC     },
-- MAGIC     "id": "b1859dc8-e55e-4791-b065-dcfada0e7334",
-- MAGIC     "partitionColumns": [],
-- MAGIC     "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}"
-- MAGIC }
-- MAGIC

-- COMMAND ----------

--------------------------------------------------------------------------------------
INSERT INTO students VALUES (1, "Yve", 1.0);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     spark.sql(
-- MAGIC         f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000001.json`"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### LOG
-- MAGIC `{
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731311664000,
-- MAGIC     "path": "part-00000-042d9e33-ce7b-46b9-b3c5-4bd54975cbaf-c000.snappy.parquet",
-- MAGIC     "size": 1055,
-- MAGIC     "stats": "{\"numRecords\":1,\"minValues\":{\"id\":1,\"name\":\"Yve\",\"value\":1.0},\"maxValues\":{\"id\":1,\"name\":\"Yve\",\"value\":1.0},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731311664000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731311664000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731311664000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }`

-- COMMAND ----------

--------------------------------------------------------------------------------------
INSERT INTO students VALUES (2, "Omar", 2.5);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000002.json`"
-- MAGIC   )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Log
-- MAGIC `{
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731311965000,
-- MAGIC     "path": "part-00000-d096ea83-f836-4f7d-b802-93f3283f9904-c000.snappy.parquet",
-- MAGIC     "size": 1063,
-- MAGIC     "stats": "{\"numRecords\":1,\"minValues\":{\"id\":2,\"name\":\"Omar\",\"value\":2.5},\"maxValues\":{\"id\":2,\"name\":\"Omar\",\"value\":2.5},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731311965000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731311965000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731311965000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }`

-- COMMAND ----------

--------------------------------------------------------------------------------------
INSERT INTO students VALUES (3, "Elia", 3.3);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   spark.sql(
-- MAGIC     f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000003.json`"
-- MAGIC   )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Log
-- MAGIC `{
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731312813000,
-- MAGIC     "path": "part-00000-95682af0-ffca-42cf-930f-514d111941e9-c000.snappy.parquet",
-- MAGIC     "size": 1063,
-- MAGIC     "stats": "{\"numRecords\":1,\"minValues\":{\"id\":3,\"name\":\"Elia\",\"value\":3.3},\"maxValues\":{\"id\":3,\"name\":\"Elia\",\"value\":3.3},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731312813000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731312813000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731312813000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }`

-- COMMAND ----------

--------------------------------------------------------------------------------------
INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display (
-- MAGIC   spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000004.json`"
-- MAGIC             )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Log
-- MAGIC `{
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731313275000,
-- MAGIC     "path": "part-00000-fde0d36c-8bfc-4b22-9aaf-466237117de3-c000.snappy.parquet",
-- MAGIC     "size": 1089,
-- MAGIC     "stats": "{\"numRecords\":3,\"minValues\":{\"id\":4,\"name\":\"Ted\",\"value\":4.7},\"maxValues\":{\"id\":6,\"name\":\"Vini\",\"value\":6.3},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731313275000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731313275000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731313275000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }`

-- COMMAND ----------

--------------------------------------------------------------------------------------
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000005.json`")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Log
-- MAGIC `{
-- MAGIC     "dataChange": true,
-- MAGIC     "deletionTimestamp": 1731313591634,
-- MAGIC     "extendedFileMetadata": true,
-- MAGIC     "path": "part-00000-fde0d36c-8bfc-4b22-9aaf-466237117de3-c000.snappy.parquet",
-- MAGIC     "size": 1089,
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731313275000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731313275000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731313275000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731313591000,
-- MAGIC     "path": "part-00000-37459b92-8997-4120-9a96-35da13c4ca12-c000.snappy.parquet",
-- MAGIC     "size": 1089,
-- MAGIC     "stats": "{\"numRecords\":3,\"minValues\":{\"id\":4,\"name\":\"Ted\",\"value\":5.7},\"maxValues\":{\"id\":6,\"name\":\"Vini\",\"value\":6.5},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731313275000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731313275000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731313275000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }`

-- COMMAND ----------

SELECT * FROM students
ORDER BY id;

-- COMMAND ----------

--------------------------------------------------------------------------------------
DELETE FROM students 
WHERE value > 6;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000006.json`")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Log
-- MAGIC `{
-- MAGIC     "dataChange": true,
-- MAGIC     "deletionTimestamp": 1731315889614,
-- MAGIC     "extendedFileMetadata": true,
-- MAGIC     "path": "part-00000-37459b92-8997-4120-9a96-35da13c4ca12-c000.snappy.parquet",
-- MAGIC     "size": 1089,
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731313275000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731313275000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731313275000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731315889000,
-- MAGIC     "path": "part-00000-96bbce09-0b28-416c-9ed0-dd1975168541-c000.snappy.parquet",
-- MAGIC     "size": 1055,
-- MAGIC     "stats": "{\"numRecords\":1,\"minValues\":{\"id\":4,\"name\":\"Ted\",\"value\":5.7},\"maxValues\":{\"id\":4,\"name\":\"Ted\",\"value\":5.7},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731313275000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731313275000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731313275000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }`

-- COMMAND ----------

--------------------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");

-- COMMAND ----------

--------------------------------------------------------------------------------------
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     spark.sql(
-- MAGIC         f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Log
-- MAGIC `{
-- MAGIC     "dataChange": true,
-- MAGIC     "deletionTimestamp": 1731316329343,
-- MAGIC     "extendedFileMetadata": true,
-- MAGIC     "path": "part-00000-95682af0-ffca-42cf-930f-514d111941e9-c000.snappy.parquet",
-- MAGIC     "size": 1063,
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731312813000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731312813000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731312813000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "deletionTimestamp": 1731316329343,
-- MAGIC     "extendedFileMetadata": true,
-- MAGIC     "path": "part-00000-d096ea83-f836-4f7d-b802-93f3283f9904-c000.snappy.parquet",
-- MAGIC     "size": 1063,
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731311965000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731311965000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731311965000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731316329000,
-- MAGIC     "path": "part-00000-5a99b760-fe58-4356-9721-83fa2eb140fc-c000.snappy.parquet",
-- MAGIC     "size": 1063,
-- MAGIC     "stats": "{\"numRecords\":1,\"minValues\":{\"id\":2,\"name\":\"Omar\",\"value\":15.2},\"maxValues\":{\"id\":2,\"name\":\"Omar\",\"value\":15.2},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731316329000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731316329000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731311965000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731316329000,
-- MAGIC     "path": "part-00002-8dc11e53-1d03-4aa6-9a94-a6767c9c3a35-c000.snappy.parquet",
-- MAGIC     "size": 1063,
-- MAGIC     "stats": "{\"numRecords\":1,\"minValues\":{\"id\":7,\"name\":\"Blue\",\"value\":7.7},\"maxValues\":{\"id\":7,\"name\":\"Blue\",\"value\":7.7},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731316329000001",
-- MAGIC         "MAX_INSERTION_TIME": "1731316329000001",
-- MAGIC         "MIN_INSERTION_TIME": "1731311965000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### All logs together
-- MAGIC #### Notice: The name of snapshots are different because cleaning and re-running the notebook results in new parquet files generated by databrics. However the underlyining logic remains the same.
-- MAGIC
-- MAGIC 1. student table is created
-- MAGIC `{
-- MAGIC     "createdTime": 1731310950271,
-- MAGIC     "format": {
-- MAGIC         "provider": "parquet"
-- MAGIC     },
-- MAGIC     "id": "b1859dc8-e55e-4791-b065-dcfada0e7334",
-- MAGIC     "partitionColumns": [],
-- MAGIC     "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}]}"
-- MAGIC }
-- MAGIC
-- MAGIC 2. Insert transaction
-- MAGIC   * id 1, Yve is added
-- MAGIC   * The snapshot: "part-00000-042d9e33-ce7b-46b9-b3c5-4bd54975cbaf-c000.snappy.parquet" is created
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731311664000,
-- MAGIC     "path": "part-00000-042d9e33-ce7b-46b9-b3c5-4bd54975cbaf-c000.snappy.parquet",
-- MAGIC     "size": 1055,
-- MAGIC     "stats": "{\"numRecords\":1,\"minValues\":{\"id\":1,\"name\":\"Yve\",\"value\":1.0},\"maxValues\":{\"id\":1,\"name\":\"Yve\",\"value\":1.0},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731311664000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731311664000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731311664000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC 3. Insert transaction
-- MAGIC   * id 2, Omar is added
-- MAGIC   * Snapshot "part-00000-d096ea83-f836-4f7d-b802-93f3283f9904-c000.snappy.parquet" is created
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731311965000,
-- MAGIC     "path": "part-00000-d096ea83-f836-4f7d-b802-93f3283f9904-c000.snappy.parquet",
-- MAGIC     "size": 1063,
-- MAGIC     "stats": "{\"numRecords\":1,\"minValues\":{\"id\":2,\"name\":\"Omar\",\"value\":2.5},\"maxValues\":{\"id\":2,\"name\":\"Omar\",\"value\":2.5},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731311965000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731311965000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731311965000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC 4. Insert transaction
-- MAGIC   * id 3, Elia is added
-- MAGIC   * Snapshot "part-00000-95682af0-ffca-42cf-930f-514d111941e9-c000.snappy.parquet" is created
-- MAGIC   * Notice: If you display(dbutils.fs.ls(f"{DA.paths.user_db}/students})) you will see that this last snapshot is listed before than snapshot at point 3. This is because the snapshots are listed alphabetically and not in order of creation. Therefore, never think that the last listed snapshot represents the last version of the table. It is not always the case.
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731312813000,
-- MAGIC     "path": "part-00000-95682af0-ffca-42cf-930f-514d111941e9-c000.snappy.parquet",
-- MAGIC     "size": 1063,
-- MAGIC     "stats": "{\"numRecords\":1,\"minValues\":{\"id\":3,\"name\":\"Elia\",\"value\":3.3},\"maxValues\":{\"id\":3,\"name\":\"Elia\",\"value\":3.3},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731312813000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731312813000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731312813000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC 5. Insert transaction
-- MAGIC   * id 4 (Ted), 5 (Tiffany), and 6 (Vini) are added
-- MAGIC   * Snapshot "part-00000-fde0d36c-8bfc-4b22-9aaf-466237117de3-c000.snappy.parquet" is created
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731313275000,
-- MAGIC     "path": "part-00000-fde0d36c-8bfc-4b22-9aaf-466237117de3-c000.snappy.parquet",
-- MAGIC     "size": 1089,
-- MAGIC     "stats": "{\"numRecords\":3,\"minValues\":{\"id\":4,\"name\":\"Ted\",\"value\":4.7},\"maxValues\":{\"id\":6,\"name\":\"Vini\",\"value\":6.3},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731313275000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731313275000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731313275000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC 6. Update transaction
-- MAGIC   * It consists of 2 steps: a delete step and an insert step
-- MAGIC   * delete step: the snapshot "part-00000-fde0d36c-8bfc-4b22-9aaf-466237117de3-c000.snappy.parquet" is deleted -> it means that records (4, Ted, 4.7),  (5, Tiffany, 5.5), and (6, Vini, 6.3) are deleted
-- MAGIC   * insert step: the new snapshot "part-00000-37459b92-8997-4120-9a96-35da13c4ca12-c000.snappy.parquet" is created -> here is added id [(4, Ted, 5,7), (5, Tiffany, 6.5), (6, Vini, 6.3)
-- MAGIC   * Notice: The complete previous transaction is deleted and recreated with new values for Ted_value and Tiffany_value (while Vini_value remains the same)
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "deletionTimestamp": 1731313591634,
-- MAGIC     "extendedFileMetadata": true,
-- MAGIC     "path": "part-00000-fde0d36c-8bfc-4b22-9aaf-466237117de3-c000.snappy.parquet",
-- MAGIC     "size": 1089,
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731313275000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731313275000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731313275000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731313591000,
-- MAGIC     "path": "part-00000-37459b92-8997-4120-9a96-35da13c4ca12-c000.snappy.parquet",
-- MAGIC     "size": 1089,
-- MAGIC     "stats": "{\"numRecords\":3,\"minValues\":{\"id\":4,\"name\":\"Ted\",\"value\":5.7},\"maxValues\":{\"id\":6,\"name\":\"Vini\",\"value\":6.5},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731313275000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731313275000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731313275000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC 7. Deletion transaction
-- MAGIC   * It consists in two steps: a delete step and an insert step
-- MAGIC   * delete step: The snapshot "part-00000-37459b92-8997-4120-9a96-35da13c4ca12-c000.snappy.parquet" is deleted. Logically this happens because we want to delete all records whose value is >= 6. Such records have been added in the previous transaction. That's why now this snapshot is deleted
-- MAGIC   * insert step: the snapshot "part-00000-96bbce09-0b28-416c-9ed0-dd1975168541-c000.snappy.parquet" is created. Here, the record (4, Ted, 5.7) is added again in the table
-- MAGIC   * Notice: Databricks deletes a whole snapshot corresponding to a transaction and not single records. Threfore, if during the deletion of the transaction some records have been deleted which should have not been deleted, databricks performs an insert to insert again these records.
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "deletionTimestamp": 1731315889614,
-- MAGIC     "extendedFileMetadata": true,
-- MAGIC     "path": "part-00000-37459b92-8997-4120-9a96-35da13c4ca12-c000.snappy.parquet",
-- MAGIC     "size": 1089,
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731313275000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731313275000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731313275000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731315889000,
-- MAGIC     "path": "part-00000-96bbce09-0b28-416c-9ed0-dd1975168541-c000.snappy.parquet",
-- MAGIC     "size": 1055,
-- MAGIC     "stats": "{\"numRecords\":1,\"minValues\":{\"id\":4,\"name\":\"Ted\",\"value\":5.7},\"maxValues\":{\"id\":4,\"name\":\"Ted\",\"value\":5.7},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731313275000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731313275000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731313275000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC 8. Merge transaction
-- MAGIC   * Merge transaction consists of many steps. In tis case are 4 steps: delete 1, delete 2, insert 1, insert 2
-- MAGIC   * delete 1: The snapshot "part-00000-95682af0-ffca-42cf-930f-514d111941e9-c000.snappy.parquet is deleted. This was the snapshot where (3, Elia, 3.3) was added. Therefore, this delete step correspond to the "WHEN MATCHED AND u.type = "delete"
-- MAGIC   THEN DELETE" part of Merge operation
-- MAGIC   * delete 2: The snapshot "part-00000-d096ea83-f836-4f7d-b802-93f3283f9904-c000.snappy.parquet" is deleted. This was the snapshot where (2, Omar, 2.5) was added. Therefore, this delete step corresponds to the deletion part of the update command "WHEN MATCHED AND u.type = "update" THEN UPDATE SET *" in the Merge clause
-- MAGIC   * insert 1: The snapshot "part-00000-5a99b760-fe58-4356-9721-83fa2eb140fc-c000.snappy.parquet" is added. This insert corresponds to the insertion part of the update command "WHEN MATCHED AND u.type = "update" THEN UPDATE SET *" in the Merge clause. Basically, in this step the record (2, Omar, 15.2) is added
-- MAGIC   * insert 2: The snapshot "part-00002-8dc11e53-1d03-4aa6-9a94-a6767c9c3a35-c000.snappy.parquet" is added. This corresponds to the "WHEN NOT MATCHED AND u.type = "insert" THEN INSERT *" in the Merge clause. In this step the record (7, Blue, 7.7) is added.
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "deletionTimestamp": 1731316329343,
-- MAGIC     "extendedFileMetadata": true,
-- MAGIC     "path": "part-00000-95682af0-ffca-42cf-930f-514d111941e9-c000.snappy.parquet",
-- MAGIC     "size": 1063,
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731312813000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731312813000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731312813000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "deletionTimestamp": 1731316329343,
-- MAGIC     "extendedFileMetadata": true,
-- MAGIC     "path": "part-00000-d096ea83-f836-4f7d-b802-93f3283f9904-c000.snappy.parquet",
-- MAGIC     "size": 1063,
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731311965000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731311965000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731311965000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731316329000,
-- MAGIC     "path": "part-00000-5a99b760-fe58-4356-9721-83fa2eb140fc-c000.snappy.parquet",
-- MAGIC     "size": 1063,
-- MAGIC     "stats": "{\"numRecords\":1,\"minValues\":{\"id\":2,\"name\":\"Omar\",\"value\":15.2},\"maxValues\":{\"id\":2,\"name\":\"Omar\",\"value\":15.2},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731316329000000",
-- MAGIC         "MAX_INSERTION_TIME": "1731316329000000",
-- MAGIC         "MIN_INSERTION_TIME": "1731311965000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }
-- MAGIC
-- MAGIC {
-- MAGIC     "dataChange": true,
-- MAGIC     "modificationTime": 1731316329000,
-- MAGIC     "path": "part-00002-8dc11e53-1d03-4aa6-9a94-a6767c9c3a35-c000.snappy.parquet",
-- MAGIC     "size": 1063,
-- MAGIC     "stats": "{\"numRecords\":1,\"minValues\":{\"id\":7,\"name\":\"Blue\",\"value\":7.7},\"maxValues\":{\"id\":7,\"name\":\"Blue\",\"value\":7.7},\"nullCount\":{\"id\":0,\"name\":0,\"value\":0}}",
-- MAGIC     "tags": {
-- MAGIC         "INSERTION_TIME": "1731316329000001",
-- MAGIC         "MAX_INSERTION_TIME": "1731316329000001",
-- MAGIC         "MIN_INSERTION_TIME": "1731311965000000",
-- MAGIC         "OPTIMIZE_TARGET_SIZE": "268435456"
-- MAGIC     }
-- MAGIC }`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Examine Table Details
-- MAGIC
-- MAGIC Databricks uses a Hive metastore by default to register schemas, tables, and views.
-- MAGIC
-- MAGIC Using **`DESCRIBE EXTENDED`** allows us to see important metadata about our table.

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`** is another command that allows us to explore table metadata.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note the **`Location`** field.
-- MAGIC
-- MAGIC While we've so far been thinking about our table as just a relational entity within a schema, a Delta Lake table is actually backed by a collection of files stored in cloud object storage.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Explore Delta Lake Files
-- MAGIC
-- MAGIC We can see the files backing our Delta Lake table by using a Databricks Utilities function.
-- MAGIC
-- MAGIC **NOTE**: It's not important right now to know everything about these files to work with Delta Lake, but it will help you gain a greater appreciation for how the technology is implemented.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(DA.paths.user_db)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note that our directory contains a number of Parquet data files and a directory named **`_delta_log`**.
-- MAGIC
-- MAGIC Records in Delta Lake tables are stored as data in Parquet files.
-- MAGIC
-- MAGIC Transactions to Delta Lake tables are recorded in the **`_delta_log`**.
-- MAGIC
-- MAGIC We can peek inside the **`_delta_log`** to see more.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Each transaction results in a new JSON file being written to the Delta Lake transaction log. Here, we can see that there are 8 total transactions against this table (Delta Lake is 0 indexed).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Reasoning about Data Files
-- MAGIC
-- MAGIC We just saw a lot of data files for what is obviously a very small table.
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`** allows us to see some other details about our Delta table, including the number of files.

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Here we see that our table currently contains 4 data files in its present version. So what are all those other Parquet files doing in our table directory? 
-- MAGIC
-- MAGIC Rather than overwriting or immediately deleting files containing changed data, Delta Lake uses the transaction log to indicate whether or not files are valid in a current version of the table.
-- MAGIC
-- MAGIC Here, we'll look at the transaction log corresponding the **`MERGE`** statement above, where records were inserted, updated, and deleted.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     spark.sql(
-- MAGIC         f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000000.json`"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC The **`add`** column contains a list of all the new files written to our table; the **`remove`** column indicates those files that no longer should be included in our table.
-- MAGIC
-- MAGIC When we query a Delta Lake table, the query engine uses the transaction logs to resolve all the files that are valid in the current version, and ignores all other data files.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Compacting Small Files and Indexing
-- MAGIC
-- MAGIC Small files can occur for a variety of reasons; in our case, we performed a number of operations where only one or several records were inserted.
-- MAGIC
-- MAGIC Files will be combined toward an optimal size (scaled based on the size of the table) by using the **`OPTIMIZE`** command.
-- MAGIC
-- MAGIC **`OPTIMIZE`** will replace existing data files by combining records and rewriting the results.
-- MAGIC
-- MAGIC When executing **`OPTIMIZE`**, users can optionally specify one or several fields for **`ZORDER`** indexing. While the specific math of Z-order is unimportant, it speeds up data retrieval when filtering on provided fields by colocating data with similar values within data files.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000008.json`")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * OPTIMIZE with ZORDER BY introduces a new transaction (00000000000000000008)
-- MAGIC * This new transaction consists of 4 delete steps and 1 insert step
-- MAGIC * In the 4 delete steps are deleted all parquet snapshots corresponding to the latest layout of the table:
-- MAGIC   1. delete the snapshot of transaction 2: insert (1, Yve, 1)
-- MAGIC   2. delete the snapshot of transaction 7: re-insert (4, Ted, 5.7) after deletion step
-- MAGIC   3. delete the snapshot in Merge transaction 8: update (2, Omar, 15.2)
-- MAGIC   4. delete the snapshot in Merge transaction 8: insert (7, Bule, 7.7)
-- MAGIC * In the insert step the records of the latest version of the table are re-inserted. But now the distribution of the data is optimized according to the ZORDER BY clustering. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Given how small our data is, **`ZORDER`** does not provide any benefit, but we can see all of the metrics that result from this operation.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Reviewing Delta Lake Transactions
-- MAGIC
-- MAGIC Because all changes to the Delta Lake table are stored in the transaction log, we can easily review the <a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">table history</a>.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW history_students_tvw AS
DESCRIBE HISTORY students;

SELECT * FROM history_students_tvw
ORDER BY version;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC As expected, **`OPTIMIZE`** created another version of our table, meaning that version 8 is our most current version.
-- MAGIC
-- MAGIC Remember all of those extra data files that had been marked as removed in our transaction log? These provide us with the ability to query previous versions of our table.
-- MAGIC
-- MAGIC These time travel queries can be performed by specifying either the integer version or a timestamp.
-- MAGIC
-- MAGIC **NOTE**: In most cases, you'll use a timestamp to recreate data at a time of interest. For our demo we'll use version, as this is deterministic (whereas you may be running this demo at any time in the future).

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3
ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC What's important to note about time travel is that we're not recreating a previous state of the table by undoing transactions against our current version; rather, we're just querying all those data files that were indicated as valid as of the specified version.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Rollback Versions
-- MAGIC
-- MAGIC Suppose you're typing up query to manually delete some records from a table and you accidentally execute this query in the following state.

-- COMMAND ----------

DELETE FROM students

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000009.json`")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Notice about "DELETE FROM students"
-- MAGIC * We have a new transaction where the snapshot of the latest transaction (the OPTIMIZE operation) is removed
-- MAGIC * Since no new re-insert step is done, the final result is an empty table 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC From the output above, we can see that 4 rows were removed.
-- MAGIC
-- MAGIC Let's confirm this below.

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Deleting all the records in your table is probably not a desired outcome. Luckily, we can simply rollback this commit.

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8

-- COMMAND ----------

SELECT * FROM students
ORDER BY id;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   dbutils.fs.ls(f"{DA.paths.user_db}/students")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000010.json`")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Notice about "RESTORE TABLE students TO VERSION AS OF 8"
-- MAGIC * The RESTORE command introduces a new transaction and a new parquet snapshot is added
-- MAGIC * In this snapshot the 4 records of the table at version 8 are re-inserted
-- MAGIC * RESTORE introduce also a "metadata" step and a "protocol" step, since it is like the table is re-initialized

-- COMMAND ----------

DESCRIBE HISTORY students;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Note that a **`RESTORE`** <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">command</a> is recorded as a transaction; you won't be able to completely hide the fact that you accidentally deleted all the records in the table, but you will be able to undo the operation and bring your table back to a desired state.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Cleaning Up Stale Files
-- MAGIC
-- MAGIC Databricks will automatically clean up stale log files (> 30 days by default) in Delta Lake tables.
-- MAGIC Each time a checkpoint is written, Databricks automatically cleans up log entries older than this retention interval.
-- MAGIC
-- MAGIC While Delta Lake versioning and time travel are great for querying recent versions and rolling back queries, keeping the data files for all versions of large production tables around indefinitely is very expensive (and can lead to compliance issues if PII is present).
-- MAGIC
-- MAGIC If you wish to manually purge old data files, this can be performed with the **`VACUUM`** operation.
-- MAGIC
-- MAGIC Uncomment the following cell and execute it with a retention of **`0 HOURS`** to keep only the current version:

-- COMMAND ----------

-- VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC By default, **`VACUUM`** will prevent you from deleting files less than 7 days old, just to ensure that no long-running operations are still referencing any of the files to be deleted. If you run **`VACUUM`** on a Delta table, you lose the ability time travel back to a version older than the specified data retention period.  In our demos, you may see Databricks executing code that specifies a retention of **`0 HOURS`**. This is simply to demonstrate the feature and is not typically done in production.  
-- MAGIC
-- MAGIC In the following cell, we:
-- MAGIC 1. Turn off a check to prevent premature deletion of data files
-- MAGIC 1. Make sure that logging of **`VACUUM`** commands is enabled
-- MAGIC 1. Use the **`DRY RUN`** version of vacuum to print out all records to be deleted
-- MAGIC
-- MAGIC * NOTICE: Adding DRY RUN results in a safe opration. The files are not vacuued. This operation just returns the files that are going to be vacuued permanently in case you run VACUUM students RETAIN 0 HOURS 

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC By running **`VACUUM`** and deleting the 08 files above, we will permanently remove access to versions of the table that require these files to materialize.

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

SELECT * FROM students;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### see next display:
-- MAGIC * All parquet files have been deleted, but the last one (the one re-created during the RESTORE operation)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   dbutils.fs.ls(f"{DA.paths.user_db}/students")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Transaction 00000000000000000011 is the "VACUUM START"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000011.json`")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Transaction 00000000000000000012 is the "VACUUM END"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC   spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000012.json`")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC Check the table directory to show that files have been successfully deleted.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
