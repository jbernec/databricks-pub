# Databricks notebook source
# MAGIC %md
# MAGIC ## Helper functions to implement delta change data feed read, predictive i/o (enable deletion vectors) for accelerated delta merge operations, custom delta merge and other operations in databricks.

# COMMAND ----------

def enable_cdf_existing_table(table_name:str, table_path:str):
    from pyspark.sql import DataFrame, SparkSession
    from delta import DeltaTable
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.functions import col

    """
    Description: 
        Enable delta change data feed on existing data files and tables.
    """

    try:
        # enable cdf on new delta tables
        spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

        # enable cdf on existing delta tables

        # check if cdf already enabled
        dt = DeltaTable.forPath(sparkSession=spark, path=table_path)
        if "delta.enableChangeDataFeed" in str(dt.detail().select(col("properties")).first()["properties"]):
            print("CDF is enabled")
        else:
            # enable cdf on existing table by first dropping external table metadata, the data remains intact
            spark.sql(f"drop table if exists {table_name};")

            # create new delta table on existing data
            spark.sql(f"create external table if not exists {table_name} location '{table_path}'")
            spark.sql(f"alter table {table_name} set tblproperties (delta.enableChangeDataFeed = true)")
            print(f"A new table {table_name} has been created using existing location at `{table_path}`")

            if "delta.enableChangeDataFeed" in str(dt.detail().select(col("properties")).first()["properties"]):
                print(f" CDF has been successfully enable on table {table_name}")
    except AnalysisException as ae:
        print(ae)



# COMMAND ----------

def enable_pio_existing_table(table_name:str, table_path:str):
    # import packages
    from pyspark.sql import DataFrame, SparkSession
    from delta import DeltaTable
    from pyspark.sql.utils import AnalysisException
    from pyspark.sql.functions import col


    """
    Description: 
        Enable Predictive I/O (delta.enableDeletionVectors) on existing data files and tables.
    """

    try:
        # enable cdf on existing delta tables

        # check if predictive io already enabled
        dt = DeltaTable.forPath(sparkSession=spark, path=table_path)
        if "delta.enableDeletionVectors" in str(dt.detail().select(col("properties")).first()["properties"]):
            print("Predictive I/O is enabled")
        else:
            # enable predictive io on existing table by first dropping external table metadata, the data remains intact
            spark.sql(f"drop table if exists {table_name};")

            # create new delta table on existing data
            spark.sql(f"create external table if not exists {table_name} location '{table_path}'")
            spark.sql(f"alter table {table_name} set tblproperties (delta.enableDeletionVectors = true)")
            print(f"A new table {table_name} has been created using existing location at `{table_path}`")

            if "delta.enableDeletionVectors" in str(dt.detail().select(col("properties")).first()["properties"]):
                print(f" Predictive I/O has been successfully enable on table {table_name}")
    except AnalysisException as ae:
        print(ae)

