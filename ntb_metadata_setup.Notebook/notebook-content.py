# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "61b624c2-0c32-4f59-8806-6ad9e84e54c5",
# META       "default_lakehouse_name": "lh_metadata",
# META       "default_lakehouse_workspace_id": "d626fea3-3116-4af5-9cfb-9196dd43cba1",
# META       "known_lakehouses": [
# META         {
# META           "id": "61b624c2-0c32-4f59-8806-6ad9e84e54c5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip install /lakehouse/default/Files/Libraries/adastra_etl-1.0.0-py3-none-any.whl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import adastra_etl as etl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from datetime import datetime

spark = spark

print("="*80)
print("Setting up Metadata Lakehouse")
print("="*80)

# ============================================================================
# 1. Create Log Table for ETL Pipeline Runs
# ============================================================================

log_table_loads_schema = StructType([
    StructField("log_id", StringType(), False),  # UUID or timestamp-based ID
    StructField("batch_day", StringType(), False), # YYYYMMDD of data
    StructField("pipeline_name", StringType(), False),  # e.g., "bronze_to_silver"
    StructField("source_layer", StringType(), False),  # e.g., "bronze"
    StructField("target_layer", StringType(), False),  # e.g., "silver"
    StructField("source_table", StringType(), False),  # e.g., "IAM_MESSAGES_RECEIVED"
    StructField("target_table", StringType(), False),  # e.g., "IAM_MESSAGES_RECEIVED"
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), False),  # RUNNING, SUCCESS, FAILED, WARNING
    StructField("operation", StringType(), False),  # INSERT, DELETE
    StructField("rows_read", LongType(), True),
    StructField("rows_written", LongType(), True),
    StructField("rows_rejected", LongType(), True),  # Failed DQ checks
    StructField("error_message", StringType(), True),
    StructField("job_id", StringType(), True),  # For tracking related jobs
    StructField("created_timestamp", TimestampType(), False)
])

print("\n1. Creating log_table_loads...")
df_log = spark.createDataFrame([], log_table_loads_schema)
df_log.write.format("delta").mode("overwrite").saveAsTable("lh_metadata.log_table_loads")
print("   ✓ Created: lh_metadata.log_table_loads")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
