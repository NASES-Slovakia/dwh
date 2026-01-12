# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7dfcb8da-b093-42d8-8600-9f05ef2ddc08",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "d626fea3-3116-4af5-9cfb-9196dd43cba1",
# META       "known_lakehouses": [
# META         {
# META           "id": "7dfcb8da-b093-42d8-8600-9f05ef2ddc08"
# META         },
# META         {
# META           "id": "61b624c2-0c32-4f59-8806-6ad9e84e54c5"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "2fbb6354-0f0b-a1c8-4a19-b02cd16a70f2",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

job_id = '20251209142500'
source_system = 'SM'
batch_day = '20251218'
pipeline_name ='NoPipeline'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
job_id = datetime.now().strftime('%Y%m%d%H%M%S')
print(f"job_id: {job_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime
import uuid

source_layer = "landing"
target_layer = "bronze"

def log_pipeline_start(batch_day, source_table, target_table, operation):
    "Log pipeline start"
    log_id = str(uuid.uuid4())
    
    log_data = [(
        log_id, batch_day, pipeline_name, source_layer, target_layer,
        source_table, target_table, datetime.now(), None, "RUNNING",
        operation, None, None, None, None, job_id_str, datetime.now()
    )]
    
    log_schema = StructType([
        StructField("log_id", StringType(), False),
        StructField("batch_day", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("source_layer", StringType(), False),
        StructField("target_layer", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), True),
        StructField("status", StringType(), False),
        StructField("operation", StringType(), False),
        StructField("rows_read", LongType(), True),
        StructField("rows_written", LongType(), True),
        StructField("rows_rejected", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("created_timestamp", TimestampType(), False)
    ])
    
    df_log = spark.createDataFrame(log_data, log_schema)
    df_log.write.format("delta").mode("append").saveAsTable("lh_metadata.log_table_loads")
    
    print(f"  📝 Started logging with log_id: {log_id}")
    return log_id, datetime.now()

def log_pipeline_end(log_id, batch_day, source_table, target_table, 
                     status, operation, rows_read, rows_written, 
                     rows_rejected=0, error_message=None, start_time=None):
    "Log pipeline end"
    if start_time is None:
        start_time = datetime.now()
    
    log_data = [(
        log_id, batch_day, pipeline_name, source_layer, target_layer,
        source_table, target_table, start_time, datetime.now(), status,
        operation, rows_read, rows_written, rows_rejected, error_message,
        job_id_str, datetime.now()
    )]
    
    log_schema = StructType([
        StructField("log_id", StringType(), False),
        StructField("batch_day", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("source_layer", StringType(), False),
        StructField("target_layer", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), True),
        StructField("status", StringType(), False),
        StructField("operation", StringType(), False),
        StructField("rows_read", LongType(), True),
        StructField("rows_written", LongType(), True),
        StructField("rows_rejected", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("created_timestamp", TimestampType(), False)
    ])
    
    df_log = spark.createDataFrame(log_data, log_schema)
    df_log.write.format("delta").mode("append").saveAsTable("lh_metadata.log_table_loads")
    
    status_emoji = "✅" if status == "SUCCESS" else "❌" if status == "FAILED" else "⚠️"
    print(f"  {status_emoji} Logged: {status} (rejected: {rows_rejected})")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, 
    sha2, concat_ws, coalesce, lit, struct, to_json,
    regexp_extract
)
from pyspark.sql.types import *
from datetime import datetime
from delta.tables import DeltaTable
import os
import uuid
#Custom library for logging
import adastra_etl as etl

source_base_path = f"Files/{source_system}"
lakehouse_name = "lh_bronze"
folder_name = os.path.basename(source_base_path).lower()
job_id_str = datetime.now().strftime("%Y%m%d_%H%M%S")
job_id = lit(job_id_str)

# Pipeline metadata
pipeline_name = "landing_to_bronze"
source_layer = "landing"
target_layer = "bronze"

# Initialize Spark session
spark = spark

# ============================================================================
# Bronze Layer Functions
# ============================================================================

def add_bronze_metadata(df):
    """Add standard bronze layer metadata columns including batch day from filename"""
    return df \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_batch_day", 
                   regexp_extract(col("_source_file"), r"_(\d{8})_", 1)) \
        .withColumn("_row_hash", 
                   sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) 
                                          for c in sorted(df.columns)], lit(batch_day)), 256)) \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_job_id", job_id)

def get_table_folders(base_path):
    """Get list of subfolders representing tables"""
    try:
        folders = mssparkutils.fs.ls(base_path)
        table_folders = [f.name for f in folders if f.isDir]
        return table_folders
    except Exception as e:
        print(f"Error reading folders from {base_path}: {str(e)}")
        return []

def table_exists(table_name):
    """Check if a table exists using SHOW TABLES"""
    tables = spark.sql("SHOW TABLES").collect()
    existing_tables = [row.tableName for row in tables]
    return table_name in existing_tables

def load_table_to_bronze(table_name, source_system_name, batch_day):
    """Load all AVRO files from a table folder to bronze layer with logging"""
    
    table_path = f"{source_base_path}/{table_name}"
    bronze_table = f"{source_system_name}_{table_name}"
    source_table_full = f"{source_system}/{table_name}"
    
    print(f"\n{'='*80}")
    print(f"Processing table: {table_name}")
    print(f"Source path: {table_path}")
    print(f"Target table: {bronze_table}")
    print(f"{'='*80}")
    
    # Start logging
    log_id, start_time = log_pipeline_start(
        batch_day=batch_day,
        source_table=source_table_full,
        target_table=bronze_table,
        operation="INSERT"
    )

    try:
        # Check if any AVRO files exist
        files = mssparkutils.fs.ls(table_path)
        avro_files = [f for f in files if f.name.endswith('.avro')]
        
        if not avro_files:
            print(f"  ⚠ No AVRO files found in {table_path} - skipping")
            log_pipeline_end(
                log_id=log_id,
                batch_day=batch_day,
                source_table=source_table_full,
                target_table=bronze_table,
                status="WARNING",
                operation="SKIP",
                rows_read=0,
                rows_written=0,
                error_message="No AVRO files found",
                start_time=start_time
            )
            return False
        
        print(f"  Found {len(avro_files)} AVRO file(s)")
        
        # Read all AVRO files in the folder
        df = spark.read \
            .format("avro") \
            .option("recursiveFileLookup", "false") \
            .load(f"{table_path}/*_{batch_day}_*.avro")
        
        # Get record count
        record_count = df.count()
        print(f"  Records found: {record_count}")
        
        if record_count == 0:
            print(f"  ⚠ No data in AVRO files - creating empty table with schema")
            
            # Add metadata columns to empty dataframe
            df_bronze = add_bronze_metadata(df)
            
            # Create or replace empty table with schema
            df_bronze.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(bronze_table)
            
            print(f"  ✓ Created empty table {bronze_table} with schema from AVRO")
            
            log_pipeline_end(
                log_id=log_id,
                batch_day=batch_day,
                source_table=source_table_full,
                target_table=bronze_table,
                status="SUCCESS",
                operation="CREATE",
                rows_read=0,
                rows_written=0,
                start_time=start_time
            )
            return True
        
        else:
            # Add bronze metadata columns
            df_bronze = add_bronze_metadata(df)
            
            # Get distinct batch days from incoming data
            incoming_batch_days = [row._batch_day for row in df_bronze.select("_batch_day").distinct().collect()]
            print(f"  Incoming batch days: {incoming_batch_days}")
            
            # Check if table exists
            if not table_exists(bronze_table):
                # First load - just create the table
                print(f"  Table {bronze_table} doesn't exist - creating new table")
                
                df_bronze.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("overwriteSchema", "true") \
                    .saveAsTable(bronze_table)
                
                print(f"  ✓ Successfully created {bronze_table} with {record_count} records")
                
                log_pipeline_end(
                    log_id=log_id,
                    batch_day=batch_day,
                    source_table=source_table_full,
                    target_table=bronze_table,
                    status="SUCCESS",
                    operation="INSERT",
                    rows_read=record_count,
                    rows_written=record_count,
                    start_time=start_time
                )
                return True
            
            else:
                # Table exists - delete existing batch days and append new data
                delta_table = DeltaTable.forName(spark, bronze_table)
                
                # Count rows before delete
                rows_before = spark.table(bronze_table).count()
                rows_deleted = 0
                
                # Delete records for incoming batch days
                if incoming_batch_days:
                    batch_days_filter = " OR ".join([f"_batch_day = '{bd}'" for bd in incoming_batch_days if bd])
                    
                    if batch_days_filter:
                        print(f"  Deleting existing records for batch days: {incoming_batch_days}")
                        
                        # Count rows to be deleted
                        rows_to_delete = spark.table(bronze_table).filter(batch_days_filter).count()
                        print(f"  Rows to delete: {rows_to_delete}")
                        
                        # Delete
                        delta_table.delete(batch_days_filter)
                        
                        rows_after_delete = spark.table(bronze_table).count()
                        rows_deleted = rows_before - rows_after_delete
                        print(f"  Rows deleted: {rows_deleted}")
                
                # Append new data
                df_bronze.write \
                    .format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(bronze_table)
                
                rows_after = spark.table(bronze_table).count()
                print(f"  ✓ Successfully loaded {record_count} records to {bronze_table}")
                print(f"  Table stats: {rows_before} → {rows_after} rows (deleted: {rows_deleted}, added: {record_count})")
                
                # Show current batch days in table
                print("  Current batch days in table:")
                spark.table(bronze_table).select("_batch_day").distinct().orderBy("_batch_day").show(truncate=False)
                
                log_pipeline_start_end(
                    log_id=log_id,
                    batch_day=batch_day,
                    source_table=source_table_full,
                    target_table=bronze_table,
                    status="SUCCESS",
                    operation="DELETE+INSERT",
                    rows_read=record_count,
                    rows_written=record_count,
                    start_time=start_time
                )
                return True
        
    except Exception as e:
        error_msg = str(e)
        print(f"  ✗ Error loading table {table_name}: {error_msg}")
        import traceback
        traceback.print_exc()
        
        log_pipeline_end(
            log_id=log_id,
            batch_day=batch_day,
            source_table=source_table_full,
            target_table=bronze_table,
            status="FAILED",
            operation="ERROR",
            rows_read=0,
            rows_written=0,
            error_message=error_msg[:500],
            start_time=start_time
        )
        return False

# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function"""
    
    print("="*80)
    print("Landing to Bronze Load Process Started")
    print(f"Source System: {source_system}")
    print(f"Batch Day: {batch_day}")
    print(f"Job ID: {job_id_str}")
    print(f"Timestamp: {datetime.now()}")
    print("="*80)
    
    # Get all table folders
    table_folders = get_table_folders(source_base_path)
    
    if not table_folders:
        print(f"\n⚠ No subfolders found in {source_base_path}")
        return
    
    print(f"\nFound {len(table_folders)} table folders: {table_folders}")
    
    # Process each table
    results = {}
    for table_name in table_folders:
        success = load_table_to_bronze(table_name, folder_name, batch_day)
        results[table_name] = "Success" if success else "Failed"
    
    # Summary
    print("\n" + "="*80)
    print("Bronze Load Summary")
    print("="*80)
    successful = sum(1 for status in results.values() if status == "Success")
    print(f"Total tables: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {len(results) - successful}")
    
    print("\nDetails:")
    for table, status in results.items():
        status_symbol = "✓" if status == "Success" else "✗"
        print(f"  {status_symbol} {table}: {status}")
    
    print("\n" + "="*80)
    print("Process completed!")
    print(f"Check logs: SELECT * FROM lh_metadata.log_table_loads WHERE job_id = '{job_id_str}'")
    print("="*80)

    # Print logs
    df_log = spark.sql(f"SELECT * FROM lh_metadata.log_table_loads WHERE job_id = '{job_id_str}' order by start_time desc")
    display(df_log)

# Execute
main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# **OLD CSV loader**

# CELL ********************

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, 
    sha2, concat_ws, coalesce, lit, struct, to_json
)
from pyspark.sql.types import *
from datetime import datetime
import os

# Configuration
source_base_path = "Files/CNM"
lakehouse_name = "lh_bronze"
folder_name = os.path.basename(source_base_path).lower()  # Fixed: .lower() instead of lower()
job_id = lit(datetime.now().strftime("%Y%m%d_%H%M%S"))  # Added: wrap in lit() for PySpark

# Initialize Spark session (already available in Fabric notebook)
spark = spark

# Add audit columns for bronze layer
def add_bronze_metadata(df):
    """Add standard bronze layer metadata columns"""
    return df \
        .withColumn("_row_hash", sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in df.columns]), 256)) \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_job_id", job_id)
        

# Get list of subfolders (table names)
def get_table_folders(base_path):
    """Get list of subfolders representing tables"""
    try:
        folders = mssparkutils.fs.ls(base_path)
        table_folders = [f.name for f in folders if f.isDir]
        return table_folders
    except Exception as e:
        print(f"Error reading folders from {base_path}: {str(e)}")
        return []

# Load CSV files from a table folder
def load_table_to_bronze(table_name, source_system):
    """Load all CSV files from a table folder to bronze layer"""
    
    table_path = f"{source_base_path}/{table_name}"
    print(f"\nProcessing table: {table_name}")
    print(f"Source path: {table_path}")
    
    try:
        # Read all CSV files in the folder
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("recursiveFileLookup", "false") \
            .option("delimiter",';') \
            .csv(f"{table_path}/*.csv")
        
        # Check if any data was loaded
        record_count = df.count()
        print(f"Records found: {record_count}")
        
        if record_count == 0:
            print(f"No data found in {table_path}")
            return False
        
        # Add bronze metadata columns
        df_bronze = add_bronze_metadata(df)
        
        # Write to bronze delta table
        bronze_table = f"{source_system}_{table_name}"
        
        df_bronze.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(bronze_table)
        
        print(f"✓ Successfully loaded {record_count} records to {bronze_table}")
        return True
        
    except Exception as e:
        print(f"✗ Error loading table {table_name}: {str(e)}")
        return False

# Main execution
def main():
    """Main execution function"""
    
    print("="*80)
    print("Bronze Load Process Started")
    print(f"Timestamp: {datetime.now()}")
    print("="*80)
    
    # Get all table folders
    table_folders = get_table_folders(source_base_path)
    
    if not table_folders:
        print(f"No subfolders found in {source_base_path}")
        return
    
    print(f"\nFound {len(table_folders)} table folders: {table_folders}")
    
    # Process each table
    results = {}
    for table_name in table_folders:
        success = load_table_to_bronze(table_name,folder_name)
        results[table_name] = "Success" if success else "Failed"
    
    # Summary
    print("\n" + "="*80)
    print("Bronze Load Summary")
    print("="*80)
    for table, status in results.items():
        status_symbol = "✓" if status == "Success" else "✗"
        print(f"{status_symbol} {table}: {status}")
    
    print("\nProcess completed!")

# Execute
main()
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
