# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a804b1aa-bfca-4298-9ae6-da94bce13c1e",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "d626fea3-3116-4af5-9cfb-9196dd43cba1",
# META       "known_lakehouses": [
# META         {
# META           "id": "a804b1aa-bfca-4298-9ae6-da94bce13c1e"
# META         },
# META         {
# META           "id": "61b624c2-0c32-4f59-8806-6ad9e84e54c5"
# META         },
# META         {
# META           "id": "7dfcb8da-b093-42d8-8600-9f05ef2ddc08"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

job_id = '20251209142500'
source_system = 'SM'
batch_day = '20251218'
pipeline_name ='ntb_bronze_2_silver'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
Bronze to Silver Data Movement with Quality Validation
=======================================================
Moves data from Bronze to Silver with:
- Schema validation (data type compatibility)
- NOT NULL constraint validation
- Rejection logging for rows that don't fit
- Comprehensive pipeline logging
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, when, coalesce,
    row_number, concat_ws, sha2, to_json, struct
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
from delta.tables import DeltaTable
import uuid

# ============================================================================
# Configuration
# ============================================================================

batch_day = "20251218"
job_id = datetime.now().strftime("%Y%m%d_%H%M%S")
job_id_str = str(job_id)
pipeline_name = "ntb_bronze_2_silver"
source_layer = "bronze"
target_layer = "silver"

spark = spark

# ============================================================================
# Logging Functions
# ============================================================================

def log_pipeline_start(batch_day, source_table, target_table, operation):
    """Log pipeline start"""
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
    """Log pipeline end"""
    
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

# ============================================================================
# Rejection Logging
# ============================================================================

def create_rejection_table_if_not_exists():
    """Create rejection table if it doesn't exist"""
    
    rejection_schema = StructType([
        StructField("rejection_id", StringType(), False),
        StructField("job_id", StringType(), False),
        StructField("batch_day", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("source_row_hash", StringType(), False),
        StructField("rejection_reason", StringType(), False),
        StructField("failed_column", StringType(), True),
        StructField("failed_value", StringType(), True),
        StructField("expected_type", StringType(), True),
        StructField("row_data", StringType(), True),  # JSON of full row
        StructField("rejection_timestamp", TimestampType(), False)
    ])
    
    try:
        spark.table("lh_metadata.rejection_log")
        print("  ✓ Rejection log table exists")
    except:
        print("  Creating rejection log table...")
        df_empty = spark.createDataFrame([], rejection_schema)
        df_empty.write.format("delta").mode("overwrite") \
            .saveAsTable("lh_metadata.rejection_log")
        print("  ✓ Created rejection log table")

def log_rejected_rows(rejected_df, source_table, target_table, batch_day):
    """Log rejected rows to rejection table"""
    
    if rejected_df.count() == 0:
        return
    
    # Prepare rejection records
    rejection_df = rejected_df.select(
        lit(str(uuid.uuid4())).alias("rejection_id"),
        lit(job_id_str).alias("job_id"),
        lit(batch_day).alias("batch_day"),
        lit(source_table).alias("source_table"),
        lit(target_table).alias("target_table"),
        col("_source_row_hash").alias("source_row_hash"),
        col("_rejection_reason").alias("rejection_reason"),
        col("_failed_column").alias("failed_column"),
        col("_failed_value").alias("failed_value"),
        col("_expected_type").alias("expected_type"),
        col("_row_data_json").alias("row_data"),
        current_timestamp().alias("rejection_timestamp")
    )
    
    # Append to rejection log
    rejection_df.write.format("delta").mode("append") \
        .saveAsTable("lh_metadata.rejection_log")
    
    print(f"  📋 Logged {rejected_df.count()} rejected rows to lh_metadata.rejection_log")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Data Quality Validation
# ============================================================================

def get_silver_schema(table_name):
    """Get Silver table schema"""
    try:
        return spark.table(table_name).schema
    except Exception as e:
        print(f"  ✗ Error reading Silver table schema: {str(e)}")
        return None

def validate_and_cast_row(df_bronze, silver_schema, source_table):
    """
    Validate Bronze data against Silver schema and cast types
    Returns: (valid_df, rejected_df)
    """
    
    print("  Validating data quality...")
    # Metadata columns
    meta_columns = ['_row_hash','_source_file','_batch_day','_load_timestamp','_job_id']

    # Add row identifier for tracking rejections
    df_with_id = df_bronze.withColumn("_source_row_hash",col("_row_hash"))
    
    # Convert full row to JSON for rejection logging
    non_meta_cols = [c for c in df_bronze.columns if c not in meta_columns]
    df_with_id = df_with_id.withColumn(
        "_row_data_json",
        to_json(struct(*non_meta_cols))
    )
    
    # Initialize rejection tracking columns
    df_validated = df_with_id \
        .withColumn("_is_valid", lit(True)) \
        .withColumn("_rejection_reason", lit(None).cast(StringType())) \
        .withColumn("_failed_column", lit(None).cast(StringType())) \
        .withColumn("_failed_value", lit(None).cast(StringType())) \
        .withColumn("_expected_type", lit(None).cast(StringType()))
    
    # Check each column in Silver schema
    for field in silver_schema.fields:
        col_name = field.name
        target_type = field.dataType
        is_nullable = field.nullable
        
        # Skip Silver metadata columns
        if col_name in meta_columns:
            continue
        
        # Skip if column doesn't exist in Bronze
        if col_name not in df_bronze.columns:
            continue
        
        print(f"    Validating {col_name}: {target_type} (nullable={is_nullable})")
        
        # Check NOT NULL constraint
        if not is_nullable:
            df_validated = df_validated.withColumn(
                "_is_valid",
                when(
                    (col(col_name).isNull()) & (col("_is_valid") == True),
                    False
                ).otherwise(col("_is_valid"))
            ).withColumn(
                "_rejection_reason",
                when(
                    (col(col_name).isNull()) & (col("_rejection_reason").isNull()),
                    lit(f"NOT NULL constraint violation")
                ).otherwise(col("_rejection_reason"))
            ).withColumn(
                "_failed_column",
                when(
                    (col(col_name).isNull()) & (col("_failed_column").isNull()),
                    lit(col_name)
                ).otherwise(col("_failed_column"))
            ).withColumn(
                "_failed_value",
                when(
                    (col(col_name).isNull()) & (col("_failed_value").isNull()),
                    lit("NULL")
                ).otherwise(col("_failed_value"))
            ).withColumn(
                "_expected_type",
                when(
                    (col(col_name).isNull()) & (col("_expected_type").isNull()),
                    lit(f"{target_type} NOT NULL")
                ).otherwise(col("_expected_type"))
            )
        
        # Try to cast to target type (this validates type compatibility)
        # Use a temporary column to check if cast is successful
        if isinstance(target_type, (IntegerType, LongType, FloatType, DoubleType)):
            # For numeric types, check if value can be cast
            df_validated = df_validated.withColumn(
                "_temp_cast",
                col(col_name).cast(target_type)
            )
            
            df_validated = df_validated.withColumn(
                "_is_valid",
                when(
                    (col(col_name).isNotNull()) & 
                    (col("_temp_cast").isNull()) & 
                    (col("_is_valid") == True),
                    False
                ).otherwise(col("_is_valid"))
            ).withColumn(
                "_rejection_reason",
                when(
                    (col(col_name).isNotNull()) & 
                    (col("_temp_cast").isNull()) & 
                    (col("_rejection_reason").isNull()),
                    lit(f"Type cast failure")
                ).otherwise(col("_rejection_reason"))
            ).withColumn(
                "_failed_column",
                when(
                    (col(col_name).isNotNull()) & 
                    (col("_temp_cast").isNull()) & 
                    (col("_failed_column").isNull()),
                    lit(col_name)
                ).otherwise(col("_failed_column"))
            ).withColumn(
                "_failed_value",
                when(
                    (col(col_name).isNotNull()) & 
                    (col("_temp_cast").isNull()) & 
                    (col("_failed_value").isNull()),
                    col(col_name).cast(StringType())
                ).otherwise(col("_failed_value"))
            ).withColumn(
                "_expected_type",
                when(
                    (col(col_name).isNotNull()) & 
                    (col("_temp_cast").isNull()) & 
                    (col("_expected_type").isNull()),
                    lit(str(target_type))
                ).otherwise(col("_expected_type"))
            )
            
            df_validated = df_validated.drop("_temp_cast")
    
    # Split into valid and rejected
    df_valid = df_validated.filter(col("_is_valid") == True)
    df_rejected = df_validated.filter(col("_is_valid") == False)
    
    valid_count = df_valid.count()
    rejected_count = df_rejected.count()
    
    print(f"  ✓ Valid rows: {valid_count}")
    print(f"  ✗ Rejected rows: {rejected_count}")
    
    # Cast valid rows to target types
    if valid_count > 0:
        for field in silver_schema.fields:
            col_name = field.name
            if col_name.startswith("_silver") or col_name not in df_valid.columns:
                continue
            df_valid = df_valid.withColumn(col_name, col(col_name).cast(field.dataType))
    
    # Clean up validation columns from valid data
    validation_cols = ["_is_valid", "_rejection_reason", "_failed_column", 
                      "_failed_value", "_expected_type", "_source_row_hash", "_row_data_json"]
    
    for vc in validation_cols:
        if vc in df_valid.columns:
            df_valid = df_valid.drop(vc)
    
    return df_valid, df_rejected

# ============================================================================
# Main Bronze to Silver Load Function
# ============================================================================

def load_bronze_to_silver(batch_day, bronze_table_name, silver_table_name=None):
    """
    Load data from Bronze to Silver with validation
    """
    
    if silver_table_name is None:
        silver_table_name = bronze_table_name  # Same name
    
    print(f"\n{'='*80}")
    print(f"Processing: {bronze_table_name} → {silver_table_name}")
    print(f"{'='*80}")
    
    # Start logging
    log_id, start_time = log_pipeline_start(
        batch_day=batch_day,
        source_table=f"lh_bronze.{bronze_table_name}",
        target_table=f"lh_silver.{silver_table_name}",
        operation="INSERT"
    )
    
    try:
        # Create rejection table if needed
        create_rejection_table_if_not_exists()
        
        # Read Bronze data
        print(f"  Reading from lh_bronze.{bronze_table_name}")
        df_bronze = spark.table(f"lh_bronze.{bronze_table_name}").filter(col("_batch_day") == lit(batch_day))
        rows_read = df_bronze.count()
        print(f"  Rows in Bronze for batch_day = {batch_day}: {rows_read}")
        
        if rows_read == 0:
            print(f"  ⚠ No data in Bronze table")
            log_pipeline_end(
                log_id, batch_day, f"lh_bronze.{bronze_table_name}",
                f"lh_silver.{silver_table_name}", "SUCCESS", "INSERT",
                0, 0, 0, "No data to process", start_time
            )
            return True
        
        # Get Silver schema
        print(f"  Reading Silver table schema...")
        silver_schema = get_silver_schema(f"lh_silver.{silver_table_name}")
        
        if silver_schema is None:
            raise Exception(f"Cannot read Silver table schema")
        
        # Validate and cast
        df_valid, df_rejected = validate_and_cast_row(
            df_bronze, silver_schema, bronze_table_name
        )
        
        valid_count = df_valid.count()
        rejected_count = df_rejected.count()
        
        # Log rejected rows
        if rejected_count > 0:
            print(f"  Logging {rejected_count} rejected rows...")
            log_rejected_rows(
                df_rejected, 
                f"lh_bronze.{bronze_table_name}",
                f"lh_silver.{silver_table_name}",
                batch_day
            )
            
            # Show sample rejections
            print(f"\n  Sample rejections:")
            df_rejected.select(
                "_failed_column", "_failed_value", "_rejection_reason", "_expected_type"
            ).show(5, truncate=False)
        
        # Load valid rows to Silver
        if valid_count > 0:
            print(f"  Loading {valid_count} valid rows to Silver...")
            
            # Add Silver metadata
            df_silver = df_valid \
                .withColumn("_silver_load_timestamp", current_timestamp()) \
                .withColumn("_silver_job_id", lit(job_id_str))
            
            # Write to Silver (overwrite mode - adjust as needed)
            df_silver.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(f"lh_silver.{silver_table_name}")
            
            print(f"  ✓ Loaded {valid_count} rows to Silver")
        else:
            print(f"  ⚠ No valid rows to load")
        
        # Determine status
        if rejected_count > 0 and valid_count == 0:
            status = "FAILED"
        elif rejected_count > 0:
            status = "WARNING"  # Some rows rejected but some succeeded
        else:
            status = "SUCCESS"
        
        # Log completion
        log_pipeline_end(
            log_id, batch_day,
            f"lh_bronze.{bronze_table_name}",
            f"lh_silver.{silver_table_name}",
            status, "INSERT",
            rows_read, valid_count, rejected_count,
            None, start_time
        )
        
        return True
        
    except Exception as e:
        error_msg = str(e)
        print(f"  ✗ Error: {error_msg}")
        import traceback
        traceback.print_exc()
        
        log_pipeline_end(
            log_id, batch_day,
            f"lh_bronze.{bronze_table_name}",
            f"lh_silver.{silver_table_name}",
            "FAILED", "INSERT",
            0, 0, 0, error_msg[:500], start_time
        )
        
        return False

# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function"""
    
    print("="*80)
    print("Bronze to Silver Data Movement with Quality Validation")
    print(f"Batch Day: {batch_day}")
    print(f"Job ID: {job_id_str}")
    print(f"Timestamp: {datetime.now()}")
    print("="*80)
    
    # Get list of Bronze tables
    print("\nFetching Bronze tables...")
    bronze_tables = [table.name for table in spark.catalog.listTables("lh_bronze") 
                     if not table.name.startswith("_")]
    
    print(f"Found {len(bronze_tables)} Bronze tables")
    
    if not bronze_tables:
        print("No Bronze tables found!")
        return
    
    # Get list of Silver tables
    print("\nFetching Silver tables...")
    silver_tables = [table.name for table in spark.catalog.listTables("lh_silver") 
                     if not table.name.startswith("_")]
    
    print(f"Found {len(silver_tables)} Silver tables")
    
    # Find matching tables
    matching_tables = [t for t in bronze_tables if t in silver_tables]
    
    print(f"\nFound {len(matching_tables)} matching Bronze-Silver table pairs")
    
    if not matching_tables:
        print("No matching tables found!")
        return
    
    # Process each matching table
    results = {}
    
    for table_name in matching_tables:
        success = load_bronze_to_silver(batch_day,table_name)
        results[table_name] = "Success" if success else "Failed"
    
    # Summary
    print("\n" + "="*80)
    print("Data Movement Summary")
    print("="*80)
    
    successful = sum(1 for status in results.values() if status == "Success")
    failed = sum(1 for status in results.values() if status == "Failed")
    
    print(f"Total tables: {len(results)}")
    print(f"  ✓ Successful: {successful}")
    print(f"  ✗ Failed: {failed}")
    
    print("\nDetails:")
    for table, status in sorted(results.items()):
        symbol = "✓" if status == "Success" else "✗"
        print(f"  {symbol} {table}: {status}")
    
    print("\n" + "="*80)
    print("Process completed!")
    print(f"Check logs: SELECT * FROM lh_metadata.log_table_loads WHERE job_id = '{job_id_str}'")
    print(f"Check rejections: SELECT * FROM lh_metadata.rejection_log WHERE job_id = '{job_id_str}'")
    print("="*80)

# Execute
main()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC SELECT * FROM lh_metadata.log_table_loads WHERE job_id = '20251223_132632' order by start_time desc;
# MAGIC SELECT * FROM lh_metadata.rejection_log WHERE job_id = '20251223_132632';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
