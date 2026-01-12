# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "102a3645-0540-49a5-b3cd-a4ff928d05e7",
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "d626fea3-3116-4af5-9cfb-9196dd43cba1",
# META       "known_lakehouses": [
# META         {
# META           "id": "102a3645-0540-49a5-b3cd-a4ff928d05e7"
# META         },
# META         {
# META           "id": "a804b1aa-bfca-4298-9ae6-da94bce13c1e"
# META         },
# META         {
# META           "id": "61b624c2-0c32-4f59-8806-6ad9e84e54c5"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Define Parameters**

# PARAMETERS CELL ********************

job_id = '20251209142500'
source_system = 'SM'
batch_day = '20251218'
pipeline_name ='ntb_silver_2_gold'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Setting up logging functions**

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
pipeline_name = "ntb_silver_2_gold"
source_layer = "silver"
target_layer = "gold"

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Get column names and column order from Silver**

# CELL ********************

# Get all tables from lh_silver lakehouse
tables = spark.catalog.listTables("lh_silver")

# Collect column info for all tables
all_columns = []

for table in tables:
    table_name = f"lh_silver.{table.name}"
    
    try:
        # Get columns for this table
        columns = spark.table(table_name).columns
        
        # Add to list with table name
        for i, col in enumerate(columns):
            all_columns.append((table.name, i+1, col))
        
        print(f"✓ Processed: {table.name} ({len(columns)} columns)")
        
    except Exception as e:
        print(f"✗ Error processing {table.name}: {str(e)}")

# Create DataFrame
df_column_info = spark.createDataFrame(
    all_columns, 
    ["table_name", "ordinal_position", "column_name"]
)

# Create temp view
df_column_info.createOrReplaceTempView("column_info")

print(f"\n✓ Created temp view 'column_info' with {df_column_info.count()} rows")

# Display sample
#display(df_column_info.orderBy("table_name","column_name", "ordinal_position"))

df=spark.sql("""
select distinct 
    tt.typ as table_type, --DIM/FACT
    tt.nazov_tabulky_gold as gold_table_name,
    tc.nazov_tabulky as silver_table_name,
    tc.atribut as silver_column_name,
    tc.gold_name as gold_column_name,
    tc.data_type_final as gold_column_data_type,
    case when tc.povinny = 'ano' then TRUE ELSE FALSE END AS gold_column_is_nullable,
    case when tc.`pk?` = 'PK' then TRUE ELSE FALSE END AS gold_column_is_pk,
    ci.ordinal_position
--,*
 from lh_metadata.table_config tc
left join lh_metadata.table_type_config tt 
    on tt.system = tc.system
    and tc.nazov_tabulky = tt.nazov_tabulky_bronze
left join column_info ci
    on ci.table_name = tc.nazov_tabulky
    and ci.column_name = tc.atribut
where tc.system like '%DEV'
order by silver_table_name, ordinal_position
""")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lh_metadata.metadata_table_column_setup")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Silver to Bronze processing**

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, when, coalesce, concat_ws, sha2, 
    to_json, struct, md5, row_number, max as spark_max
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

# ============================================================================
# SCD2 and FACT Processing Functions
# ============================================================================

def get_table_metadata(gold_table_name):
    """
    Get metadata configuration for a specific gold table
    Returns: DataFrame with column configuration
    """
    df_meta = spark.sql(f"""
        SELECT 
            table_type,
            gold_table_name,
            silver_table_name,
            silver_column_name,
            gold_column_name,
            gold_column_data_type,
            gold_column_is_nullable,
            gold_column_is_pk,
            ordinal_position
        FROM lh_metadata.metadata_table_column_setup
        WHERE gold_table_name = '{gold_table_name}'
        ORDER BY ordinal_position
    """)
    
    if df_meta.count() == 0:
        raise ValueError(f"No metadata found for table: {gold_table_name}")
    
    return df_meta


def get_business_key_columns(df_meta):
    """Get primary key columns from metadata"""
    pk_cols = df_meta.filter(col("gold_column_is_pk") == True) \
                     .select("gold_column_name") \
                     .rdd.flatMap(lambda x: x).collect()
    
    if not pk_cols:
        raise ValueError("No primary key columns defined in metadata")
    
    return pk_cols


def calculate_row_hash(df, columns_to_hash, exclude_columns=None):
    """
    Calculate hash for SCD2 comparison
    Excludes metadata columns and optional custom columns
    """
    if exclude_columns is None:
        exclude_columns = []
    
    # Default metadata columns to exclude from hash
    default_exclude = [
        "_source_file", "_batch_day", "_row_hash", "_load_timestamp", 
        "_job_id", "_silver_load_timestamp", "_silver_job_id",
        "_gold_load_timestamp", "_gold_modified_timestamp", "_gold_job_id",
        "_is_current", "_valid_from", "_valid_to"
    ]
    
    all_exclude = list(set(default_exclude + exclude_columns))
    
    # Get columns to include in hash
    hash_columns = [c for c in columns_to_hash if c not in all_exclude]
    
    if not hash_columns:
        raise ValueError("No columns available for hash calculation")
    
    # Create hash from concatenated values
    hash_expr = md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("NULL")) 
                                       for c in hash_columns]))
    
    return df.withColumn("_content_hash", hash_expr)


def process_dim_scd2(silver_table_name, gold_table_name, batch_day, 
                     exclude_from_comparison=None, job_id_str=None):
    """
    Process Dimension table with SCD Type 2 logic
    
    Args:
        silver_table_name: Source silver table name
        gold_table_name: Target gold table name
        batch_day: Batch day for processing
        exclude_from_comparison: Additional columns to exclude from SCD2 comparison
        job_id_str: Job ID for logging
    
    Returns:
        tuple: (rows_read, rows_written, rows_rejected)
    """
    
    print(f"\n{'='*80}")
    print(f"Processing DIM Table: {gold_table_name} (SCD Type 2)")
    print(f"{'='*80}")
    
    # Get metadata
    df_meta = get_table_metadata(gold_table_name)
    table_type = df_meta.select("table_type").first()[0]
    
    if table_type != "DIM":
        raise ValueError(f"Expected DIM table type, got: {table_type}")
    
    # Get business keys and all columns
    pk_columns = get_business_key_columns(df_meta)
    all_gold_columns = df_meta.select("gold_column_name").rdd.flatMap(lambda x: x).collect()
    
    print(f"  Primary Keys: {', '.join(pk_columns)}")
    print(f"  Total Columns: {len(all_gold_columns)}")
    
    # Start logging
    log_id, start_time = log_pipeline_start(batch_day, silver_table_name, gold_table_name, "SCD2_UPSERT")
    
    try:
        # Read source data from silver
        df_silver = spark.table(f"lh_silver.{silver_table_name}")
        rows_read = df_silver.count()
        print(f"  📊 Silver records: {rows_read:,}")
        
        # Calculate content hash for SCD2 comparison
        df_silver_hashed = calculate_row_hash(df_silver, df_silver.columns, exclude_from_comparison)
        
        # Add gold metadata columns (preserve silver metadata)
        df_silver_prepared = df_silver_hashed \
            .withColumn("_gold_load_timestamp", current_timestamp()) \
            .withColumn("_gold_modified_timestamp", lit(None).cast(TimestampType())) \
            .withColumn("_gold_job_id", lit(job_id_str)) \
            .withColumn("_is_current", lit(True)) \
            .withColumn("_valid_from", current_timestamp()) \
            .withColumn("_valid_to", lit(None).cast(TimestampType()))
        
        # Check if target table exists
        target_table_path = f"lh_gold.{gold_table_name}"
        table_exists = spark.catalog.tableExists(target_table_path)
        
        if not table_exists:
            # Initial load - create table
            print(f"  🆕 Creating new table: {gold_table_name}")
            
            df_silver_prepared.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table_path)
            
            rows_written = rows_read
            rows_rejected = 0
            
            print(f"  ✅ Initial load complete: {rows_written:,} rows")
            
        else:
            # SCD2 merge logic
            print(f"  🔄 Performing SCD2 merge...")
            
            delta_table = DeltaTable.forName(spark, target_table_path)
            
            # Build merge condition on business keys
            merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_columns])
            
            # Perform SCD2 merge
            merge_result = delta_table.alias("target").merge(
                df_silver_prepared.alias("source"),
                merge_condition
            ).whenMatchedUpdate(
                condition = "target._is_current = true AND target._content_hash <> source._content_hash",
                set = {
                    "_is_current": "false",
                    "_valid_to": "current_timestamp()",
                    "_gold_modified_timestamp": "current_timestamp()"
                }
            ).whenNotMatchedInsertAll().execute()
            
            # Insert new versions of updated records
            # Get records that were just closed (updated)
            df_closed = spark.sql(f"""
                SELECT source.*
                FROM ({df_silver_prepared.createOrReplaceTempView("silver_temp")} silver_temp) source
                INNER JOIN {target_table_path} target
                    ON {" AND ".join([f"target.{pk} = source.{pk}" for pk in pk_columns])}
                WHERE target._is_current = false 
                    AND target._valid_to >= current_timestamp() - INTERVAL 1 MINUTE
                    AND target._content_hash <> source._content_hash
            """)
            
            if df_closed.count() > 0:
                # Insert new current versions
                df_closed.write \
                    .format("delta") \
                    .mode("append") \
                    .saveAsTable(target_table_path)
            
            rows_written = df_silver_prepared.count()
            rows_rejected = 0
            
            print(f"  ✅ SCD2 merge complete")
        
        # Log success
        log_pipeline_end(
            log_id, batch_day, silver_table_name, gold_table_name,
            "SUCCESS", "SCD2_UPSERT", rows_read, rows_written, 
            rows_rejected, None, start_time
        )
        
        return rows_read, rows_written, rows_rejected
        
    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ Error: {error_msg}")
        
        log_pipeline_end(
            log_id, batch_day, silver_table_name, gold_table_name,
            "FAILED", "SCD2_UPSERT", 0, 0, 0, error_msg, start_time
        )
        
        raise


def process_fact_append(silver_table_name, gold_table_name, batch_day, job_id_str=None):
    """
    Process Fact table with append/replace logic
    
    Args:
        silver_table_name: Source silver table name
        gold_table_name: Target gold table name
        batch_day: Batch day for processing
        job_id_str: Job ID for logging
    
    Returns:
        tuple: (rows_read, rows_written, rows_rejected)
    """
    
    print(f"\n{'='*80}")
    print(f"Processing FACT Table: {gold_table_name} (Append/Replace)")
    print(f"{'='*80}")
    
    # Get metadata
    df_meta = get_table_metadata(gold_table_name)
    table_type = df_meta.select("table_type").first()[0]
    
    if table_type != "FACT":
        raise ValueError(f"Expected FACT table type, got: {table_type}")
    
    # Start logging
    log_id, start_time = log_pipeline_start(batch_day, silver_table_name, gold_table_name, "FACT_APPEND")
    
    try:
        # Read source data from silver
        df_silver = spark.table(f"lh_silver.{silver_table_name}")
        rows_read = df_silver.count()
        print(f"  📊 Silver records: {rows_read:,}")
        
        # Add gold metadata columns (preserve silver metadata)
        df_silver_prepared = df_silver \
            .withColumn("_gold_load_timestamp", current_timestamp()) \
            .withColumn("_gold_job_id", lit(job_id_str))
        
        # Check if target table exists
        target_table_path = f"lh_gold.{gold_table_name}"
        table_exists = spark.catalog.tableExists(target_table_path)
        
        if not table_exists:
            # Initial load - create table
            print(f"  🆕 Creating new table: {gold_table_name}")
            
            df_silver_prepared.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table_path)
            
            rows_written = rows_read
            
        else:
            # Check if batch_day already exists in gold
            existing_batch = spark.sql(f"""
                SELECT COUNT(*) as cnt 
                FROM {target_table_path} 
                WHERE _batch_day = '{batch_day}'
            """).first()['cnt']
            
            if existing_batch > 0:
                # Replace existing batch_day data
                print(f"  🔄 Replacing existing batch_day: {batch_day}")
                
                # Delete existing records for this batch_day
                delta_table = DeltaTable.forName(spark, target_table_path)
                delta_table.delete(f"_batch_day = '{batch_day}'")
                
                # Append new records
                df_silver_prepared.write \
                    .format("delta") \
                    .mode("append") \
                    .saveAsTable(target_table_path)
                
                rows_written = rows_read
                print(f"  ✅ Replaced {rows_written:,} rows for batch_day {batch_day}")
                
            else:
                # Append new batch_day data
                print(f"  ➕ Appending new batch_day: {batch_day}")
                
                df_silver_prepared.write \
                    .format("delta") \
                    .mode("append") \
                    .saveAsTable(target_table_path)
                
                rows_written = rows_read
                print(f"  ✅ Appended {rows_written:,} rows")
        
        rows_rejected = 0
        
        # Log success
        log_pipeline_end(
            log_id, batch_day, silver_table_name, gold_table_name,
            "SUCCESS", "FACT_APPEND", rows_read, rows_written, 
            rows_rejected, None, start_time
        )
        
        return rows_read, rows_written, rows_rejected
        
    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ Error: {error_msg}")
        
        log_pipeline_end(
            log_id, batch_day, silver_table_name, gold_table_name,
            "FAILED", "FACT_APPEND", 0, 0, 0, error_msg, start_time
        )
        
        raise


def process_gold_table(gold_table_name, batch_day, exclude_from_comparison=None, job_id_str=None):
    """
    Main function to process a gold table based on metadata configuration
    
    Args:
        gold_table_name: Name of the gold table to process
        batch_day: Batch day for processing
        exclude_from_comparison: Additional columns to exclude from SCD2 comparison (for DIM only)
        job_id_str: Job ID for logging
    
    Returns:
        tuple: (rows_read, rows_written, rows_rejected)
    """
    
    # Get metadata
    df_meta = get_table_metadata(gold_table_name)
    
    # Get table type and silver table name
    first_row = df_meta.select("table_type", "silver_table_name").first()
    table_type = first_row['table_type']
    silver_table_name = first_row['silver_table_name']
    
    # Process based on table type
    if table_type == "DIM":
        return process_dim_scd2(
            silver_table_name, 
            gold_table_name, 
            batch_day,
            exclude_from_comparison,
            job_id_str
        )
    
    elif table_type == "FACT":
        return process_fact_append(
            silver_table_name,
            gold_table_name,
            batch_day,
            job_id_str
        )
    
    else:
        print(f"⚠️  Unknown table type: {table_type} for table {gold_table_name}")
        print(f"    Supported types: DIM, FACT")
        print(f"    Skipping processing for this table.")
        return 0, 0, 0


# ============================================================================
# Process All Gold Tables
# ============================================================================

def process_all_gold_tables(batch_day, job_id_str=None, exclude_from_comparison=None):
    """
    Process all gold tables based on metadata configuration
    
    Args:
        batch_day: Batch day for processing
        job_id_str: Job ID for logging
        exclude_from_comparison: Additional columns to exclude from SCD2 comparison
    """
    
    print(f"\n{'='*80}")
    print(f"GOLD LAYER PROCESSING - Batch Day: {batch_day}")
    print(f"{'='*80}\n")
    
    # Get all unique gold tables from metadata
    df_tables = spark.sql("""
        SELECT DISTINCT 
            table_type,
            gold_table_name,
            silver_table_name
        FROM lh_metadata.metadata_table_column_setup
        ORDER BY table_type, gold_table_name
    """)
    
    tables = df_tables.collect()
    
    print(f"Found {len(tables)} tables to process\n")
    
    # Summary tracking
    results = {
        "success": [],
        "failed": [],
        "skipped": [],
        "total_rows_read": 0,
        "total_rows_written": 0
    }
    
    # Process each table
    for row in tables:
        table_type = row['table_type']
        gold_table_name = row['gold_table_name']
        silver_table_name = row['silver_table_name']
        
        try:
            rows_read, rows_written, rows_rejected = process_gold_table(
                gold_table_name,
                batch_day,
                exclude_from_comparison,
                job_id_str
            )
            
            results["success"].append(gold_table_name)
            results["total_rows_read"] += rows_read
            results["total_rows_written"] += rows_written
            
        except Exception as e:
            print(f"  ❌ Failed to process {gold_table_name}: {str(e)}")
            results["failed"].append(gold_table_name)
    
    # Print summary
    print(f"\n{'='*80}")
    print(f"PROCESSING SUMMARY")
    print(f"{'='*80}")
    print(f"Total tables:        {len(tables)}")
    print(f"Successfully processed: {len(results['success'])}")
    print(f"Failed:              {len(results['failed'])}")
    print(f"Skipped:             {len(results['skipped'])}")
    print(f"Total rows read:     {results['total_rows_read']:,}")
    print(f"Total rows written:  {results['total_rows_written']:,}")
    
    if results["success"]:
        print(f"\n✅ Successful tables:")
        for table in results["success"]:
            print(f"   - {table}")
    
    if results["failed"]:
        print(f"\n❌ Failed tables:")
        for table in results["failed"]:
            print(f"   - {table}")
    
    if results["skipped"]:
        print(f"\n⚠️  Skipped tables:")
        for table in results["skipped"]:
            print(f"   - {table}")
    
    print(f"\n{'='*80}\n")
    
    return results


# ============================================================================
# Usage Examples
# ============================================================================

# Example 1: Process single table
# rows_read, rows_written, rows_rejected = process_gold_table(
#     gold_table_name="dim_customer",
#     batch_day="20251218",
#     job_id_str=job_id_str
# )

# Example 2: Process all tables
# results = process_all_gold_tables(
#     batch_day="20251218",
#     job_id_str=job_id_str,
#     exclude_from_comparison=["last_modified_by", "audit_timestamp"]
# )
results = process_all_gold_tables(
     batch_day="20251218",
     job_id_str=job_id_str,
     exclude_from_comparison=["last_modified_by", "audit_timestamp"]
 )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Get metadata mapping for Gold layer**

# CELL ********************

# Get all tables from lh_silver lakehouse
tables = spark.catalog.listTables("lh_gold")

# Collect column info for all tables
all_columns = []

for table in tables:
    table_name = f"lh_gold.{table.name}"
    
    try:
        # Get columns for this table
        columns = spark.table(table_name).columns
        
        # Add to list with table name
        for i, col in enumerate(columns):
            all_columns.append((table.name, i+1, col))
        
        print(f"✓ Processed: {table.name} ({len(columns)} columns)")
        
    except Exception as e:
        print(f"✗ Error processing {table.name}: {str(e)}")

# Create DataFrame
df_column_info = spark.createDataFrame(
    all_columns, 
    ["table_name", "ordinal_position", "column_name"]
)

# Create temp view
df_column_info.createOrReplaceTempView("column_info_gold")

print(f"\n✓ Created temp view 'column_info_gold' with {df_column_info.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Generate Create scripts for Gold layer**

# CELL ********************

df=spark.sql("""
    selecT 
        ci.table_name as gold_table_name,
        ci.ordinal_position,
        ci.column_name as silver_column_name,
        coalesce(c.gold_column_name,ci.column_name) as gold_column_name
    from column_info_gold ci
    left join lh_metadata.metadata_table_column_setup c
    on ci.table_name = c.gold_table_name
    and ci.column_name = c.silver_column_name
""")
df.createOrReplaceTempView("gold_gen_metadata")

df_gen=spark.sql("""
SELECT
    concat(
        '-- source [lh_gold].[dbo].[', gold_table_name, ']', chr(10),
        '-- target [base].[', gold_table_name, ']', chr(10),
        'CREATE OR ALTER VIEW [base].[', gold_table_name, ']', chr(10),
        'AS', chr(10),
        'SELECT', chr(10),
        concat_ws(
            concat(',', chr(10)),
            transform(
                sort_array(
                    collect_list(
                        struct(
                            coalesce(ordinal_position, 999) AS ord,
                            concat(
                                '    ',
                                CASE
                                    WHEN silver_column_name <> gold_column_name
                                        THEN concat('[', silver_column_name, '] AS [', gold_column_name, ']')
                                    ELSE concat('[', silver_column_name, ']')
                                END
                            ) AS col
                        )
                    )
                ),
                x -> x.col
            )
        ),
        chr(10),
        'FROM [lh_gold].[dbo].[', gold_table_name, ']; 
GO', chr(10)
    ) AS create_view_sql
FROM gold_gen_metadata
GROUP BY gold_table_name
""")

from pyspark.sql.functions import col, concat, lit

# -------------------------------------------------------
# 1. Prepare output DataFrame (one SQL block per row)
# -------------------------------------------------------
df_out = df_gen.select(
    concat(col("create_view_sql"), lit("\n")).alias("value")
)

# -------------------------------------------------------
# 2. Target OneLake path (Lakehouse Files area)
# -------------------------------------------------------
output_dir = (
    "abfss://d626fea3-3116-4af5-9cfb-9196dd43cba1@onelake.dfs.fabric.microsoft.com/61b624c2-0c32-4f59-8806-6ad9e84e54c5/Files/GoldWHViewGenerator"
)

# -------------------------------------------------------
# 3. Write SINGLE file (coalesce = 1)
# -------------------------------------------------------
(
    df_out
    .coalesce(1)
    .write
    .mode("overwrite")
    .text(output_dir)
)
df_out.write.mode("overwrite").format("delta").saveAsTable("lh_metadata.gold_view_generator_sql")

# -------------------------------------------------------
# 4. Rename Spark part file → GoldViewGenerator.sql
# -------------------------------------------------------
files = notebookutils.fs.ls(output_dir)

for f in files:
    if f.name.startswith("part-"):
        notebookutils.fs.mv(
            f.path,
            f"{output_dir}/GoldViewGenerator.sql",
            overwrite=True
        )

print("✅ GoldViewGenerator.sql created successfully")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Don't forget deploy views in wh_gold**
