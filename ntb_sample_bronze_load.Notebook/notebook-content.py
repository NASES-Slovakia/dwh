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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **IAM sample csv files Load to Bronze**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, 
    sha2, concat_ws, coalesce, lit
)
from datetime import datetime

# Configuration
batch_day = "20251222"
source_folder_path = "Files/IAM"  # Folder containing CSV files
source = "IAM"  # Source system prefix
job_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Initialize Spark session (already available in Fabric notebook)
spark = spark

# Add audit columns for bronze layer
def add_bronze_metadata(df):
    """Add standard bronze layer metadata columns"""
    return df \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_batch_day", lit(batch_day)) \
        .withColumn("_row_hash", sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in df.columns]), 256)) \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_job_id", lit(job_id))

# Get list of CSV files in folder
def get_csv_files(folder_path):
    """Get list of CSV files in the folder"""
    try:
        files = mssparkutils.fs.ls(folder_path)
        csv_files = [f.name for f in files if f.name.lower().endswith('.csv') and not f.isDir]
        return csv_files
    except Exception as e:
        print(f"Error reading files from {folder_path}: {str(e)}")
        return []

# Load single CSV file to bronze table
def load_csv_to_bronze(csv_filename, folder_path, source_prefix):
    """Load CSV file to bronze layer using filename as table name"""
    
    file_path = f"{folder_path}/{csv_filename}"
    # Extract table name from filename (remove .csv extension)
    table_name = csv_filename.replace('.csv', '').replace('.CSV', '')
    bronze_table = f"{source_prefix}_{table_name}"
    
    print(f"\nProcessing file: {csv_filename}")
    print(f"Target table: {bronze_table}")
    
    try:
        # Read CSV file
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", ';') \
            .option("quote", "'") \
            .csv(file_path)
        
        # Check record count
        record_count = df.count()
        print(f"Records found: {record_count}")
        
        if record_count == 0:
            print(f"Warning: No data found in {file_path}")
            return False
        
        # Add bronze metadata columns
        df_bronze = add_bronze_metadata(df)
        
        # Write to bronze delta table
        df_bronze.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(bronze_table)
        
        print(f"✓ Successfully loaded {record_count} records to {bronze_table}")
        return True
        
    except Exception as e:
        print(f"✗ Error loading file {csv_filename}: {str(e)}")
        return False

# Main execution
def main():
    """Main execution function"""
    
    print("="*80)
    print(f"Bronze Load Process Started - {datetime.now()}")
    print("="*80)
    
    # Get all CSV files
    csv_files = get_csv_files(source_folder_path)
    
    if not csv_files:
        print(f"No CSV files found in {source_folder_path}")
        return
    
    print(f"\nFound {len(csv_files)} CSV files: {csv_files}")
    
    # Process each CSV file
    results = {}
    for csv_file in csv_files:
        success = load_csv_to_bronze(csv_file, source_folder_path, source)
        results[csv_file] = "Success" if success else "Failed"
    
    # Summary
    print("\n" + "="*80)
    print("Bronze Load Summary")
    print("="*80)
    for file, status in results.items():
        status_symbol = "✓" if status == "Success" else "✗"
        print(f"{status_symbol} {file}: {status}")
    
    successful = sum(1 for s in results.values() if s == "Success")
    print(f"\nTotal: {successful}/{len(results)} files loaded successfully")
    print("\nProcess completed!")

# Execute
main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **CUD Bronze sample load**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, 
    sha2, concat_ws, coalesce, lit
)
from datetime import datetime

# Configuration
batch_day = "20251222"
source_folder_path = "Files/CUD"  # Folder containing CSV files
source = "CUD"  # Source system prefix
job_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Initialize Spark session (already available in Fabric notebook)
spark = spark

# Add audit columns for bronze layer
def add_bronze_metadata(df):
    """Add standard bronze layer metadata columns"""
    return df \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_batch_day", lit(batch_day)) \
        .withColumn("_row_hash", sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in df.columns]), 256)) \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_job_id", lit(job_id))

# Get list of CSV files in folder
def get_csv_files(folder_path):
    """Get list of CSV files in the folder"""
    try:
        files = mssparkutils.fs.ls(folder_path)
        csv_files = [f.name for f in files if f.name.lower().endswith('.csv') and not f.isDir]
        return csv_files
    except Exception as e:
        print(f"Error reading files from {folder_path}: {str(e)}")
        return []

# Load single CSV file to bronze table
def load_csv_to_bronze(csv_filename, folder_path, source_prefix):
    """Load CSV file to bronze layer using filename as table name"""
    
    file_path = f"{folder_path}/{csv_filename}"
    # Extract table name from filename (remove .csv extension)
    table_name = csv_filename.replace('.csv', '').replace('.CSV', '')
    bronze_table = f"{source_prefix}_{table_name}"
    
    print(f"\nProcessing file: {csv_filename}")
    print(f"Target table: {bronze_table}")
    
    try:
        # Read CSV file
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", '|') \
            .option("quote", "'") \
            .csv(file_path)
        
        # Check record count
        record_count = df.count()
        print(f"Records found: {record_count}")
        
        if record_count == 0:
            print(f"Warning: No data found in {file_path}")
            return False
        
        # Add bronze metadata columns
        df_bronze = add_bronze_metadata(df)
        
        # Write to bronze delta table
        df_bronze.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(bronze_table)
        
        print(f"✓ Successfully loaded {record_count} records to {bronze_table}")
        return True
        
    except Exception as e:
        print(f"✗ Error loading file {csv_filename}: {str(e)}")
        return False

# Main execution
def main():
    """Main execution function"""
    
    print("="*80)
    print(f"Bronze Load Process Started - {datetime.now()}")
    print("="*80)
    
    # Get all CSV files
    csv_files = get_csv_files(source_folder_path)
    
    if not csv_files:
        print(f"No CSV files found in {source_folder_path}")
        return
    
    print(f"\nFound {len(csv_files)} CSV files: {csv_files}")
    
    # Process each CSV file
    results = {}
    for csv_file in csv_files:
        success = load_csv_to_bronze(csv_file, source_folder_path, source)
        results[csv_file] = "Success" if success else "Failed"
    
    # Summary
    print("\n" + "="*80)
    print("Bronze Load Summary")
    print("="*80)
    for file, status in results.items():
        status_symbol = "✓" if status == "Success" else "✗"
        print(f"{status_symbol} {file}: {status}")
    
    successful = sum(1 for s in results.values() if s == "Success")
    print(f"\nTotal: {successful}/{len(results)} files loaded successfully")
    print("\nProcess completed!")

# Execute
main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **G2G Bronze sample load**

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, 
    sha2, concat_ws, coalesce, lit
)
from datetime import datetime

# Configuration
batch_day = "20251222"
source_folder_path = "Files/G2G"  # Folder containing CSV files
source = "G2G"  # Source system prefix
job_id = datetime.now().strftime("%Y%m%d_%H%M%S")

# Initialize Spark session (already available in Fabric notebook)
spark = spark

# Add audit columns for bronze layer
def add_bronze_metadata(df):
    """Add standard bronze layer metadata columns"""
    return df \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_batch_day", lit(batch_day)) \
        .withColumn("_row_hash", sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in df.columns]), 256)) \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_job_id", lit(job_id))

# Get list of CSV files in folder
def get_csv_files(folder_path):
    """Get list of CSV files in the folder"""
    try:
        files = mssparkutils.fs.ls(folder_path)
        csv_files = [f.name for f in files if f.name.lower().endswith('.csv') and not f.isDir]
        return csv_files
    except Exception as e:
        print(f"Error reading files from {folder_path}: {str(e)}")
        return []

# Load single CSV file to bronze table
def load_csv_to_bronze(csv_filename, folder_path, source_prefix):
    """Load CSV file to bronze layer using filename as table name"""
    
    file_path = f"{folder_path}/{csv_filename}"
    # Extract table name from filename (remove .csv extension)
    table_name = csv_filename.replace('.csv', '').replace('.CSV', '')
    bronze_table = f"{source_prefix}_{table_name}"
    
    print(f"\nProcessing file: {csv_filename}")
    print(f"Target table: {bronze_table}")
    
    try:
        # Read CSV file
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", ';') \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("multiLine", "true") \
            .csv(file_path)
                
        # Check record count
        record_count = df.count()
        print(f"Records found: {record_count}")
        
        if record_count == 0:
            print(f"Warning: No data found in {file_path}")
            return False
        
        # Add bronze metadata columns
        df_bronze = add_bronze_metadata(df)
        
        # Write to bronze delta table
        df_bronze.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(bronze_table)
        
        print(f"✓ Successfully loaded {record_count} records to {bronze_table}")
        return True
        
    except Exception as e:
        print(f"✗ Error loading file {csv_filename}: {str(e)}")
        return False

# Main execution
def main():
    """Main execution function"""
    
    print("="*80)
    print(f"Bronze Load Process Started - {datetime.now()}")
    print("="*80)
    
    # Get all CSV files
    csv_files = get_csv_files(source_folder_path)
    
    if not csv_files:
        print(f"No CSV files found in {source_folder_path}")
        return
    
    print(f"\nFound {len(csv_files)} CSV files: {csv_files}")
    
    # Process each CSV file
    results = {}
    for csv_file in csv_files:
        success = load_csv_to_bronze(csv_file, source_folder_path, source)
        results[csv_file] = "Success" if success else "Failed"
    
    # Summary
    print("\n" + "="*80)
    print("Bronze Load Summary")
    print("="*80)
    for file, status in results.items():
        status_symbol = "✓" if status == "Success" else "✗"
        print(f"{status_symbol} {file}: {status}")
    
    successful = sum(1 for s in results.values() if s == "Success")
    print(f"\nTotal: {successful}/{len(results)} files loaded successfully")
    print("\nProcess completed!")

# Execute
main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, 
    sha2, concat_ws, coalesce, lit
)
from datetime import datetime

# Configuration
batch_day = "20251222"
source_folder_path = "Files/G2G"
source = "G2G"
job_id = datetime.now().strftime("%Y%m%d_%H%M%S")

spark = spark

def add_bronze_metadata(df):
    """Add standard bronze layer metadata columns"""
    return df \
        .withColumn("_source_file", input_file_name()) \
        .withColumn("_batch_day", lit(batch_day)) \
        .withColumn("_row_hash", sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in df.columns]), 256)) \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_job_id", lit(job_id))

def get_csv_files(folder_path):
    """Get list of CSV files in the folder"""
    try:
        files = mssparkutils.fs.ls(folder_path)
        csv_files = [f.name for f in files if f.name.lower().endswith('.csv') and not f.isDir]
        return csv_files
    except Exception as e:
        print(f"Error reading files from {folder_path}: {str(e)}")
        return []

def load_csv_to_bronze(csv_filename, folder_path, source_prefix):
    """Load CSV file to bronze layer using filename as table name"""
    
    file_path = f"{folder_path}/{csv_filename}"
    table_name = csv_filename.replace('.csv', '').replace('.CSV', '')
    bronze_table = f"{source_prefix}_{table_name}"
    
    print(f"\nProcessing file: {csv_filename}")
    print(f"Target table: {bronze_table}")
    
    try:
        # CRITICAL: Your CSV files use:
        # - Semicolon (;) as delimiter
        # - Double quote (") for field enclosure
        # - Backslash (\) as escape character (not standard!)
        # - Literal newlines inside quoted fields (multiLine must be true)
        
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("delimiter", ';') \
            .option("quote", '"') \
            .option("escape", '\\') \
            .option("multiLine", "true") \
            .option("encoding", "UTF-8") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .csv(file_path)
        
        record_count = df.count()
        print(f"Records found: {record_count}")
        print(f"Schema columns: {len(df.columns)}")
        
        # Check for corrupt records
        if "_corrupt_record" in df.columns:
            corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
            if corrupt_count > 0:
                print(f"WARNING: Found {corrupt_count} corrupt records")
                df.filter(col("_corrupt_record").isNotNull()).select("_corrupt_record").show(3, truncate=100)
        
        # Display sample of first few column names
        print(f"Column names: {', '.join(df.columns[:10])}...")
        
        if record_count == 0:
            print(f"Warning: No data found in {file_path}")
            return False
        
        df_bronze = add_bronze_metadata(df)
        
        df_bronze.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(bronze_table)
        
        print(f"✓ Successfully loaded {record_count} records to {bronze_table}")
        return True
        
    except Exception as e:
        print(f"✗ Error loading file {csv_filename}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main execution function"""
    
    print("="*80)
    print(f"Bronze Load Process Started - {datetime.now()}")
    print("="*80)
    
    csv_files = get_csv_files(source_folder_path)
    
    if not csv_files:
        print(f"No CSV files found in {source_folder_path}")
        return
    
    print(f"\nFound {len(csv_files)} CSV files: {csv_files}")
    
    results = {}
    for csv_file in csv_files:
        success = load_csv_to_bronze(csv_file, source_folder_path, source)
        results[csv_file] = "Success" if success else "Failed"
    
    print("\n" + "="*80)
    print("Bronze Load Summary")
    print("="*80)
    for file, status in results.items():
        status_symbol = "✓" if status == "Success" else "✗"
        print(f"{status_symbol} {file}: {status}")
    
    successful = sum(1 for s in results.values() if s == "Success")
    print(f"\nTotal: {successful}/{len(results)} files loaded successfully")
    print("\nProcess completed!")

main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit,
    sha2, concat_ws, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
import re

# Configuration
batch_day = "20251222"
source_folder_path = "Files/G2G"
source = "G2G"
job_id = datetime.now().strftime("%Y%m%d_%H%M%S")

spark = spark

def add_bronze_metadata(df):
    """Add standard bronze layer metadata columns"""
    return df \
        .withColumn("_source_file", lit("manual_parse")) \
        .withColumn("_batch_day", lit(batch_day)) \
        .withColumn("_row_hash", sha2(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in df.columns]), 256)) \
        .withColumn("_load_timestamp", current_timestamp()) \
        .withColumn("_job_id", lit(job_id))

def get_csv_files(folder_path):
    """Get list of CSV files in the folder"""
    try:
        files = mssparkutils.fs.ls(folder_path)
        csv_files = [f.name for f in files if f.name.lower().endswith('.csv') and not f.isDir]
        return csv_files
    except Exception as e:
        print(f"Error reading files from {folder_path}: {str(e)}")
        return []

def parse_malformed_csv(file_path):
    """
    Parse CSV that has:
    - Semicolon delimiter
    - Double quote enclosure
    - Backslash escaped quotes (\\") inside fields
    - Literal newlines inside quoted fields
    """
    print(f"  Reading and parsing CSV file...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    lines = content.split('\n')
    
    # Parse header
    header_line = lines[0].strip('\r')
    header = []
    for field in re.findall(r'"([^"]*)"', header_line):
        header.append(field)
    
    print(f"  Found {len(header)} columns in header")
    
    # Parse data rows - identify logical rows by lines starting with quoted number
    logical_rows = []
    current_row_lines = []
    
    for line in lines[1:]:
        line = line.rstrip('\r')
        if not line:
            continue
        
        # Check if this starts a new row (begins with "number";)
        if re.match(r'^"\d+";', line):
            # Save previous row if exists
            if current_row_lines:
                full_row = '\n'.join(current_row_lines)
                logical_rows.append(full_row)
            # Start new row
            current_row_lines = [line]
        else:
            # Continue current row (multiline field)
            if current_row_lines:
                current_row_lines.append(line)
    
    # Don't forget last row
    if current_row_lines:
        full_row = '\n'.join(current_row_lines)
        logical_rows.append(full_row)
    
    print(f"  Found {len(logical_rows)} logical data rows")
    
    # Parse each logical row into fields
    parsed_rows = []
    for row_text in logical_rows:
        fields = parse_row_fields(row_text)
        if len(fields) == len(header):
            parsed_rows.append(fields)
    
    print(f"  Successfully parsed {len(parsed_rows)} complete rows")
    
    return header, parsed_rows

def parse_row_fields(row_text):
    """Parse a single CSV row into fields, handling escaped quotes and embedded semicolons"""
    fields = []
    current_field = ""
    in_quotes = False
    i = 0
    
    while i < len(row_text):
        char = row_text[i]
        
        if char == '\\' and i + 1 < len(row_text) and row_text[i + 1] == '"':
            # Escaped quote - add the quote to field
            current_field += '"'
            i += 2  # Skip both backslash and quote
            continue
        elif char == '"':
            # Toggle quote state
            in_quotes = not in_quotes
            i += 1
            continue
        elif char == ';' and not in_quotes:
            # Field separator outside quotes
            fields.append(current_field)
            current_field = ""
            i += 1
            continue
        else:
            # Regular character
            current_field += char
            i += 1
    
    # Add last field
    if current_field or row_text.endswith(';'):
        fields.append(current_field)
    
    return fields

def load_csv_to_bronze(csv_filename, folder_path, source_prefix):
    """Load CSV file to bronze layer using filename as table name"""
    
    file_path = f"{folder_path}/{csv_filename}"
    table_name = csv_filename.replace('.csv', '').replace('.CSV', '')
    bronze_table = f"{source_prefix}_{table_name}"
    
    print(f"\nProcessing file: {csv_filename}")
    print(f"Target table: {bronze_table}")
    
    try:
        # Get full path
        full_path = f"/lakehouse/default/{file_path}"
        
        # Parse the malformed CSV
        header, rows = parse_malformed_csv(full_path)
        
        if not rows:
            print(f"Warning: No data found in {file_path}")
            return False
        
        # Create Spark DataFrame
        schema = StructType([StructField(name, StringType(), True) for name in header])
        df = spark.createDataFrame(rows, schema)
        
        record_count = df.count()
        print(f"  Created DataFrame with {record_count} records")
        
        # Show sample
        print(f"  Sample data:")
        df.select("NKEY", "class", "message_size").show(3, truncate=False)
        
        # Add bronze metadata
        df_bronze = add_bronze_metadata(df)
        
        # Write to delta table
        df_bronze.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(bronze_table)
        
        print(f"✓ Successfully loaded {record_count} records to {bronze_table}")
        return True
        
    except Exception as e:
        print(f"✗ Error loading file {csv_filename}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main execution function"""
    
    print("="*80)
    print(f"Bronze Load Process Started - {datetime.now()}")
    print("="*80)
    
    csv_files = get_csv_files(source_folder_path)
    
    if not csv_files:
        print(f"No CSV files found in {source_folder_path}")
        return
    
    print(f"\nFound {len(csv_files)} CSV files: {csv_files}")
    
    results = {}
    for csv_file in csv_files:
        success = load_csv_to_bronze(csv_file, source_folder_path, source)
        results[csv_file] = "Success" if success else "Failed"
    
    print("\n" + "="*80)
    print("Bronze Load Summary")
    print("="*80)
    for file, status in results.items():
        status_symbol = "✓" if status == "Success" else "✗"
        print(f"{status_symbol} {file}: {status}")
    
    successful = sum(1 for s in results.values() if s == "Success")
    print(f"\nTotal: {successful}/{len(results)} files loaded successfully")
    print("\nProcess completed!")

main()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
