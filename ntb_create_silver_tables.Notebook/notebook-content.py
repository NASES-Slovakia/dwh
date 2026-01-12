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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd

# Read Excel file
pdf = pd.read_excel(
    "abfss://d626fea3-3116-4af5-9cfb-9196dd43cba1@onelake.dfs.fabric.microsoft.com/61b624c2-0c32-4f59-8806-6ad9e84e54c5/Files/Mapping/DWH_Systemy_IF_1.0.xlsx",
    sheet_name="Atribúty"
)

df = spark.createDataFrame(pdf)

import re
import unicodedata

def normalize_col(name):
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = re.sub(r"[ ,;{}()\n\t=]", "_", name)
    return name.lower()

for c in df.columns:
    df = df.withColumnRenamed(c, normalize_col(c))

df.write.format("delta").mode("overwrite").saveAsTable("lh_metadata.table_config")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# Read Excel file
pdf = pd.read_excel(
    "abfss://d626fea3-3116-4af5-9cfb-9196dd43cba1@onelake.dfs.fabric.microsoft.com/61b624c2-0c32-4f59-8806-6ad9e84e54c5/Files/Mapping/DWH_Systemy_IF_1.0.xlsx",
    sheet_name="Tabuľky"
)

df = spark.createDataFrame(pdf)

import re
import unicodedata

def normalize_col(name):
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = re.sub(r"[ ,;{}()\n\t=]", "_", name)
    return name.lower()

for c in df.columns:
    df = df.withColumnRenamed(c, normalize_col(c))

#display(df)
df.write.format("delta").mode("overwrite").saveAsTable("lh_metadata.table_type_config")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from lh_metadata.table_config

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select distinct 
# MAGIC     tt.typ as table_type, --DIM/FACT
# MAGIC     tt.nazov_tabulky_gold as gold_table_name,
# MAGIC     tc.nazov_tabulky as silver_table_name,
# MAGIC     tc.atribut as silver_column_name,
# MAGIC     tc.gold_name as gold_column_name,
# MAGIC     tc.data_type_final as gold_column_data_type,
# MAGIC     case when tc.povinny = 'ano' then TRUE ELSE FALSE END AS gold_column_is_nullable,
# MAGIC     case when tc.`pk?` = 'PK' then TRUE ELSE FALSE END AS gold_column_is_pk,
# MAGIC     ci.ordinal_position
# MAGIC --,*
# MAGIC  from lh_metadata.table_config tc
# MAGIC left join lh_metadata.table_type_config tt 
# MAGIC     on tt.system = tc.system
# MAGIC     and tc.nazov_tabulky = tt.nazov_tabulky_bronze
# MAGIC left join column_info ci
# MAGIC     on ci.table_name = tc.nazov_tabulky
# MAGIC     and ci.column_name = tc.atribut
# MAGIC where tc.system like '%DEV'
# MAGIC order by silver_table_name, ordinal_position

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

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
display(df_column_info.orderBy("table_name","column_name", "ordinal_position"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

schema_name = "lh_bronze"
prefixes = ("sm_", "cnm_")

rows = []

tables = spark.catalog.listTables(schema_name)

for t in tables:
    if t.tableType.lower() not in ("managed", "external"):
        continue
    if not t.name.lower().startswith(prefixes):
        continue

    full_table = f"{schema_name}.{t.name}"
    df = spark.table(full_table)

    # Add column order using enumerate
    for idx, field in enumerate(df.schema.fields, start=1):
        rows.append(
            Row(
                schema_name=schema_name,
                table_name=t.name,
                column_name=field.name,
                data_type=field.dataType.simpleString(),
                column_order=idx
            )
        )

result_df = spark.createDataFrame(rows)

# Create temp view for SQL join
result_df.createOrReplaceTempView("temp_metadata")

# Join with table_config as before
metadata_df = spark.sql("""
                        with base as (
                        select 
                            m.*,
                            c.data_type_final,
                            case when c.povinny = 'ano' then 'NOT NULL' ELSE NULL END AS NULLABLE
                        from temp_metadata m
                        left join lh_metadata.table_config c
                            on   m.column_name = c.atribut
                            and  m.table_name = c.nazov_tabulky
                            and     c.system in ('CNM - DEV','SM - DEV')
                        )
                        select 
                        * from base --where data_type != data_type_final
                        ORDER BY table_name, column_order
""")

# Display ordered by table and column order
display(metadata_df.orderBy("table_name", "column_order"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Get list of tables
tables = [row.table_name for row in metadata_df.select("table_name").distinct().collect()]

ddl_statements = []

for table in tables:
    # Filter metadata for the current table and order by column_order
    cols = (
        metadata_df.filter(F.col("table_name") == table)
        .orderBy("column_order")
        .collect()
    )

    # Generate column definitions
    column_defs = []
    for c in cols:
        col_def = f"{c['column_name']} {c['data_type_final']}"
        if c['NULLABLE'] == "NOT NULL":
            col_def += " NOT NULL"
        column_defs.append(col_def)

    # Join columns for readability
    columns_str = ",\n  ".join(column_defs)

    # Full CREATE TABLE statement
    ddl_stmt = f"""
CREATE TABLE lh_silver.{table} (
  {columns_str}
)
USING DELTA;
""".strip()

    ddl_statements.append(ddl_stmt)

# Print all DDLs
for ddl in ddl_statements:
    print("\n" + "-" * 80)
    print(ddl)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.cnm_notification_history (
# MAGIC   _key int NOT NULL,
# MAGIC   notification_key string NOT NULL,
# MAGIC   user_id string NOT NULL,
# MAGIC   locale string NOT NULL,
# MAGIC   email string,
# MAGIC   post_time timestamp NOT NULL,
# MAGIC   send_time timestamp NOT NULL,
# MAGIC   app_name string,
# MAGIC   reference_id string NOT NULL,
# MAGIC   success boolean,
# MAGIC   app_token string,
# MAGIC   phone_no string,
# MAGIC   queue_key int NOT NULL,
# MAGIC   removal_source int,
# MAGIC   sending_method int,
# MAGIC   uri string NOT NULL,
# MAGIC   dispatch_override timestamp,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.cnm_notification (
# MAGIC   _key string NOT NULL,
# MAGIC   _rev string,
# MAGIC   template_key string NOT NULL,
# MAGIC   flags boolean NOT NULL,
# MAGIC   frequency int NOT NULL,
# MAGIC   hour int NOT NULL,
# MAGIC   day_of_week int NOT NULL,
# MAGIC   day_of_month int NOT NULL,
# MAGIC   month int NOT NULL,
# MAGIC   deactivated boolean NOT NULL,
# MAGIC   owner string NOT NULL,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.cnm_notification_history_attempt (
# MAGIC   _key int NOT NULL,
# MAGIC   history_key int NOT NULL,
# MAGIC   attempt timestamp NOT NULL,
# MAGIC   success boolean NOT NULL,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.cnm_notification_template (
# MAGIC   _key string NOT NULL,
# MAGIC   _rev string NOT NULL,
# MAGIC   owner string NOT NULL,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.cnm_notification_template_images (
# MAGIC   notification_key string NOT NULL,
# MAGIC   filename string NOT NULL,
# MAGIC   base64data string NOT NULL,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.cnm_notification_history_params (
# MAGIC   history_key int NOT NULL,
# MAGIC   name string NOT NULL,
# MAGIC   value string NOT NULL,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.cnm_planned_job (
# MAGIC   key string NOT NULL,
# MAGIC   last_execution timestamp NOT NULL,
# MAGIC   last_started timestamp NOT NULL,
# MAGIC   last_count string,
# MAGIC   last_index string,
# MAGIC   paused boolean,
# MAGIC   last_id string,
# MAGIC   finished boolean,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.cnm_notification_template_text (
# MAGIC   notification_key string NOT NULL,
# MAGIC   type int NOT NULL,
# MAGIC   notification_type int NOT NULL,
# MAGIC   locale string NOT NULL,
# MAGIC   text string NOT NULL,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.cnm_notification_template_params (
# MAGIC   notification_key string NOT NULL,
# MAGIC   name string NOT NULL,
# MAGIC   type int NOT NULL,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.cnm_notification_queue (
# MAGIC   _key int NOT NULL,
# MAGIC   notification_key string,
# MAGIC   user_id string,
# MAGIC   email string,
# MAGIC   channel_override int,
# MAGIC   locale_override string,
# MAGIC   post_time timestamp NOT NULL,
# MAGIC   app_name string NOT NULL,
# MAGIC   reference_id string,
# MAGIC   attempts int NOT NULL,
# MAGIC   app_token string,
# MAGIC   last_send_attempt timestamp,
# MAGIC   phone_no string,
# MAGIC   uri string,
# MAGIC   dispatch_override string,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.sm_conversation_mukc_chat (
# MAGIC   id int,
# MAGIC   conversation_id string NOT NULL,
# MAGIC   mukc_chat_id string NOT NULL,
# MAGIC   created_on timestamp,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.cnm_notification_queue_params (
# MAGIC   queue_key int NOT NULL,
# MAGIC   name string NOT NULL,
# MAGIC   value string NOT NULL,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.sm_conversation_user (
# MAGIC   id int,
# MAGIC   conversation_id string NOT NULL,
# MAGIC   user_id string NOT NULL,
# MAGIC   created_on timestamp,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.sm_event (
# MAGIC   id int,
# MAGIC   conversation_id string NOT NULL,
# MAGIC   event_timestamp timestamp,
# MAGIC   event_type string,
# MAGIC   message_type string,
# MAGIC   message_content string,
# MAGIC   message_winning_intent string,
# MAGIC   message_winning_intent_confidence Int,
# MAGIC   message_invalid_intent boolean,
# MAGIC   action_name string,
# MAGIC   action_data string,
# MAGIC   slot_name string,
# MAGIC   slot_value string,
# MAGIC   operator_id string,
# MAGIC   operator_display_name string,
# MAGIC   created_on timestamp,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.sm_update_status (
# MAGIC   table_name string,
# MAGIC   update_in_progress boolean,
# MAGIC   last_update_started_on timestamp,
# MAGIC   last_update_finished_on timestamp,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC --------------------------------------------------------------------------------
# MAGIC CREATE TABLE lh_silver.sm_conversation (
# MAGIC   id int,
# MAGIC   conversation_id string NOT NULL,
# MAGIC   user_id string,
# MAGIC   mukc_chat_id string,
# MAGIC   archived_on timestamp,
# MAGIC   _source_file string,
# MAGIC   _batch_day string,
# MAGIC   _row_hash string,
# MAGIC   _load_timestamp timestamp,
# MAGIC   _job_id string
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
