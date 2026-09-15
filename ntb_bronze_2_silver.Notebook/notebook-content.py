# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a13e3d64-d65b-4196-a813-3475454ce68a",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "d626fea3-3116-4af5-9cfb-9196dd43cba1",
# META       "known_lakehouses": [
# META         {
# META           "id": "0abf8ee5-18ac-4c6f-85f2-d828a032ff48"
# META         },
# META         {
# META           "id": "4c73f3f2-9c1c-45d3-b6be-3607c949dc6d"
# META         },
# META         {
# META           "id": "a13e3d64-d65b-4196-a813-3475454ce68a"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# ============================================================================
# Parameters cell  (tag this cell 'parameters' so the Fabric pipeline can override)
# ============================================================================
source_system    = 'G2G'        # one of CNM, CUD, EDESK, G2G, IAM, LS, METAIS, SM
batch_day        = '20260728'   # YYYYMMDD. catch_up: inclusive UPPER bound; single: the one day
job_id           = ''           # optional; auto-generated when empty
pipeline_name    = 'ntb_bronze_2_silver'

# load_mode (every mode does ONE read + ONE write per table):
#   'catch_up' (default) -> select days that are new to silver OR re-ingested in
#                           bronze (watermark), up to batch_day. Daily incremental
#                           AND large recoveries both load in a single pass.
#   'bulk'               -> select the whole range [day_from .. batch_day] and
#                           reload it authoritatively. Use for initial history load.
#   'single'             -> exactly batch_day.
#   'explicit'           -> the days listed in batch_days.
load_mode        = 'batch'
batch_days       = ''           # explicit mode: comma-separated YYYYMMDD list
day_from         = ''           # catch_up: optional inclusive LOWER bound (YYYYMMDD)

# Quality gate: if rejected/read exceeds this fraction the table load is marked
# FAILED and silver is left untouched (0..1). 1.0 = never fail on rejections.
reject_threshold = '1.0'

# Optional explicit table list (comma-separated bronze table names). Empty = all
# in-scope bronze tables that have a matching silver table.
only_tables      = ''


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Imports & runtime constants
# ============================================================================
import uuid
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType
)

job_id_str   = job_id or datetime.now().strftime("%Y%m%d_%H%M%S")

# Read side: tolerant datetime PARSING — unparseable values become NULL (then
# rejections via parse_to_type's coalesce) instead of throwing SparkUpgradeException.
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
# Write side: allow pre-1582/1900 datetimes to be WRITTEN as-is (proleptic Gregorian)
# instead of aborting with WRITE_ANCIENT_DATETIME. Files are read only by Spark 3.0+/Fabric.
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

SOURCE       = source_system.strip().upper()
SOURCE_PFX   = SOURCE.lower() + "_"
REJECT_LIMIT = float(reject_threshold)
ONLY_TABLES  = [t.strip() for t in only_tables.split(",") if t.strip()]

LOAD_MODE    = str(load_mode).strip().lower()
if LOAD_MODE not in ("catch_up", "bulk", "single", "explicit"):
    LOAD_MODE = "catch_up"
EXPLICIT_DAYS = sorted({d.strip() for d in str(batch_days).split(",") if d.strip()})
DAY_FROM      = str(day_from).strip()

source_layer = "bronze"
target_layer = "silver"
BRONZE_LH    = "lh_bronze"
SILVER_LH    = "lh_silver"
METADATA_LH  = "lh_metadata"

# Lineage carried from bronze (NOT validated as payload).
BRONZE_META       = {"_source_file", "_batch_day", "_row_hash", "_load_timestamp", "_job_id"}
# Silver-side metadata columns present in the generated silver schema.
SILVER_ADDED_META = {"_silver_load_timestamp", "_silver_job_id", "_batch_month", "_bronze_load_timestamp"}
SILVER_CARRY_META = {"_batch_day", "_row_hash", "_source_file"}
SILVER_META_ALL   = SILVER_ADDED_META | SILVER_CARRY_META

# Tolerant parsing for date/timestamp targets. Bronze is all-string and sources
# deliver non-ISO formats (e.g. Slovak '4.9.2023 13:26:48' with unpadded d/M/H).
# Each format is tried in order, then a plain cast (handles ISO). Edit as needed.
TIMESTAMP_FORMATS = [
    "d.M.yyyy H:mm:ss",           # 4.9.2023 13:26:48
    "d.M.yyyy H:mm",              # 4.9.2023 13:26
    "yyyy-MM-dd HH:mm:ss.SSSSSS", # 2025-11-17 10:34:03.281722 (microseconds)
    "yyyy-MM-dd HH:mm:ss.SSS",    # milliseconds
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
]
DATE_FORMATS = [
    "d.M.yyyy",             # 4.9.2023
    "yyyy-MM-dd",
]

print(f"job_id_str    : {job_id_str}")
print(f"source_system : {SOURCE}")
print(f"batch_day     : {batch_day}")
print(f"reject_limit  : {REJECT_LIMIT}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Logging  -- single terminal row per (table, batch_day) load.
# log_pipeline_start is in-memory only; only log_pipeline_end writes.
# ============================================================================
LOG_SCHEMA = StructType([
    StructField("log_id",            StringType(),    False),
    StructField("batch_day",         StringType(),    False),
    StructField("pipeline_name",     StringType(),    False),
    StructField("source_layer",      StringType(),    False),
    StructField("target_layer",      StringType(),    False),
    StructField("source_table",      StringType(),    False),
    StructField("target_table",      StringType(),    False),
    StructField("start_time",        TimestampType(), False),
    StructField("end_time",          TimestampType(), True),
    StructField("status",            StringType(),    False),
    StructField("operation",         StringType(),    False),
    StructField("rows_read",         LongType(),      True),
    StructField("rows_written",      LongType(),      True),
    StructField("rows_rejected",     LongType(),      True),
    StructField("error_message",     StringType(),    True),
    StructField("job_id",            StringType(),    True),
    StructField("created_timestamp", TimestampType(), False),
])

def log_pipeline_start(batch_day, source_table, target_table, operation):
    """In-memory only: returns (log_id, start_time). No write -> no duplicate rows."""
    return str(uuid.uuid4()), datetime.now()

def log_pipeline_end(log_id, batch_day, source_table, target_table,
                     status, operation, rows_read, rows_written,
                     rows_rejected=0, error_message=None, start_time=None):
    """Write exactly one terminal row to lh_metadata.log_table_loads."""
    start_time = start_time or datetime.now()
    row = [(
        log_id, batch_day, pipeline_name, source_layer, target_layer,
        source_table, target_table, start_time, datetime.now(), status,
        operation, rows_read, rows_written, rows_rejected,
        (error_message[:500] if error_message else None),
        job_id_str, datetime.now(),
    )]
    (spark.createDataFrame(row, LOG_SCHEMA)
        .write.format("delta").mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{METADATA_LH}.log_table_loads"))
    emoji = {"SUCCESS": "✅", "FAILED": "❌"}.get(status, "⚠️")
    print(f"  {emoji} {status}  read={rows_read} written={rows_written} rejected={rows_rejected}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Rejection log  -- one row per rejected source row (first violation + full set)
# ============================================================================
REJECTION_SCHEMA = StructType([
    StructField("rejection_id",        StringType(),    False),
    StructField("job_id",              StringType(),    False),
    StructField("batch_day",           StringType(),    False),
    StructField("source_table",        StringType(),    False),
    StructField("target_table",        StringType(),    False),
    StructField("source_row_hash",     StringType(),    True),
    StructField("rejection_reason",    StringType(),    False),
    StructField("failed_column",       StringType(),    True),
    StructField("failed_value",        StringType(),    True),
    StructField("expected_type",       StringType(),    True),
    StructField("violation_count",     LongType(),      True),
    StructField("all_violations",      StringType(),    True),   # JSON array
    StructField("row_data",            StringType(),    True),   # JSON of payload
    StructField("rejection_timestamp", TimestampType(), False),
])

def ensure_rejection_table():
    if not spark.catalog.tableExists(f"{METADATA_LH}.rejection_log"):
        (spark.createDataFrame([], REJECTION_SCHEMA)
            .write.format("delta").mode("overwrite")
            .saveAsTable(f"{METADATA_LH}.rejection_log"))
        print("  ✓ created lh_metadata.rejection_log")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Validation  -- cast bronze (all-string) to the silver contract.
# One pass builds a _violations array; rows with any violation are rejected.
#   - TYPE_CAST : source value present but does not cast to the target type
#   - NOT_NULL  : source value NULL/empty on a NOT NULL silver column
# ============================================================================
from pyspark.sql.types import TimestampType, DateType

def parse_to_type(src, dtype):
    """Cast a bronze string to the silver target type. For timestamp/date it
    tries TIMESTAMP_FORMATS / DATE_FORMATS first (tolerating non-ISO input like
    '4.9.2023 13:26:48'), then a plain cast. Returns null only if nothing matches."""
    if isinstance(dtype, TimestampType):
        return F.coalesce(*[F.to_timestamp(src, f) for f in TIMESTAMP_FORMATS], src.cast(dtype))
    if isinstance(dtype, DateType):
        return F.coalesce(*[F.to_date(src, f) for f in DATE_FORMATS], src.cast(dtype))
    return src.cast(dtype)

def validate_against_silver(df_bronze, silver_schema):
    """Return (df_valid_payload, df_rejected) using the silver schema as contract."""
    bronze_cols = {c.lower(): c for c in df_bronze.columns}

    # payload (non-metadata) fields the silver table expects
    targets = [f for f in silver_schema.fields if f.name not in SILVER_META_ALL]

    # keep lineage for valid rows + rejection reporting
    carry = [c for c in ("_row_hash", "_source_file", "_batch_day", "_load_timestamp")
             if c in df_bronze.columns]
    payload_src = [bronze_cols[f.name.lower()] for f in targets if f.name.lower() in bronze_cols]
    df = df_bronze.withColumn("_row_json", F.to_json(F.struct(*payload_src)) if payload_src else F.lit(None).cast("string"))

    VSTRUCT = "struct<column:string,reason:string,value:string,expected:string>"
    null_struct = F.lit(None).cast(VSTRUCT)
    violation_structs = []
    for f in targets:
        name, dtype, nullable = f.name, f.dataType, f.nullable
        src = F.col(bronze_cols[name.lower()]) if name.lower() in bronze_cols else F.lit(None).cast("string")
        casted   = parse_to_type(src, dtype)
        type_bad = src.isNotNull() & casted.isNull()                     # un-castable value
        null_bad = (~F.lit(nullable)) & src.isNull()                     # required but missing
        violation_structs.append(
            F.when(type_bad, F.struct(
                F.lit(name).alias("column"),
                F.lit("TYPE_CAST").alias("reason"),
                src.cast("string").alias("value"),
                F.lit(dtype.simpleString()).alias("expected")).cast(VSTRUCT))
             .when(null_bad, F.struct(
                F.lit(name).alias("column"),
                F.lit("NOT_NULL").alias("reason"),
                F.lit(None).cast("string").alias("value"),
                F.lit(dtype.simpleString() + " NOT NULL").alias("expected")).cast(VSTRUCT))
             .otherwise(null_struct)
        )

    if violation_structs:
        df = df.withColumn(
            "_violations",
            F.filter(F.array(*violation_structs), lambda x: x["column"].isNotNull()),
        )
    else:
        df = df.withColumn("_violations", F.array().cast(f"array<{VSTRUCT}>"))

    df = df.withColumn("_vcount", F.size("_violations"))
    df_valid    = df.filter(F.col("_vcount") == 0)
    df_rejected = df.filter(F.col("_vcount") > 0)

    # cast valid rows to the exact silver payload types
    select_exprs = []
    for f in targets:
        if f.name.lower() in bronze_cols:
            select_exprs.append(parse_to_type(F.col(bronze_cols[f.name.lower()]), f.dataType).alias(f.name))
        else:
            select_exprs.append(F.lit(None).cast(f.dataType).alias(f.name))
    for c in carry:
        select_exprs.append(F.col(c))
    df_valid_payload = df_valid.select(*select_exprs)

    return df_valid_payload, df_rejected

def write_rejections(df_rejected, source_table, target_table):
    first = F.element_at(F.col("_violations"), 1)
    bd = F.col("_batch_day") if "_batch_day" in df_rejected.columns else F.lit(None).cast("string")
    out = df_rejected.select(
        F.expr("uuid()").alias("rejection_id"),
        F.lit(job_id_str).alias("job_id"),
        bd.alias("batch_day"),
        F.lit(source_table).alias("source_table"),
        F.lit(target_table).alias("target_table"),
        (F.col("_row_hash") if "_row_hash" in df_rejected.columns else F.lit(None).cast("string")).alias("source_row_hash"),
        first["reason"].alias("rejection_reason"),
        first["column"].alias("failed_column"),
        first["value"].alias("failed_value"),
        first["expected"].alias("expected_type"),
        F.col("_vcount").cast("long").alias("violation_count"),
        F.to_json(F.col("_violations")).alias("all_violations"),
        F.col("_row_json").alias("row_data"),
        F.current_timestamp().alias("rejection_timestamp"),
    )
    (out.write.format("delta").mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{METADATA_LH}.rejection_log"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Per-table load: bronze[batch_day] -> silver via replaceWhere (atomic, scoped)
# ============================================================================
def promote(table_name, days, label):
    """Promote a set of bronze days to silver in one read+validate+write.

    days  : list of YYYYMMDD strings to load (1 for incremental, many for bulk)
    label : value stored in log_table_loads.batch_day (a day, or a range string)

    Write strategy:
      - empty silver  -> plain overwrite (fastest; ideal for initial 6y load)
      - non-empty     -> replaceWhere over the days that actually produced valid
                         rows, so all-rejected days never wipe prior good data.
    """
    bronze_table = f"{BRONZE_LH}.{table_name}"
    silver_table = f"{SILVER_LH}.{table_name}"

    print(f"\n{'-'*80}\n{bronze_table}  ->  {silver_table}  ({label}, {len(days)} day(s))\n{'-'*80}")
    log_id, start_time = log_pipeline_start(label, bronze_table, silver_table, "BULK" if len(days) > 1 else "REPLACE_WHERE")
    rows_read = 0

    try:
        if not spark.catalog.tableExists(silver_table):
            log_pipeline_end(log_id, label, bronze_table, silver_table,
                             "FAILED", "REPLACE_WHERE", 0, 0,
                             error_message="Silver table does not exist (run ntb_create_silver_tables).",
                             start_time=start_time)
            return "FAILED"

        df_bronze = spark.table(bronze_table).filter(F.col("_batch_day").isin(days))
        rows_read = df_bronze.count()
        if rows_read == 0:
            log_pipeline_end(log_id, label, bronze_table, silver_table,
                             "SUCCESS", "REPLACE_WHERE", 0, 0,
                             error_message="No bronze rows for the requested day(s).", start_time=start_time)
            return "SUCCESS"

        silver_schema = spark.table(silver_table).schema
        df_valid, df_rejected = validate_against_silver(df_bronze, silver_schema)

        valid_count    = df_valid.count()
        rejected_count = rows_read - valid_count

        if rejected_count > 0:
            write_rejections(df_rejected, bronze_table, silver_table)
            print(f"  📋 logged {rejected_count} rejected row(s) to rejection_log")

        # quality gate (over the whole set)
        if rows_read and (rejected_count / rows_read) > REJECT_LIMIT:
            log_pipeline_end(log_id, label, bronze_table, silver_table,
                             "FAILED", "REPLACE_WHERE", rows_read, 0, rejected_count,
                             error_message=f"Rejection rate {rejected_count}/{rows_read} exceeds {REJECT_LIMIT}.",
                             start_time=start_time)
            return "FAILED"

        # days that actually produced valid rows (so we never delete a day we can't refill)
        valid_days = sorted({r["_batch_day"] for r in
                             df_valid.select("_batch_day").distinct().collect()})
        if not valid_days:
            status = "FAILED" if rejected_count else "SUCCESS"
            log_pipeline_end(log_id, label, bronze_table, silver_table,
                             status, "REPLACE_WHERE", rows_read, 0, rejected_count,
                             error_message="No valid rows to write." if rejected_count else None,
                             start_time=start_time)
            return status

        silver_field_names = [f.name for f in silver_schema.fields]
        df_silver = (df_valid
                     .withColumn("_silver_load_timestamp", F.current_timestamp())
                     .withColumn("_silver_job_id", F.lit(job_id_str)))
        if "_bronze_load_timestamp" in silver_field_names:
            src_ts = F.col("_load_timestamp") if "_load_timestamp" in df_silver.columns else F.lit(None).cast("timestamp")
            df_silver = df_silver.withColumn("_bronze_load_timestamp", src_ts)
        if "_batch_month" in silver_field_names:
            df_silver = df_silver.withColumn("_batch_month", F.col("_batch_day").substr(1, 6))
        df_silver = df_silver.select(*silver_field_names)

        # Empty target -> plain overwrite (fast initial load). Otherwise scoped
        # replaceWhere over the valid days only.
        silver_empty = spark.table(silver_table).limit(1).count() == 0
        writer = df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "false")
        if not silver_empty:
            try:
                part_cols = (spark.sql(f"DESCRIBE DETAIL {silver_table}")
                             .select("partitionColumns").collect()[0][0]) or []
            except Exception:
                part_cols = []
            day_in = "', '".join(valid_days)
            if "_batch_month" in part_cols:
                month_in = "', '".join(sorted({d[:6] for d in valid_days}))
                predicate = f"_batch_month IN ('{month_in}') AND _batch_day IN ('{day_in}')"
            else:
                predicate = f"_batch_day IN ('{day_in}')"
            writer = writer.option("replaceWhere", predicate)
        writer.saveAsTable(silver_table)

        status = "WARNING" if rejected_count else "SUCCESS"
        log_pipeline_end(log_id, label, bronze_table, silver_table,
                         status, "REPLACE_WHERE", rows_read, valid_count, rejected_count,
                         start_time=start_time)
        return status

    except Exception as e:
        import traceback; traceback.print_exc()
        log_pipeline_end(log_id, label, bronze_table, silver_table,
                         "FAILED", "ERROR", rows_read, 0, error_message=str(e), start_time=start_time)
        return "FAILED"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Orchestration
# ============================================================================
def distinct_days(table_fqn):
    """Sorted DISTINCT _batch_day present in a Delta table -- the coverage truth.
    (Read from the table itself, NOT from log_table_loads, whose bulk range
    markers can mask individually skipped days.)"""
    try:
        rows = (spark.table(table_fqn)
                .select("_batch_day").distinct()
                .where(F.col("_batch_day").isNotNull()).collect())
        return sorted(r["_batch_day"] for r in rows if r["_batch_day"])
    except Exception:
        return []

def day_watermarks(table_fqn, ts_col):
    """{ _batch_day : MAX(ts_col) } for a table. Used to detect days whose
    bronze data was re-ingested after silver last consumed them."""
    try:
        rows = (spark.table(table_fqn)
                .where(F.col("_batch_day").isNotNull())
                .groupBy("_batch_day").agg(F.max(ts_col).alias("ts")).collect())
        return {r["_batch_day"]: r["ts"] for r in rows}
    except Exception:
        return {}

def days_for_table(table_name):
    """Which days to promote for one table, per LOAD_MODE.
      single   : [batch_day]
      explicit : the requested days that actually exist in bronze
      bulk     : every bronze day in [DAY_FROM .. batch_day] (authoritative reload)
      catch_up : days new to silver OR whose bronze _load_timestamp is newer than
                 what silver recorded (re-ingested days), bounded by [DAY_FROM..batch_day]
    """
    bronze_table = f"{BRONZE_LH}.{table_name}"
    silver_table = f"{SILVER_LH}.{table_name}"

    if LOAD_MODE == "single":
        return [batch_day]

    if LOAD_MODE in ("explicit", "bulk"):
        bronze_days = distinct_days(bronze_table)
        if LOAD_MODE == "explicit":
            missing = [d for d in EXPLICIT_DAYS if d not in bronze_days]
            if missing:
                print(f"  ⚠ {table_name}: requested day(s) not in bronze (skipped): {missing}")
            return [d for d in EXPLICIT_DAYS if d in bronze_days]
        return [d for d in bronze_days
                if d <= batch_day and (not DAY_FROM or d >= DAY_FROM)]

    # catch_up
    def in_bounds(d):
        return d <= batch_day and (not DAY_FROM or d >= DAY_FROM)

    bronze_wm = day_watermarks(bronze_table, "_load_timestamp")
    silver_has_wm = (spark.catalog.tableExists(silver_table)
                     and "_bronze_load_timestamp" in spark.table(silver_table).columns)

    if silver_has_wm:
        silver_wm = day_watermarks(silver_table, "_bronze_load_timestamp")
        out = []
        for d in sorted(bronze_wm):
            if not in_bounds(d):
                continue
            sv, bv = silver_wm.get(d), bronze_wm.get(d)
            if sv is None or (bv is not None and bv > sv):   # new, or re-ingested
                out.append(d)
        return out

    # legacy silver without the watermark column -> day-level set difference
    silver_set = set(distinct_days(silver_table))
    return [d for d in sorted(bronze_wm) if in_bounds(d) and d not in silver_set]

def main():
    print("=" * 80)
    print("Bronze -> Silver  (typed, validated, replaceWhere)")
    print(f"source={SOURCE}  mode={LOAD_MODE}  upto={batch_day}  job_id={job_id_str}")
    if LOAD_MODE == "explicit":
        print(f"explicit days: {EXPLICIT_DAYS}")
    if LOAD_MODE == "catch_up" and DAY_FROM:
        print(f"from day:      {DAY_FROM}")
    print("=" * 80)

    ensure_rejection_table()

    bronze_tables = sorted(
        t.name for t in spark.catalog.listTables(BRONZE_LH)
        if not t.name.startswith("_") and t.name.lower().startswith(SOURCE_PFX)
    )
    silver_tables = {t.name for t in spark.catalog.listTables(SILVER_LH) if not t.name.startswith("_")}

    targets = [t for t in bronze_tables if t in silver_tables]
    if ONLY_TABLES:
        targets = [t for t in targets if t in ONLY_TABLES]

    missing_silver = [t for t in bronze_tables if t not in silver_tables]
    if missing_silver:
        print(f"⚠ bronze tables with no silver target (run ntb_create_silver_tables): {missing_silver}")
    if not targets:
        print("Nothing to process.")
        return

    # Build the per-table work plan (which days each table still needs).
    plan = {t: days_for_table(t) for t in targets}
    total_loads = sum(len(d) for d in plan.values())
    print(f"\nWork plan ({total_loads} table-day load(s) across {len(targets)} table(s)):")
    for t, days in plan.items():
        if days:
            print(f"    {t}: {days}")
    idle = [t for t, days in plan.items() if not days]
    if idle:
        print(f"    up to date (no missing days): {idle}")
    if total_loads == 0:
        print("\n✓ Silver already covers all bronze days in scope. Nothing to load.")
        return

    # One read + one write per table (filter bronze to the selected days, write
    # once). promote() scopes the write to the days that produced valid rows, so
    # this stays safe whether days is 1 or 1000.
    results = {}
    for t, days in plan.items():
        if not days:
            continue
        label = days[0] if len(days) == 1 else f"{days[0]}..{days[-1]}"
        results[(t, label)] = promote(t, days, label)

    print("\n" + "=" * 80 + "\nSummary (table-day loads)\n" + "=" * 80)
    for sym, label in [("✅", "SUCCESS"), ("⚠️", "WARNING"), ("❌", "FAILED")]:
        items = [f"{t}@{d}" for (t, d), s in results.items() if s == label]
        print(f"  {sym} {label}: {len(items)}  {items}")
    print(f"\nLogs:       SELECT * FROM {METADATA_LH}.log_table_loads WHERE job_id='{job_id_str}'")
    print(f"Rejections: SELECT * FROM {METADATA_LH}.rejection_log    WHERE job_id='{job_id_str}'")

    failed = [f"{t}@{d}" for (t, d), s in results.items() if s == "FAILED"]
    if failed:
        raise RuntimeError(f"Bronze->Silver failed for {len(failed)} load(s): {failed}")

main()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
