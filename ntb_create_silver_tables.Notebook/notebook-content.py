# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4c73f3f2-9c1c-45d3-b6be-3607c949dc6d",
# META       "default_lakehouse_name": "lh_metadata",
# META       "default_lakehouse_workspace_id": "d626fea3-3116-4af5-9cfb-9196dd43cba1",
# META       "known_lakehouses": [
# META         {
# META           "id": "a13e3d64-d65b-4196-a813-3475454ce68a"
# META         },
# META         {
# META           "id": "0abf8ee5-18ac-4c6f-85f2-d828a032ff48"
# META         },
# META         {
# META           "id": "4c73f3f2-9c1c-45d3-b6be-3607c949dc6d"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# ============================================================================
# Parameters cell  (tag this cell 'parameters' so the Fabric pipeline can override)
# ============================================================================
# Runs ONCE per day from Pipe_Main (source_system='ALL'), BEFORE the per-source
# fan-out. It is the single writer of lh_metadata.table_config,
# table_type_config and metadata_table_column_setup.
#
# source_system : 'ALL' (pipeline default) or one of CNM, CUD, EDESK, G2G, IAM, LS, METAIS, SM
#                 (single source = manual / debugging runs only)
source_system     = 'ALL'
mapping_filename  = 'DWH_Systemy_IF_1.5.xlsx'

# reload_metadata   : re-read the Excel into lh_metadata.table_config / table_type_config
#   'auto'  -> reload ONLY when the Excel in Files/Mapping changed since the last
#              load (filename or OneLake modifyTime differs from the stamp in
#              table_config). Safe as a daily default: one fs.ls + one 1-row read.
#   'true'  -> always reload   'false' -> never reload
reload_metadata   = 'auto'
# rebuild_gold_metadata : force re-derivation of lh_metadata.metadata_table_column_setup
#   (it is rebuilt automatically whenever the Excel was reloaded or the table is missing)
rebuild_gold_metadata = 'false'
# run_consistency   : run the bronze <-> mapping column-coverage check
run_consistency   = 'true'
# generate_tables   : (re)generate the silver DDL and create missing silver tables
generate_tables   = 'true'
# drop_and_recreate : DROP each target silver table before CREATE  -- DESTRUCTIVE
#   Refused when source_system='ALL' (would drop every silver table on the platform).
#   Manual, single-source notebook runs only. Do NOT expose as a Pipe_Main parameter.
drop_and_recreate = 'false'
# block_on_unmapped : mark BRONZE_COLUMN_NOT_IN_MAPPING as BLOCKER (vs WARNING)
block_on_unmapped = 'false'
# silver_layout   : 'cluster' (liquid clustering - RECOMMENDED), 'none', 'month', 'day'
silver_layout     = 'cluster'
# cluster_columns : comma-separated CLUSTER BY keys (used when silver_layout='cluster')
cluster_columns   = '_batch_day'

job_id            = ''   # optional; auto-generated when empty
pipeline_name     = 'ntb_create_silver_tables'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Imports, constants, helpers
# ============================================================================
import re
import uuid
import unicodedata
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, BooleanType
)

# --- runtime flags (pipeline passes everything as strings) ------------------
job_id_str        = job_id or datetime.now().strftime("%Y%m%d_%H%M%S")
RELOAD_MODE       = str(reload_metadata).strip().lower()
RELOAD_MODE       = {"1": "true", "yes": "true", "0": "false", "no": "false"}.get(RELOAD_MODE, RELOAD_MODE)
if RELOAD_MODE not in ("auto", "true", "false"):
    raise ValueError(f"reload_metadata must be auto|true|false, got {reload_metadata!r}")
REBUILD_GOLD_META = str(rebuild_gold_metadata).strip().lower() in ("true", "1", "yes")
RUN_CONSISTENCY   = str(run_consistency).strip().lower()   in ("true", "1", "yes")
GENERATE_TABLES   = str(generate_tables).strip().lower()   in ("true", "1", "yes")
DROP_AND_RECREATE = str(drop_and_recreate).strip().lower() in ("true", "1", "yes")
BLOCK_ON_UNMAPPED = str(block_on_unmapped).strip().lower() in ("true", "1", "yes")

KNOWN_SOURCES = ["CNM", "CUD", "EDESK", "G2G", "IAM", "LS", "METAIS", "SM"]
#KNOWN_SOURCES = ["CNM", "CUD", "IAM", "METAIS", "SM"]
SRC           = source_system.strip().upper()
SOURCES       = KNOWN_SOURCES if SRC in ("", "ALL") else [SRC]

if DROP_AND_RECREATE and len(SOURCES) > 1:
    raise ValueError("drop_and_recreate='true' is refused for source_system='ALL' - "
                     "it would drop every silver table. Run it per source, manually.")

BRONZE_LH   = "lh_bronze"
SILVER_LH   = "lh_silver"
METADATA_LH = "lh_metadata"

# Bronze lineage columns (excluded from the mapping coverage check).
BRONZE_META = {"_source_file", "_batch_day", "_row_hash", "_load_timestamp", "_job_id"}

# Silver lineage columns appended to every generated silver table.
SILVER_META_DDL = [
    ("_row_hash",              "STRING"),
    ("_source_file",           "STRING"),
    ("_bronze_load_timestamp", "TIMESTAMP"),
    ("_silver_load_timestamp", "TIMESTAMP"),
    ("_silver_job_id",         "STRING"),
    ("_batch_day",             "STRING"),
]
# Physical layout for generated silver tables:
#   'cluster' -> liquid clustering: CLUSTER BY (cluster_columns). RECOMMENDED.
#                No directories, no small-file blow-up from daily loads; pruning
#                on the clustering keys is applied during OPTIMIZE (schedule it).
#                Eligible key types: numeric / date / timestamp / string.
#                NOT eligible: boolean, null. Confirm Fabric Runtime 2.0 for
#                cheap incremental clustering (1.3 rewrites whole table on OPTIMIZE).
#   'none'    -> no physical layout; replaceWhere on _batch_day still works.
#   'month'   -> PARTITIONED BY (_batch_month): ~12 partitions/year.
#   'day'     -> PARTITIONED BY (_batch_day): only for genuinely big tables.
SILVER_LAYOUT = str(silver_layout).strip().lower()
if SILVER_LAYOUT not in ("cluster", "none", "day", "month"):
    SILVER_LAYOUT = "cluster"
CLUSTER_COLS = [c.strip() for c in str(cluster_columns).split(",") if c.strip()] or ["_batch_day"]

print(f"job_id_str        : {job_id_str}")
print(f"sources in scope  : {SOURCES}")
print(f"reload_metadata   : {RELOAD_MODE}")
print(f"rebuild_gold_meta : {REBUILD_GOLD_META}")
print(f"run_consistency   : {RUN_CONSISTENCY}")
print(f"generate_tables   : {GENERATE_TABLES}")
print(f"drop_and_recreate : {DROP_AND_RECREATE}")

# --- mapping file location (via Variable Library) ---------------------------
vl = notebookutils.variableLibrary.getLibrary("variable_library")
WorkspaceID         = vl.getVariable("WorkspaceID")
MetadataLakehouseID = vl.getVariable("MetadataLakehouseID")
mapping_path = (
    f"abfss://{WorkspaceID}@onelake.dfs.fabric.microsoft.com/"
    f"{MetadataLakehouseID}/Files/Mapping/{mapping_filename}"
)

# --- type vocabulary: mapping 'data_type_final' -> Spark SQL DDL type -------
TYPE_MAP = {
    "string": "STRING", "str": "STRING", "varchar": "STRING", "text": "STRING", "nvarchar": "STRING",
    "int": "INT", "integer": "INT", "smallint": "INT",
    "bigint": "BIGINT", "long": "BIGINT",
    "boolean": "BOOLEAN", "bool": "BOOLEAN",
    "timestamp": "TIMESTAMP", "datetime": "TIMESTAMP", "datetime2": "TIMESTAMP",
    "date": "DATE",
    "double": "DOUBLE", "float": "DOUBLE", "real": "DOUBLE",
    "decimal": "DECIMAL(38,18)", "numeric": "DECIMAL(38,18)",
}

def to_ddl_type(raw):
    """Normalize a mapping data type to a Spark DDL type, or None if unknown."""
    if raw is None:
        return None
    key = str(raw).strip().lower()
    if key in ("", "none", "nan"):
        return None
    return TYPE_MAP.get(key)

def normalize_col(name):
    """ASCII-fold + snake_case a spreadsheet header (matches the original logic)."""
    name = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    name = re.sub(r"[ ,;{}()\n\t=]", "_", name)
    return name.lower()

def source_of(table_name):
    """Derive the source system from a '<source>_<table>' bronze/mapping name."""
    head = str(table_name).split("_", 1)[0].upper()
    return head if head in KNOWN_SOURCES else None

def in_scope(table_name):
    return source_of(table_name) in SOURCES


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# (Re)load the structure-mapping Excel into lh_metadata
#   Atribúty -> lh_metadata.table_config        (column-level contract)
#   Tabuľky  -> lh_metadata.table_type_config   (table-level DIM/FACT + gold name)
# Adds:  system_norm (TRIM+UPPER), _ordinal (column order), load lineage, and the
#        Excel's (filename, OneLake modifyTime) stamp used by reload_metadata='auto'.
#
# Change detection compares epoch-ms with epoch-ms (file modifyTime vs the stamped
# _mapping_file_mtime) - NOT _loaded_at vs modifyTime, which mixes a session-local
# timestamp with a UTC one and silently mis-fires.
# ============================================================================
MAPPING_DIR = mapping_path.rsplit("/", 1)[0]
TABLE_CONFIG = f"{METADATA_LH}.table_config"


def _mapping_file_info():
    """{name, mtime(epoch ms), size} of the mapping Excel, or raise if missing."""
    for f in notebookutils.fs.ls(MAPPING_DIR):
        if f.name == mapping_filename:
            if int(f.size) == 0:
                raise ValueError(f"{mapping_path} is 0 bytes - upload still in progress?")
            return {"name": f.name, "mtime": int(f.modifyTime), "size": int(f.size)}
    raise FileNotFoundError(f"{mapping_path} not found under {MAPPING_DIR}")


def _last_loaded():
    """Stamp on the current table_config: (file, mtime, loaded_at, job) or None."""
    if not spark.catalog.tableExists(TABLE_CONFIG):
        return None
    t = spark.table(TABLE_CONFIG)
    if "_mapping_file_mtime" not in t.columns:
        return None                           # written by an older notebook version
    r = t.select("_mapping_file", "_mapping_file_mtime", "_loaded_at", "_load_job_id").first()
    return (r[0], int(r[1]), r[2], r[3]) if r else None


def _load_sheet(sheet_name, target_table, file_info):
    pdf = pd.read_excel(mapping_path, sheet_name=sheet_name)
    pdf = pdf.reset_index(drop=True)
    pdf["_ordinal"] = pdf.index + 1          # preserve spreadsheet row order
    df = spark.createDataFrame(pdf)
    for c in df.columns:
        df = df.withColumnRenamed(c, normalize_col(c))

    # normalized, reliable scoping key (raw 'system' has trailing spaces / mixed case)
    df = df.withColumn("system_norm", F.upper(F.trim(F.col("system"))))
    df = (df
          .withColumn("_mapping_file",       F.lit(file_info["name"]))
          .withColumn("_mapping_file_mtime", F.lit(file_info["mtime"]).cast("long"))   # epoch ms, UTC
          .withColumn("_mapping_file_modified",
                      F.to_timestamp(F.from_unixtime(F.lit(file_info["mtime"] // 1000))))
          .withColumn("_loaded_at",          F.current_timestamp())
          .withColumn("_load_job_id",        F.lit(job_id_str)))

    (df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{METADATA_LH}.{target_table}"))
    print(f"  ✓ {METADATA_LH}.{target_table}: {df.count()} rows")
    return df


def _reload_mapping(file_info):
    """Overwrite both config tables. If a parallel instance of this notebook already
       loaded the SAME file, swallow the Delta conflict and continue - the outcome
       is identical. Any other error propagates."""
    try:
        _load_sheet("Atribúty", "table_config",      file_info)
        _load_sheet("Tabuľky",  "table_type_config", file_info)
        return True
    except Exception as e:
        if "Concurrent" in type(e).__name__ or "DELTA_CONCURRENT" in str(e):
            now = _last_loaded()
            if now and now[0] == file_info["name"] and now[1] == file_info["mtime"]:
                print(f"  ~ concurrent instance (job {now[3]}) already loaded this file - continuing")
                return True
        raise


file_info = _mapping_file_info()
last      = _last_loaded()
print(f"mapping file : {file_info['name']}  modified={datetime.utcfromtimestamp(file_info['mtime']/1000):%Y-%m-%d %H:%M:%S}Z  {file_info['size']:,} B")
if last:
    print(f"last loaded  : {last[0]}  modified={datetime.utcfromtimestamp(last[1]/1000):%Y-%m-%d %H:%M:%S}Z  at {last[2]}  job={last[3]}")
else:
    print("last loaded  : (none / no stamp)")

if RELOAD_MODE == "true":
    do_reload, why = True, "reload_metadata='true'"
elif RELOAD_MODE == "false":
    do_reload, why = False, "reload_metadata='false'"
elif last is None:
    do_reload, why = True, "no previous load stamp"
elif last[0] != file_info["name"]:
    do_reload, why = True, f"mapping file changed: {last[0]} -> {file_info['name']}"
elif last[1] != file_info["mtime"]:
    do_reload, why = True, "mapping file was modified since the last load"
else:
    do_reload, why = False, "mapping file unchanged since the last load"

METADATA_RELOADED = False
if do_reload:
    print(f"Reloading mapping metadata from Excel ({why}) ...")
    METADATA_RELOADED = _reload_mapping(file_info)
else:
    print(f"Skipping metadata reload ({why}).")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Derive lh_metadata.metadata_table_column_setup (gold column contract)
# Joins table_config + table_type_config, derives load_type, ordinal from _ordinal.
#
# This is the ONLY writer of that table. It is rebuilt when the Excel was just
# reloaded, when the table is missing, or on rebuild_gold_metadata='true'.
# ntb_silver_2_gold* must READ it, never rebuild it: seven parallel gold
# instances overwriting one table = ConcurrentAppendException.
# ============================================================================
META_SETUP_TABLE  = f"{METADATA_LH}.metadata_table_column_setup"
LOAD_TYPE_DEFAULT = "delta"

def build_gold_column_metadata():
    ttc_cols = set(spark.table(f"{METADATA_LH}.table_type_config").columns)
    if "load_type" in ttc_cols:
        lt_expr = "lower(trim(tt.load_type))"
    elif "stereotyp" in ttc_cols:
        lt_expr = ("CASE WHEN lower(trim(tt.stereotyp)) LIKE '%full%' "
                   "OR lower(trim(tt.stereotyp)) LIKE '%snapshot%' THEN 'full' ELSE 'delta' END")
    else:
        lt_expr = f"'{LOAD_TYPE_DEFAULT}'"

    return spark.sql(f"""
        SELECT
            tt.typ                  AS table_type,
            tt.nazov_tabulky_gold   AS gold_table_name,
            {lt_expr}               AS load_type,
            tc.nazov_tabulky        AS silver_table_name,
            tc.atribut              AS silver_column_name,
            tc.gold_name            AS gold_column_name,
            tc.data_type_final      AS gold_column_data_type,
            CASE WHEN lower(trim(tc.povinny)) = 'ano' THEN FALSE ELSE TRUE END AS gold_column_is_nullable,
            CASE WHEN upper(trim(tc.`pk?`))   = 'PK'  THEN TRUE  ELSE FALSE END AS gold_column_is_pk,
            tc.`pk?`                AS pk_flag,
            tc._ordinal             AS ordinal_position,
            tc._mapping_file        AS _mapping_file,
            tc._mapping_file_mtime  AS _mapping_file_mtime,
            current_timestamp()     AS _derived_at,
            '{job_id_str}'          AS _derive_job_id
        FROM {METADATA_LH}.table_config tc
        LEFT JOIN {METADATA_LH}.table_type_config tt
            ON tt.system_norm = tc.system_norm
           AND upper(trim(tc.nazov_tabulky)) = upper(trim(tt.nazov_tabulky_bronze))
        WHERE tc.nazov_tabulky IS NOT NULL
          AND lower(trim(tc.nazov_tabulky)) <> 'nan'
        ORDER BY silver_table_name, ordinal_position
    """)

setup_exists = spark.catalog.tableExists(META_SETUP_TABLE)
if METADATA_RELOADED or REBUILD_GOLD_META or not setup_exists:
    why = ("mapping reloaded" if METADATA_RELOADED else
           "rebuild_gold_metadata='true'" if REBUILD_GOLD_META else "table missing")
    df_meta = build_gold_column_metadata()
    (df_meta.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(META_SETUP_TABLE))
    print(f"  ✓ {META_SETUP_TABLE}: {df_meta.count()} rows ({why})")
else:
    print(f"  = {META_SETUP_TABLE} up to date (mapping unchanged)")


# CELL ********************

# ============================================================================
# Inventories used by both the consistency check and the table generator
# ============================================================================

INVENTORY_WORKERS = 4   # schema reads are metastore round-trips; ALL = ~100 tables

def _bronze_schema(name):
    try:
        return name, [(f.name, f.dataType.simpleString())
                      for f in spark.table(f"{BRONZE_LH}.{name}").schema.fields
                      if f.name not in BRONZE_META], None
    except Exception as e:
        return name, [], str(e)

def bronze_inventory(sources):
    """[(table_name, column_name, bronze_type)] for in-scope bronze tables (payload cols only)."""
    names = [t.name for t in spark.catalog.listTables(BRONZE_LH)
             if not t.name.startswith("_") and in_scope(t.name)]
    rows = []
    with ThreadPoolExecutor(max_workers=INVENTORY_WORKERS) as ex:
        for name, cols, err in ex.map(_bronze_schema, names):
            if err:
                print(f"  ⚠ cannot read schema of {name}: {err}")
            rows.extend((name, c, ct) for c, ct in cols)
    return rows

def mapping_inventory(sources):
    """
    [(table_name, column_name, data_type_final, ddl_type, is_required, ordinal)]
    for in-scope mapping rows, scoped by the '<source>_' table-name prefix.
    """
    tc = spark.table(f"{METADATA_LH}.table_config")
    pdf = (tc.select(
                F.col("nazov_tabulky").alias("table_name"),
                F.col("atribut").alias("column_name"),
                F.col("data_type_final").alias("data_type_final"),
                F.lower(F.trim(F.coalesce(F.col("povinny"), F.lit("")))).alias("povinny"),
                F.col("_ordinal").alias("ordinal"))
             .toPandas())
    out = []
    for _, r in pdf.iterrows():
        tbl = str(r["table_name"]).strip()
        coln = str(r["column_name"]).strip()
        if not tbl or tbl.lower() == "nan" or not in_scope(tbl):
            continue
        # Skip reserved metadata names: silver manages these itself (see
        # SILVER_META_DDL); a mapping row for one would duplicate the column
        # and pollute the consistency check.
        if coln.lower() in BRONZE_META:
            continue
        out.append((
            tbl, coln, r["data_type_final"], to_ddl_type(r["data_type_final"]),
            r["povinny"] == "ano", int(r["ordinal"]),
        ))
    return out


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Consistency check: do bronze columns exist in the mapping, and vice-versa?
# Writes findings to lh_metadata.schema_consistency_log (notification source).
#
# issue_type values:
#   BRONZE_COLUMN_NOT_IN_MAPPING  bronze has a column the mapping doesn't define
#                                 (would be silently dropped in silver)  <-- the ask
#   MAPPING_COLUMN_NOT_IN_BRONZE  mapping defines a column bronze hasn't delivered
#                                 (silver column will be all-NULL)
#   BRONZE_TABLE_NOT_IN_MAPPING   whole bronze table is unmapped (no silver target)
#   MAPPING_TABLE_NOT_IN_BRONZE   mapped table has no bronze data yet
#   INVALID_TARGET_TYPE           data_type_final cannot be mapped to a Spark type
# ============================================================================
CONSISTENCY_SCHEMA = StructType([
    StructField("run_id",          StringType(),    False),
    StructField("check_timestamp", TimestampType(), False),
    StructField("source_system",   StringType(),    True),
    StructField("issue_type",      StringType(),    False),
    StructField("severity",        StringType(),    False),   # BLOCKER | WARNING | INFO
    StructField("table_name",      StringType(),    True),
    StructField("column_name",     StringType(),    True),
    StructField("bronze_type",     StringType(),    True),
    StructField("mapping_type",    StringType(),    True),
    StructField("details",         StringType(),    True),
    StructField("is_resolved",     BooleanType(),   False),
    StructField("pipeline_name",   StringType(),    True),
])

def run_consistency_check():
    run_id = job_id_str
    now    = datetime.now()
    findings = []

    def add(issue, severity, table, column, btype, mtype, details):
        findings.append((
            run_id, now, source_of(table) if table else None,
            issue, severity, table, column, btype, mtype, details,
            False, pipeline_name,
        ))

    bronze = bronze_inventory(SOURCES)
    mapping = mapping_inventory(SOURCES)

    # case-insensitive lookups, but keep original names for reporting
    bronze_cols = {(t.lower(), c.lower()): (t, c, bt) for t, c, bt in bronze}
    map_cols    = {(t.lower(), c.lower()): (t, c, dtf, ddl, req, o)
                   for t, c, dtf, ddl, req, o in mapping}
    bronze_tabs = {t.lower(): t for t, _, _ in bronze}
    map_tabs    = {t.lower(): t for t, _, _, _, _, _ in mapping}

    unmapped_sev = "BLOCKER" if BLOCK_ON_UNMAPPED else "WARNING"

    # 1. bronze columns missing from mapping  (the primary requirement)
    for key, (t, c, bt) in bronze_cols.items():
        if key not in map_cols:
            add("BRONZE_COLUMN_NOT_IN_MAPPING", unmapped_sev, t, c, bt, None,
                "Bronze column has no row in table_config; it will NOT be carried to silver.")

    # 2. mapping columns missing from bronze
    for key, (t, c, dtf, ddl, req, o) in map_cols.items():
        if key not in bronze_cols and t.lower() in bronze_tabs:
            sev = "WARNING" if req else "INFO"
            add("MAPPING_COLUMN_NOT_IN_BRONZE", sev, t, c, None, str(dtf),
                "Mapped column not present in bronze; silver value will be NULL"
                + (" and violates NOT NULL." if req else "."))
        # 5. invalid / unknown target type
        if ddl is None:
            add("INVALID_TARGET_TYPE", "BLOCKER", t, c, None, str(dtf),
                "data_type_final cannot be mapped to a Spark type; silver DDL is blocked.")

    # 3. bronze tables with no mapping at all
    for tl, t in bronze_tabs.items():
        if tl not in map_tabs:
            add("BRONZE_TABLE_NOT_IN_MAPPING", "BLOCKER", t, None, None, None,
                "Bronze table is absent from the mapping; no silver target can be generated.")

    # 4. mapped tables with no bronze data yet
    for tl, t in map_tabs.items():
        if tl not in bronze_tabs:
            add("MAPPING_TABLE_NOT_IN_BRONZE", "INFO", t, None, None, None,
                "Mapped table has no bronze table yet (ingestion not started?).")

    df = spark.createDataFrame(findings, CONSISTENCY_SCHEMA)
    (df.write.format("delta").mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{METADATA_LH}.schema_consistency_log"))

    n_block = df.filter(F.col("severity") == "BLOCKER").count()
    n_warn  = df.filter(F.col("severity") == "WARNING").count()
    n_info  = df.filter(F.col("severity") == "INFO").count()
    print(f"  Consistency findings: {df.count()} "
          f"(BLOCKER={n_block}, WARNING={n_warn}, INFO={n_info})  run_id={run_id}")
    if df.count():
        display(df.orderBy("severity", "table_name", "column_name"))
    return df

consistency_df = run_consistency_check() if RUN_CONSISTENCY else None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Generate silver tables from the mapping (mapping is the silver contract)
# - column order from _ordinal
# - NOT NULL where povinný = 'ano'
# - silver metadata columns appended; partitioned by _batch_day
# - tables containing an INVALID_TARGET_TYPE column are skipped (logged above)
# ============================================================================
def build_silver_ddls():
    mapping = mapping_inventory(SOURCES)
    by_table = {}
    for t, c, dtf, ddl, req, o in mapping:
        by_table.setdefault(t, []).append((o, c, dtf, ddl, req))

    ddls, skipped = {}, {}
    for table, cols in by_table.items():
        cols.sort(key=lambda x: x[0])
        bad = [c for (_, c, dtf, ddl, _) in cols if ddl is None]
        if bad:
            skipped[table] = f"unmapped type on column(s): {bad}"
            continue

        defs = []  # (name, ddl_fragment)
        for _, c, dtf, ddl, req in cols:
            defs.append((c, f"`{c}` {ddl}" + (" NOT NULL" if req else "")))
        meta = list(SILVER_META_DDL)
        if SILVER_LAYOUT == "month":
            meta.append(("_batch_month", "STRING"))
        for mc, mt in meta:
            defs.append((mc, f"`{mc}` {mt}"))

        # Liquid clustering: keys must sit within the first 32 columns, because
        # Delta only collects data-skipping stats on the first 32 by default and
        # clustering requires stats on its keys. Hoist the keys to the front so
        # this holds for arbitrarily wide tables (DELTA_CLUSTERING_COLUMN_MISSING_STATS).
        if SILVER_LAYOUT == "cluster":
            names = [n for n, _ in defs]
            missing_keys = [k for k in CLUSTER_COLS if k not in names]
            if missing_keys:
                skipped[table] = f"cluster key(s) not found in columns: {missing_keys}"
                continue
            lead = sorted((d for d in defs if d[0] in CLUSTER_COLS),
                          key=lambda d: CLUSTER_COLS.index(d[0]))
            rest = [d for d in defs if d[0] not in CLUSTER_COLS]
            defs = lead + rest

        col_defs = [f"  {frag}" for _, frag in defs]

        if SILVER_LAYOUT == "cluster":
            clause = "\nCLUSTER BY (" + ", ".join(f"`{c}`" for c in CLUSTER_COLS) + ")"
        elif SILVER_LAYOUT == "day":
            clause = "\nPARTITIONED BY (`_batch_day`)"
        elif SILVER_LAYOUT == "month":
            clause = "\nPARTITIONED BY (`_batch_month`)"
        else:
            clause = ""

        ddl = (
            f"CREATE TABLE {SILVER_LH}.{table} (\n"
            + ",\n".join(col_defs)
            + f"\n)\nUSING DELTA{clause}"
        )
        ddls[table] = ddl
    return ddls, skipped

def silver_exists(table):
    return spark.catalog.tableExists(f"{SILVER_LH}.{table}")

def generate_silver_tables():
    ddls, skipped = build_silver_ddls()
    created, recreated, existing = [], [], []

    for table, ddl in sorted(ddls.items()):
        if DROP_AND_RECREATE:
            spark.sql(f"DROP TABLE IF EXISTS {SILVER_LH}.{table}")

        if silver_exists(table) and not DROP_AND_RECREATE:
            existing.append(table)
            continue

        try:
            spark.sql(ddl)
            (recreated if DROP_AND_RECREATE else created).append(table)
            print(f"  ✓ {'recreated' if DROP_AND_RECREATE else 'created'} {SILVER_LH}.{table}")
        except Exception as e:
            print(f"  ✗ failed {SILVER_LH}.{table}: {e}")

    print("\n" + "=" * 70)
    print("Silver generation summary")
    print("=" * 70)
    print(f"  created   : {len(created)}    {created}")
    if DROP_AND_RECREATE:
        print(f"  recreated : {len(recreated)}    {recreated}")
    print(f"  existing  : {len(existing)} (left untouched)")
    print(f"  skipped   : {len(skipped)} (type issues -> see schema_consistency_log)")
    for t, why in skipped.items():
        print(f"      - {t}: {why}")
    return ddls, skipped

if GENERATE_TABLES:
    ddl_map, skipped_map = generate_silver_tables()
else:
    print("Skipping silver table generation (generate_tables='false').")
    ddl_map, skipped_map = build_silver_ddls()

# Inspect the generated DDL for any single table, e.g.:
#   print(ddl_map['cnm_notification'])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
