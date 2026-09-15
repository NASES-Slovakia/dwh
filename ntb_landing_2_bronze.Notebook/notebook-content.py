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
# META           "id": "4c73f3f2-9c1c-45d3-b6be-3607c949dc6d"
# META         },
# META         {
# META           "id": "a13e3d64-d65b-4196-a813-3475454ce68a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================================
# Parameters cell  (tagged 'parameters' so Fabric pipeline can override)
# ============================================================================
job_id          = '20260714060023'
source_system   = 'G2G'
batch_day       = '20260713'
pipeline_name   = 'ntb_landing_2_bronze'

# ============================================================================
# BRONZE CONTRACT: APPEND-ONLY / IMMUTABLE
#   A (table, batch_day) is written exactly once and never rewritten.
#   Already-loaded days are skipped before any I/O. The only overwrite in the
#   notebook is the initial CREATE of a table (or an explicit full overwrite).
#   Consequence: the FIRST complete delivery for a day wins. A later NiFi
#   re-delivery of the same day is ignored unless force_reload='true'.
# ============================================================================

# --- Run mode (exactly one applies; precedence: debug > catch_up > single) ---
#
#   debug_mode='true'    -> BULK. Ignore `batch_day`; discover every distinct
#                           batch_day in landing and load all of them per table
#                           in ONE Spark job. Initial / historical load.
#   catch_up_mode='true' -> GAP-FILL. Load every available-but-unloaded day up
#                           to and including `batch_day`. Self-heals missed runs.
#   both 'false'         -> SINGLE DAY. Load exactly `batch_day`.
#
debug_mode      = 'false'
catch_up_mode   = 'true'

# --- Bulk chunking (debug_mode only) ----------------------------------------
# Inclusive YYYYMMDD bounds to split a large initial load ('' = no bound).
# e.g. one year at a time: debug_day_from='20200101', debug_day_to='20201231'
debug_day_from  = ''
debug_day_to    = ''

# Truncate + reload the WHOLE table in one atomic overwrite (debug_mode only).
# Ignored when a day range is set - a full overwrite would wipe earlier chunks.
# This is the ONE sanctioned way to destroy bronze history. Use deliberately.
debug_full_overwrite = 'false'

# --- Reloading an already-loaded day ----------------------------------------
# Bronze is immutable, so a loaded day is normally skipped. To REWRITE one day
# (bad export frozen in place, corrected NiFi re-delivery, etc.):
#     debug_mode='false', catch_up_mode='false',
#     batch_day='YYYYMMDD', force_reload='true'
# Scoped replaceWhere on that single day; all other days untouched.
# Ignored by catch-up and bulk - a gap-fill must never mutate history.
force_reload    = 'false'

# --- Blocked days and the pipeline verdict ---------------------------------
# A batch_day whose export is unusable (missing part, ZERO-BYTE file, corrupt
# Avro) is BLOCKED: it is never loaded partially, it is logged as FAILED in
# log_table_loads, and every OTHER day of the table still loads. Since the
# blocked day stays in landing, every subsequent run will re-block it until
# NiFi re-delivers a complete GUID for that day or the bad files are removed.
#   'true'  -> after loading every good day, raise so the activity is FAILED
#              and the gap is visible in the pipeline (strict).
#   'false' -> activity SUCCEEDS; the gap is visible only in log_table_loads /
#              the status report (lenient - fine for full-load sources).
raise_on_partial = 'true'

# --- Parallelism ------------------------------------------------------------
# Threads for driver-side work: OneLake file listing and the per-TABLE loop.
# Tables are independent Delta tables, so concurrent writes are safe.
# Days WITHIN one table are never parallelised - they go in ONE Spark job.
# 1 = fully sequential (use when debugging).
max_workers     = '1'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Imports & runtime constants
# ============================================================================
import os
import re
import uuid
import threading
import traceback
from datetime import datetime
from collections import defaultdict
from functools import reduce
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name,
    sha2, concat_ws, coalesce, lit, regexp_extract, count as f_count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType
)
from delta.tables import DeltaTable

# Fresh runtime job_id (the parameter is only a fallback default)
#job_id_str = datetime.now().strftime("%Y%m%d_%H%M%S")
job_id_str = str(job_id).strip() or datetime.now().strftime("%Y%m%d%H%M%S")
job_id_col = lit(job_id_str)

# Pipeline metadata
source_layer = "landing"
target_layer = "bronze"

# Layout: lh_bronze/Files/<SOURCE_SYSTEM>/<table_name>/<files...>.avro
source_base_path = f"Files/{source_system}"
lakehouse_name   = "lh_bronze"
folder_name      = os.path.basename(source_base_path).lower()

# ---------------------------------------------------------------------------
# Flag parsing (the pipeline passes everything as strings)
# ---------------------------------------------------------------------------
def _flag(v):
    return str(v).strip().lower() in ("true", "1", "yes")

DEBUG_MODE           = _flag(debug_mode)
CATCH_UP_MODE        = _flag(catch_up_mode)
FORCE_RELOAD         = _flag(force_reload)
DEBUG_FULL_OVERWRITE = _flag(debug_full_overwrite)
DAY_FROM             = (debug_day_from or "").strip()
DAY_TO               = (debug_day_to   or "").strip()
MAX_WORKERS          = max(1, int(str(max_workers).strip() or "1"))

# force_reload is honoured ONLY on the explicit single-day path. A gap-fill or a
# bulk run must never be able to mutate an already-loaded day.
if FORCE_RELOAD and (DEBUG_MODE or CATCH_UP_MODE):
    print("!! force_reload is ignored in bulk / catch-up mode - disabling it.")
    FORCE_RELOAD = False

# A full overwrite of a *chunked* run would wipe the other chunks.
FULL_OVERWRITE = DEBUG_MODE and DEBUG_FULL_OVERWRITE and not (DAY_FROM or DAY_TO)

if DEBUG_MODE:
    RUN_MODE = "BULK"
elif CATCH_UP_MODE:
    RUN_MODE = "CATCH_UP"
else:
    RUN_MODE = "SINGLE_DAY"

# A blocked batch_day is SKIPPED (never loaded partially). If True the notebook
# also raises at the END (after loading every good day) so the pipeline activity
# is marked failed and the gap is visible. See the parameters cell.
RAISE_ON_PARTIAL = _flag(raise_on_partial)

# Land bronze with EVERY column as STRING. NiFi/Avro source schemas drift in TYPE
# over time (a boolean column later emitted as string); Avro mergeSchema cannot
# reconcile a type change and Delta append would reject it. Casting all payload
# columns to string makes bronze a faithful, drift-proof landing copy - typing is
# applied in silver.
BRONZE_ALL_STRING = True

print("=" * 80)
print(f"source_system   : {source_system}")
print(f"run mode        : {RUN_MODE}")
print(f"batch_day       : {batch_day}")
print(f"job_id          : {job_id_str}")
print(f"source path     : {source_base_path}")
print(f"max_workers     : {MAX_WORKERS}")
print(f"raise_on_partial: {RAISE_ON_PARTIAL}")
if DAY_FROM or DAY_TO:
    print(f"day range       : {DAY_FROM or '-inf'} .. {DAY_TO or '+inf'}")
if FORCE_RELOAD:
    print(f"!! FORCE_RELOAD  : {batch_day} WILL BE REWRITTEN")
if FULL_OVERWRITE:
    print("!! FULL_OVERWRITE: every bronze table for this source WILL BE TRUNCATED")
print("=" * 80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Pipeline logging  (buffered: rows accumulate in memory, ONE batched append)
# ============================================================================
# Under a thread pool, an append-per-log-row means dozens of tiny Delta commits
# and retry churn on a shared table. Buffer instead: `log_pipeline_start` is
# in-memory only, exactly one terminal row is emitted per (table, day), and the
# whole buffer is written once at the end of the run.
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

_LOG_BUFFER = []
_LOG_LOCK   = threading.Lock()


def log_pipeline_start():
    """In-memory only. Returns (log_id, start_time). Writes nothing."""
    return str(uuid.uuid4()), datetime.now()


def log_pipeline_end(log_id, batch_day_val, source_table, target_table,
                     status, operation, rows_read, rows_written,
                     rows_rejected=0, error_message=None, start_time=None):
    """Buffer exactly one terminal row for this (table, day)."""
    if start_time is None:
        start_time = datetime.now()
    now = datetime.now()
    row = (
        log_id, batch_day_val, pipeline_name, source_layer, target_layer,
        source_table, target_table, start_time, now, status, operation,
        int(rows_read or 0), int(rows_written or 0), int(rows_rejected or 0),
        (error_message[:500] if error_message else None), job_id_str, now,
    )
    with _LOG_LOCK:
        _LOG_BUFFER.append(row)


def flush_log():
    """Write the whole buffer to lh_metadata.log_table_loads in ONE append."""
    with _LOG_LOCK:
        rows = list(_LOG_BUFFER)
        _LOG_BUFFER.clear()
    if not rows:
        print("\n(no log rows to flush)")
        return
    (spark.createDataFrame(rows, LOG_SCHEMA)
        .write.format("delta").mode("append")
        .saveAsTable("lh_metadata.log_table_loads"))
    print(f"\n[log] flushed {len(rows)} row(s) to lh_metadata.log_table_loads")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# File selection - parse NiFi filenames, pick the right files per batch_day
# ============================================================================
# Filename pattern produced by NiFi:
#   <table>_<YYYYMMDD>_<uuid_with_underscores>[_part_N_of_M].avro
#
# Selection rules (per table folder, per batch_day):
#   1. Parse all *.avro files; ignore names that don't match the pattern.
#   2. Group files by guid (a guid groups all parts of one export).
#   3. Validate each guid's part set:
#        - NO zero-byte file (a 0 B .avro has no header -> Spark aborts the
#          whole read with "Not an Avro data file"; it is an in-flight or
#          failed NiFi transfer, never a legitimately empty export - an empty
#          Avro export still carries a ~200 B header)
#        - all-or-nothing on _part_N_of_M (no mixing)
#        - all parts agree on the same total M
#        - the set of N values is exactly {1..M} - no gaps, dupes, extras
#   4. Among the COMPLETE guids, pick the one whose newest part has the latest
#      modifyTime. Selecting the newest guid *before* validating could pick an
#      interrupted re-delivery over an intact older one.
#   5. If NO guid for the day is complete, the day is blocked (never partially
#      loaded) - it is a source problem, surfaced, not silently filled.
# ============================================================================
_FILENAME_PATTERN = re.compile(
    r"^(?P<n>.+?)_"
    r"(?P<batch_day>\d{8})_"
    r"(?P<guid>[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12})"
    r"(?:_part_(?P<part>\d+)_of_(?P<total>\d+))?"
    r"\.avro$"
)


def parse_avro_filename(name):
    """Return dict {batch_day, guid, part, total} or None if the name doesn't match."""
    m = _FILENAME_PATTERN.match(name)
    if not m:
        return None
    return {
        "batch_day": m.group("batch_day"),
        "guid":      m.group("guid"),
        "part":      int(m.group("part"))  if m.group("part")  else None,
        "total":     int(m.group("total")) if m.group("total") else None,
    }


def validate_parts(guid_files):
    """Verify all parts of ONE guid form a complete, non-empty set. -> (ok, error_message)."""
    # 0. Zero-byte files first. This is the cheapest and most common failure:
    #    the transfer either has not finished or died. Blocking the GUID here
    #    (driver-side, no Spark) is what keeps one bad day from killing the
    #    whole table read. If another COMPLETE guid exists for the day it wins;
    #    otherwise the day is blocked and re-evaluated on the next run.
    empty = sorted(f["name"] for f in guid_files if (f.get("size") or 0) == 0)
    if empty:
        shown = empty[:3] + (["..."] if len(empty) > 3 else [])
        return False, (f"{len(empty)} zero-byte file(s) - transfer incomplete "
                       f"or failed: {shown}")

    parts    = [f["part"]  for f in guid_files]
    totals   = [f["total"] for f in guid_files]
    has_part = [p is not None for p in parts]

    if any(has_part) and not all(has_part):
        with_p    = [f["name"] for f in guid_files if f["part"] is not None]
        without_p = [f["name"] for f in guid_files if f["part"] is None]
        return False, (f"Mixed parted and non-parted files for the same GUID. "
                       f"with_parts={with_p}, without_parts={without_p}")

    if not any(has_part):
        return True, None  # single-file export - nothing to check

    distinct_totals = set(totals)
    if len(distinct_totals) != 1:
        return False, f"Inconsistent 'of N' values across parts: {sorted(distinct_totals)}"

    expected_total = totals[0]
    seen, duplicates = set(), []
    for p in parts:
        if p in seen:
            duplicates.append(p)
        seen.add(p)

    expected_set = set(range(1, expected_total + 1))
    missing = sorted(expected_set - seen)
    extras  = sorted(seen - expected_set)

    if missing or extras or duplicates:
        return False, (f"Incomplete part set (expected {expected_total}, got {len(parts)}): "
                       f"missing={missing}, extras={extras}, "
                       f"duplicates={sorted(set(duplicates))}")
    return True, None


def _parse_all(all_files):
    """Parse every *.avro FileInfo. -> (parsed_meta_dicts, malformed_names)."""
    parsed, malformed = [], []
    for f in all_files:
        if not f.name.endswith(".avro"):
            continue
        meta = parse_avro_filename(f.name)
        if meta is None:
            malformed.append(f.name)
            continue
        meta["path"]  = f.path
        meta["name"]  = f.name
        meta["mtime"] = f.modifyTime  # epoch ms
        meta["size"]  = f.size        # bytes - 0 means unreadable / in flight
        parsed.append(meta)
    return parsed, malformed


def _choose_and_validate(day_files):
    """
    For ONE batch_day: validate every guid, then pick the LATEST COMPLETE one.

    Returns (selected_paths, chosen_guid, n_guids, error|None).
    Order matters: complete-first, latest-second. Picking the latest guid and
    then validating it would let an interrupted re-delivery mask a good export.
    """
    by_guid = defaultdict(list)
    for p in day_files:
        by_guid[p["guid"]].append(p)

    complete, errors = {}, {}
    for guid, files in by_guid.items():
        ok, err = validate_parts(files)
        if ok:
            complete[guid] = files
        else:
            errors[guid] = err

    if not complete:
        detail = "; ".join(f"{g}: {e}" for g, e in errors.items())
        return [], None, len(by_guid), f"No complete GUID for this day. {detail}"

    chosen_guid  = max(complete, key=lambda g: max(p["mtime"] for p in complete[g]))
    chosen_files = sorted(complete[chosen_guid], key=lambda p: (p["part"] or 0))
    return [p["path"] for p in chosen_files], chosen_guid, len(by_guid), None


def select_files_per_day(all_files):
    """
    Run choose+validate for EVERY batch_day in one table folder.

    Returns (per_day, malformed) where per_day maps
        batch_day -> {selected_paths, chosen_guid, n_guids, total_in_day, error}
    """
    parsed, malformed = _parse_all(all_files)

    by_day = defaultdict(list)
    for p in parsed:
        by_day[p["batch_day"]].append(p)

    per_day = {}
    for day, day_files in by_day.items():
        paths, guid, n_guids, err = _choose_and_validate(day_files)
        per_day[day] = {
            "selected_paths": paths,
            "chosen_guid":    guid,
            "n_guids":        n_guids,
            "total_in_day":   len(day_files),
            "error":          err,
        }
    return per_day, malformed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Bronze helpers
# ============================================================================
# Pull YYYYMMDD out of the source file path. Anchored on the trailing `_<8hex>_`
# (start of the NiFi UUID) so it cannot match a date earlier in the path.
_BATCH_DAY_REGEX = r"_(\d{8})_[0-9a-fA-F]{8}_"


def add_bronze_metadata(df):
    """
    Add the standard bronze metadata columns.

    _batch_day is parsed from the FILENAME, so re-reading the same physical file
    always reproduces the same value. _row_hash includes _batch_day, so the same
    row arriving on a different day is a distinct hash.
    """
    payload_cols = sorted(df.columns)  # snapshot before adding metadata cols
    return (df
        .withColumn("_source_file", input_file_name())
        .withColumn("_batch_day",
                    regexp_extract(col("_source_file"), _BATCH_DAY_REGEX, 1))
        .withColumn(
            "_row_hash",
            sha2(
                concat_ws(
                    "||",
                    *[coalesce(col(c).cast("string"), lit("")) for c in payload_cols],
                    coalesce(col("_batch_day"), lit(""))
                ),
                256
            )
        )
        .withColumn("_load_timestamp", current_timestamp())
        .withColumn("_job_id", job_id_col)
    )


# ---------------------------------------------------------------------------
# table_exists: cached. The old version ran SHOW TABLES on EVERY call, which
# under a thread pool serialises the whole run behind the metastore.
# ---------------------------------------------------------------------------
_EXISTING_TABLES = set()
_TABLES_LOCK     = threading.Lock()


def refresh_table_cache():
    global _EXISTING_TABLES
    with _TABLES_LOCK:
        _EXISTING_TABLES = {r.tableName for r in spark.sql("SHOW TABLES").collect()}
    return _EXISTING_TABLES


def table_exists(table_name):
    with _TABLES_LOCK:
        return table_name in _EXISTING_TABLES


def _register_table(table_name):
    with _TABLES_LOCK:
        _EXISTING_TABLES.add(table_name)


refresh_table_cache()


def get_table_folders(base_path):
    """Subfolders under the source_system root - one per source table."""
    try:
        return [f.name for f in mssparkutils.fs.ls(base_path) if f.isDir]
    except Exception as e:
        print(f"Error reading folders from {base_path}: {e}")
        return []


def _list_one(table_name):
    """Worker: list one table folder. -> (table_name, files|None, error|None)."""
    try:
        return table_name, mssparkutils.fs.ls(f"{source_base_path}/{table_name}"), None
    except Exception as e:
        return table_name, None, str(e)


def list_all_folders(table_names):
    """
    Parallel OneLake listing. Pure network round-trips, no Spark - this is where
    threading pays most: a 30-table source goes from ~30 serial round-trips to
    ~30/MAX_WORKERS. Returns {table_name: [FileInfo]}.
    """
    out = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for tbl, files, err in ex.map(_list_one, table_names):
            if err:
                print(f"  !! cannot list {tbl}: {err}")
            else:
                out[tbl] = files
    return out


def _day_in_range(day):
    """Honour the optional debug_day_from / debug_day_to bounds (lexical compare)."""
    if DAY_FROM and day < DAY_FROM:
        return False
    if DAY_TO and day > DAY_TO:
        return False
    return True


def get_loaded_days(bronze_table):
    """
    Which batch_days are already loaded for this table?

    Coverage truth is the DATA (DISTINCT _batch_day). The log is an event stream,
    not a state oracle - it can claim a load that a later overwrite destroyed.

    ONE narrow exception: a legitimately EMPTY export produces zero rows, so it
    can never appear in the data. Without this, an empty day would be re-read on
    every single run, forever, and the catch-up would never converge. We consult
    the log ONLY for rows that claim zero rows written, so a bulk range marker or
    an over-optimistic log row can never mask a real gap.
    """
    loaded = set()
    if table_exists(bronze_table):
        loaded = {r._batch_day for r in
                  spark.table(bronze_table).select("_batch_day").distinct().collect()
                  if r._batch_day}

    try:
        empties = spark.sql(f"""
            SELECT DISTINCT batch_day
            FROM   lh_metadata.log_table_loads
            WHERE  target_table = '{bronze_table}'
              AND  target_layer = 'bronze'
              AND  status       = 'SUCCESS'
              AND  operation    IN ('NOOP', 'CREATE_EMPTY')
              AND  COALESCE(rows_written, 0) = 0
              AND  batch_day RLIKE '^[0-9]{{8}}$'
        """).collect()
        loaded |= {r.batch_day for r in empties}
    except Exception as e:
        print(f"  !! cannot read empty-day log for {bronze_table}: {e}")

    return loaded


# ============================================================================
# Drift-proof AVRO reader
# ============================================================================
# Source Avro schemas drift across the multi-year history - not only added or
# removed columns (mergeSchema handles those) but column TYPE changes (POBOX
# boolean -> string). Spark's Avro reader will not coerce a boolean file into a
# string column, so a single mixed read aborts. Strategy:
#   1. Group files by their actual Avro schema (one export/day shares a schema,
#      so we fingerprint one representative file per day).
#   2. Read each schema-consistent group with its NATIVE types.
#   3. Cast every column to string (a valid Spark cast from any scalar type).
#   4. unionByName(allowMissingColumns=True) across the groups.
# Result: drift-proof, all-string bronze in a single write.
# ============================================================================
def _schema_fingerprint(schema):
    """Order-independent fingerprint of (name, type), ignoring nullability."""
    return tuple(sorted((f.name, f.dataType.simpleString()) for f in schema.fields))


def _short_err(e, limit=300):
    """First line + the innermost 'Caused by' of a Py4J/Spark exception."""
    s = str(e) or type(e).__name__
    head = s.splitlines()[0]
    m = re.search(r"Caused by: ([^\n]+)", s)
    return (f"{head} | {m.group(1)}" if m else head)[:limit]


def _probe_schema(path):
    """Header-only read of ONE Avro file. -> (fingerprint|None, error|None)."""
    try:
        return _schema_fingerprint(spark.read.format("avro").load(path).schema), None
    except Exception as e:
        return None, _short_err(e)


def group_paths_by_schema(day_paths_map):
    """
    day_paths_map: {batch_day: [paths]} - each day's files share one schema.
    Probe one file per day, group days with an identical schema.
    Collapses to a single group (one read) when there is no drift.

    A day whose representative file cannot be opened is NOT allowed to abort
    the table: it is returned in `bad_days` and the caller blocks it.
    Returns (path_groups, bad_days) with bad_days = {batch_day: error}.
    """
    groups, bad_days = {}, {}
    for day in sorted(day_paths_map):
        paths = day_paths_map[day]
        if not paths:
            continue
        fp, err = _probe_schema(paths[0])
        if err:
            bad_days[day] = (f"Unreadable Avro file "
                             f"{os.path.basename(paths[0])}: {err}")
            continue
        groups.setdefault(fp, []).extend(paths)
    return list(groups.values()), bad_days


def find_unreadable_files(day_paths_map):
    """
    Last-resort isolation, used ONLY after a batch read has already failed:
    open the header of EVERY selected file and return {batch_day: error} for
    each day owning at least one unreadable file. Header-only, so cheap per
    file, but it touches every file - that is why it is not done up front.
    (A file truncated mid-body still passes this probe; in that case the retry
    fails too and the table is reported FAILED, exactly as before.)
    """
    bad = {}
    for day in sorted(day_paths_map):
        for p in day_paths_map[day]:
            _, err = _probe_schema(p)
            if err:
                bad[day] = f"Unreadable Avro file {os.path.basename(p)}: {err}"
                break
    return bad


def read_avro_normalized(path_groups):
    """Read schema-consistent groups, cast to string, union. -> one DataFrame."""
    frames = []
    for gpaths in path_groups:
        df = spark.read.format("avro").option("mergeSchema", "true").load(gpaths)
        if BRONZE_ALL_STRING:
            df = df.select([col(c).cast("string").alias(c) for c in df.columns])
        frames.append(df)
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Core loader - ONE function, ONE Spark job per table, for ALL run modes
# ============================================================================
# Single-day, catch-up and bulk differ only in WHICH days they select. They all
# read every selected day's files in one Spark job and issue one Delta write.
# A per-day loop would mean N reads and N commits for N missing days - the thing
# that made catch-up crawl on long histories.
# ============================================================================
class Out:
    """Buffer this table's output so thread interleaving doesn't shred it."""
    def __init__(self, title):
        self.lines = [f"\n{'='*80}", title, "=" * 80]

    def p(self, msg):
        self.lines.append(msg)

    def dump(self):
        print("\n".join(self.lines))


def load_table(table_name, all_files, requested_days):
    """
    Load `requested_days` for one table into bronze.

    Returns dict: {table, status, days_loaded, days_skipped, days_blocked, rows}
      status in {SUCCESS, PARTIAL, NOTHING_TO_DO, SKIP, FAILED}

    Fault isolation: a single unusable day (incomplete part set, zero-byte or
    corrupt file) is BLOCKED and logged; the remaining days still load in one
    Spark job. Three layers, cheapest first:
      L1 validate_parts        - driver-side, filename + size only (0 B files)
      L2 group_paths_by_schema - one header read per day
      L3 find_unreadable_files - every file's header, only after a read failed

    Write modes (in priority order):
      FULL_OVERWRITE  -> mode(overwrite)          [debug_full_overwrite only]
      table missing   -> mode(overwrite)          [initial CREATE]
      FORCE_RELOAD    -> replaceWhere on the day  [explicit single-day rewrite]
      otherwise       -> mode(append)             [immutable: never rewrites]
    """
    bronze_table      = f"{folder_name}_{table_name}"
    source_table_full = f"{source_system}/{table_name}"
    out = Out(f"Table: {table_name}  ->  {bronze_table}")

    result = {"table": table_name, "status": "SKIP", "days_loaded": [],
              "days_skipped": [], "days_blocked": [], "rows": 0}

    blocked = []                       # [(day, error)]
    pending = set(requested_days)      # days with no terminal log row yet

    def _block(day, err, operation):
        """Block ONE day: remember it, log FAILED, take it out of `pending`."""
        blocked.append((day, err))
        pending.discard(day)
        lid, st = log_pipeline_start()
        log_pipeline_end(lid, day, source_table_full, bronze_table,
                         "FAILED", operation, 0, 0,
                         error_message=err, start_time=st)

    def _report_blocked(new_items, reason):
        out.p(f"  XX {len(new_items)} day(s) NOT loadable ({reason}):")
        for day, err in new_items[:10]:
            out.p(f"       {day}: {err}")
        if len(new_items) > 10:
            out.p(f"       ... and {len(new_items) - 10} more")

    def _finish(status, days_loaded=None, rows=0):
        result["status"]       = status
        result["days_loaded"]  = days_loaded or []
        result["days_blocked"] = [d for d, _ in blocked]
        result["rows"]         = rows
        out.dump()
        return result

    try:
        # ---------- 1. Select files per day -------------------------------
        per_day, malformed = select_files_per_day(all_files)
        if malformed:
            out.p(f"  !! {len(malformed)} file(s) didn't match the expected pattern (ignored):")
            for n in malformed[:5]:
                out.p(f"       {n}")
            if len(malformed) > 5:
                out.p(f"       ... and {len(malformed) - 5} more")

        candidate_days = sorted(d for d in requested_days if d in per_day)
        if not candidate_days:
            out.p(f"  -- no AVRO files for the requested day(s) - nothing to do")
            return _finish("NOTHING_TO_DO")

        # ---------- 2. IMMUTABILITY GUARD ---------------------------------
        # An already-loaded day is a no-op: no read, no write, no log row.
        # Bypassed only by an explicit force_reload or a full overwrite.
        if not (FORCE_RELOAD or FULL_OVERWRITE):
            loaded = get_loaded_days(bronze_table)
            skipped = [d for d in candidate_days if d in loaded]
            candidate_days = [d for d in candidate_days if d not in loaded]
            pending -= set(skipped)
            if skipped:
                result["days_skipped"] = skipped
                preview = skipped if len(skipped) <= 8 else skipped[:8] + ["..."]
                out.p(f"  -- {len(skipped)} day(s) already loaded, skipped (immutable): {preview}")

        if not candidate_days:
            out.p("  -- every requested day is already loaded - nothing to do")
            return _finish("NOTHING_TO_DO")

        # ---------- 3. L1: filename + size validation ---------------------
        good_days, l1 = [], []
        for day in candidate_days:
            r = per_day[day]
            if r["error"]:
                _block(day, r["error"], "VALIDATION")
                l1.append((day, r["error"]))
            else:
                good_days.append(day)
        if l1:
            _report_blocked(l1, "incomplete or empty export - fix at source")

        if not good_days:
            out.p("  XX no loadable day for this table")
            return _finish("FAILED")

        # ---------- 3b. L2: header probe, one file per day ----------------
        day_paths = {d: per_day[d]["selected_paths"] for d in good_days}
        path_groups, probe_bad = group_paths_by_schema(day_paths)
        if probe_bad:
            l2 = []
            for d, err in probe_bad.items():
                _block(d, err, "UNREADABLE")
                l2.append((d, err))
            _report_blocked(l2, "corrupt Avro - fix at source")
            good_days = [d for d in good_days if d not in probe_bad]
            day_paths = {d: day_paths[d] for d in good_days}

        if not good_days:
            out.p("  XX no loadable day for this table")
            return _finish("FAILED")

        n_files = sum(len(p) for p in day_paths.values())
        out.p(f"  days to load : {len(good_days)} ({good_days[0]} .. {good_days[-1]})")
        out.p(f"  files        : {n_files}")
        if len(path_groups) > 1:
            out.p(f"  ii {len(path_groups)} distinct source schema(s) across these days "
                  f"- reading per-schema, normalizing to string")

        # ---------- 4. Read EVERY selected day in ONE Spark job -----------
        # L3: if the batch read still fails (a file that opens but is broken
        # in the body), isolate the offending day(s) by probing every file,
        # block them and retry ONCE with the rest. A second failure propagates.
        log_ids = {d: log_pipeline_start() for d in good_days}
        df, record_count = None, 0
        for attempt in (1, 2):
            try:
                df = read_avro_normalized(path_groups)
                record_count = df.count()
                break
            except Exception as e:
                if attempt == 2:
                    raise
                out.p(f"  !! read failed: {_short_err(e)}")
                out.p(f"  !! probing every selected file to isolate the bad day(s)...")
                bad = find_unreadable_files(day_paths)
                if not bad:
                    out.p("  !! every file header opens - cannot isolate, giving up")
                    raise
                l3 = []
                for d, err in bad.items():
                    _block(d, err, "UNREADABLE")
                    l3.append((d, err))
                _report_blocked(l3, "corrupt Avro - fix at source")
                good_days = [d for d in good_days if d not in bad]
                if not good_days:
                    out.p("  XX no loadable day left for this table")
                    return _finish("FAILED")
                day_paths   = {d: day_paths[d] for d in good_days}
                path_groups, _ = group_paths_by_schema(day_paths)
                out.p(f"  retrying with {len(good_days)} day(s) "
                      f"({good_days[0]} .. {good_days[-1]})")

        out.p(f"  records read : {record_count}")
        exists = table_exists(bronze_table)

        # ---------- 5. Empty payload --------------------------------------
        # Zero rows across every selected day. If the table exists we write
        # NOTHING - an overwrite here is exactly what destroyed history before.
        if record_count == 0:
            if not exists:
                # The only safe overwrite: bootstrapping the table's schema.
                (add_bronze_metadata(df).write.format("delta")
                    .mode("overwrite").option("overwriteSchema", "true")
                    .saveAsTable(bronze_table))
                _register_table(bronze_table)
                op = "CREATE_EMPTY"
                out.p(f"  ok created empty {bronze_table} (schema only)")
            else:
                op = "NOOP"
                out.p(f"  -- 0 rows for {good_days} - no write, table untouched")
            for d in good_days:
                lid, st = log_ids[d]
                log_pipeline_end(lid, d, source_table_full, bronze_table,
                                 "SUCCESS", op, 0, 0, start_time=st)
                pending.discard(d)
            return _finish("PARTIAL" if blocked else "SUCCESS", good_days)

        # ---------- 6. Metadata + per-day row counts ----------------------
        df_bronze = add_bronze_metadata(df).cache()
        try:
            day_counts = {r["_batch_day"]: r["cnt"] for r in
                          df_bronze.groupBy("_batch_day")
                                   .agg(f_count(lit(1)).alias("cnt"))
                                   .collect()}
            parsed_days = sorted(d for d in day_counts if d)

            # In an append-only table a wrong _batch_day is unrecoverable: rows
            # land under a day nobody will ever reload. Refuse to write.
            unexpected = [d for d in parsed_days if d not in good_days]
            if unexpected or not parsed_days:
                msg = (f"_batch_day parsed from filenames {parsed_days} does not match "
                       f"the selected days {good_days}. Refusing to write (bronze is "
                       f"immutable; mislabelled rows cannot be corrected in place).")
                out.p(f"  XX {msg}")
                for d in good_days:
                    lid, st = log_ids[d]
                    log_pipeline_end(lid, d, source_table_full, bronze_table,
                                     "FAILED", "BATCH_DAY_MISMATCH", record_count, 0,
                                     error_message=msg, start_time=st)
                    pending.discard(d)
                return _finish("FAILED")

            # ---------- 7. Write ------------------------------------------
            if FULL_OVERWRITE:
                (df_bronze.write.format("delta")
                    .mode("overwrite").option("overwriteSchema", "true")
                    .saveAsTable(bronze_table))
                _register_table(bronze_table)
                op = "FULL_OVERWRITE"
                out.p(f"  ok TRUNCATED + reloaded {bronze_table}: {record_count} rows")

            elif not exists:
                (df_bronze.write.format("delta")
                    .mode("overwrite").option("overwriteSchema", "true")
                    .saveAsTable(bronze_table))
                _register_table(bronze_table)
                op = "CREATE"
                out.p(f"  ok created {bronze_table} with {record_count} rows")

            elif FORCE_RELOAD:
                in_list     = ", ".join(f"'{d}'" for d in good_days)
                rows_before = spark.table(bronze_table).count()
                (df_bronze.write.format("delta")
                    .mode("overwrite")
                    .option("replaceWhere", f"_batch_day IN ({in_list})")
                    .option("mergeSchema", "true")
                    .saveAsTable(bronze_table))
                op = "FORCE_RELOAD"
                out.p(f"  ok force-reloaded {good_days}: "
                      f"{rows_before} -> {spark.table(bronze_table).count()} rows")

            else:
                # Normal path. Pure append: no delete, no overwrite, no rewrite.
                (df_bronze.write.format("delta")
                    .mode("append").option("mergeSchema", "true")
                    .saveAsTable(bronze_table))
                op = "APPEND"
                out.p(f"  ok appended {record_count} rows for {len(good_days)} day(s)")

            # ---------- 8. One terminal log row per day -------------------
            for d in good_days:
                n = int(day_counts.get(d, 0))
                lid, st = log_ids[d]
                # A day inside a multi-day load can be individually empty.
                log_pipeline_end(lid, d, source_table_full, bronze_table,
                                 "SUCCESS", op if n else "NOOP", n, n, start_time=st)
                pending.discard(d)

            return _finish("PARTIAL" if blocked else "SUCCESS", good_days, record_count)
        finally:
            df_bronze.unpersist()

    except Exception as e:
        err = _short_err(e)
        out.p(f"  XX error loading {table_name}: {err}")
        out.p(traceback.format_exc())
        # Only the days that never reached a terminal state; already-skipped
        # and already-blocked days keep the row they have.
        for d in sorted(pending):
            lid, st = log_pipeline_start()
            log_pipeline_end(lid, d, source_table_full, bronze_table,
                             "FAILED", "ERROR", 0, 0,
                             error_message=err, start_time=st)
        return _finish("FAILED")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Main execution
# ============================================================================
def resolve_requested_days(all_files):
    """Which days does the current run mode want for this table?"""
    days = set()
    for f in all_files:
        if not f.name.endswith(".avro"):
            continue
        meta = parse_avro_filename(f.name)
        if not meta:
            continue
        d = meta["batch_day"]
        if RUN_MODE == "BULK":
            if _day_in_range(d):
                days.add(d)
        elif RUN_MODE == "CATCH_UP":
            if d <= batch_day:
                days.add(d)
        else:  # SINGLE_DAY
            if d == batch_day:
                days.add(d)
    return sorted(days)


def main():
    print(f"\nLanding -> Bronze  [{RUN_MODE}]  source={source_system}\n")

    table_folders = get_table_folders(source_base_path)
    if not table_folders:
        print(f"!! no subfolders under {source_base_path} - nothing to do")
        return
    print(f"found {len(table_folders)} table folder(s): {table_folders}")

    # --- Parallel OneLake listing (I/O bound - the cheapest win here) ------
    print(f"\nlisting {len(table_folders)} folder(s) with {MAX_WORKERS} thread(s)...")
    folder_files = list_all_folders(table_folders)

    work = []
    for tbl, files in folder_files.items():
        days = resolve_requested_days(files)
        if days:
            work.append((tbl, files, days))
        else:
            print(f"  -- {tbl}: no matching batch_day - skipped")

    if not work:
        print("\nnothing to load.")
        flush_log()
        return

    total_days = sum(len(d) for _, _, d in work)
    print(f"\n{len(work)} table(s), {total_days} candidate (table, day) pair(s)")

    # --- Parallel per-TABLE load ------------------------------------------
    # Tables are independent Delta tables -> concurrent writes are safe.
    # Days within a table are NOT parallelised: they go in one Spark job.
    # Note: Spark's scheduler is FIFO by default, so threads overlap only when
    # the leading job leaves cores free. If jobs serialise, set
    # spark.scheduler.mode=FAIR at SESSION START (%%configure) - not at runtime.
    results = []
    if MAX_WORKERS == 1:
        for tbl, files, days in work:
            results.append(load_table(tbl, files, days))
    else:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(load_table, tbl, files, days)
                       for tbl, files, days in work]
            results = [f.result() for f in futures]

    # --- One batched log write --------------------------------------------
    flush_log()

    # --- Summary ----------------------------------------------------------
    print("\n" + "=" * 80)
    print(f"Summary  [{RUN_MODE}]  job_id={job_id_str}")
    print("=" * 80)
    sym = {"SUCCESS": "ok", "PARTIAL": "~~", "NOTHING_TO_DO": "--",
           "SKIP": "--", "FAILED": "XX"}
    for r in sorted(results, key=lambda x: x["table"]):
        bits = []
        if r["days_loaded"]:
            bits.append(f"loaded={len(r['days_loaded'])}")
        if r["days_skipped"]:
            bits.append(f"already={len(r['days_skipped'])}")
        if r["days_blocked"]:
            bits.append(f"blocked={len(r['days_blocked'])}")
        if r["rows"]:
            bits.append(f"rows={r['rows']}")
        print(f"  {sym.get(r['status'], '??')} {r['table']:<40} "
              f"{r['status']:<14} {'  '.join(bits)}")

    loaded_days  = sum(len(r["days_loaded"])  for r in results)
    blocked_days = sum(len(r["days_blocked"]) for r in results)
    skipped_days = sum(len(r["days_skipped"]) for r in results)
    total_rows   = sum(r["rows"] for r in results)
    print(f"\n  tables={len(results)}  days_loaded={loaded_days}  "
          f"days_already_loaded={skipped_days}  days_blocked={blocked_days}  "
          f"rows={total_rows}")
    print(f"\n  SELECT * FROM lh_metadata.log_table_loads WHERE job_id = '{job_id_str}'")

    # --- Propagate failures to the pipeline activity ----------------------
    hard_failed = [r["table"] for r in results if r["status"] == "FAILED"]
    partial     = [r["table"] for r in results if r["days_blocked"]]
    if hard_failed:
        raise RuntimeError(f"Landing->Bronze FAILED for table(s): {hard_failed}")
    if partial and RAISE_ON_PARTIAL:
        raise RuntimeError(
            f"Loaded every good day, but {blocked_days} day(s) were skipped due to "
            f"incomplete / unreadable exports across table(s): {partial}. See "
            f"lh_metadata.log_table_loads (status='FAILED', "
            f"operation IN ('VALIDATION','UNREADABLE')). Set raise_on_partial='false' "
            f"to let the activity succeed while the gap stays logged."
        )


main()
print("\nProcess completed!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
