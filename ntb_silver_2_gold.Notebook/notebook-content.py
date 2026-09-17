# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "18773a25-3876-4b71-8d0b-9e1535932283",
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "d626fea3-3116-4af5-9cfb-9196dd43cba1",
# META       "known_lakehouses": [
# META         {
# META           "id": "0abf8ee5-18ac-4c6f-85f2-d828a032ff48"
# META         },
# META         {
# META           "id": "4c73f3f2-9c1c-45d3-b6be-3607c949dc6d"
# META         },
# META         {
# META           "id": "18773a25-3876-4b71-8d0b-9e1535932283"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # `ntb_silver_2_gold` — append-only GOLD variant
# 
# **Dimensions **and** facts both land in **`lh_gold`** (`GOLD_LH`).
# 
# - **Dimensions** keep their existing behavior unchanged: hash surrogate keys, SCD1/SCD2, Unknown member, full-snapshot expire/delete. Only the target lakehouse changed.
# - **Facts** are **append-only**. `_batch_day` = the day the record ARRIVED.
#   - **full** — source re-exports everything each `_batch_day`; we extract the DELTA (new/changed rows vs. what gold already holds).
#   - **delta** — each `_batch_day` already holds only changes; we append those days as events.
#   - No updates, no deletes. Corrections happen only via **`force_reload`** (full rebuild).
#   - Change detection is **key-aware**: with a PK it chains `_content_hash` per key (reverts A→B→A emit correctly); without a PK it anti-joins on content.


# PARAMETERS CELL ********************

# Parameters (overridden by Pipe_Source_process at runtime). Tag this cell as 'parameters'.
job_id        = '20260714060023'
source_system = 'METAIS'
batch_day     = '20260727'   # log label; gold computes the slice from silver itself
pipeline_name = 'ntb_silver_2_gold'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Config


# CELL ********************

from pyspark.sql import DataFrame, Column, Window, functions as F
from delta.tables import DeltaTable
from datetime import datetime

job_id_str = str(job_id)            # do NOT regenerate job_id — it flows from Pipe_Main

SILVER_LH = "lh_silver"
GOLD_LH   = "lh_gold"        # THIS VARIANT: dims + facts both land here
META_LH   = "lh_metadata"
META_SETUP_TABLE = f"{META_LH}.metadata_table_column_setup"
RELATIONS_TABLE  = f"{META_LH}.table_relations"     # optional: fact -> dim FK mapping
LOG_TABLE        = f"{META_LH}.gold_load_log"       # separate log; does not touch log_table_loads

SCD2_DEFAULT      = True             # dimension default: True = keep history, False = overwrite current
LOAD_TYPE_DEFAULT = "delta"          # 'delta' (day = changes) or 'full' (day = whole snapshot)
UNKNOWN_SK        = "-1"             # surrogate key for the Unknown / late-arriving member

FORCE_FACT_RELOAD = False            # on-demand fact rebuild switch (append-only otherwise)
SEED_DAY          = "00000000"       # sorts before any real _batch_day when chaining state

# bronze/silver lineage columns present in the IF as attributes but excluded from gold business
# columns (gold adds its own lineage). Business keys like _key / _rev are NOT here, so they survive.
LINEAGE_COLS = {
    "_batch_day", "_load_timestamp", "_job_id", "_row_hash", "_source_file",
    "_silver_load_timestamp", "_silver_job_id", "_bronze_load_timestamp", "_batch_month",
}

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Build gold column metadata (from the IF config tables)
# 
# Joins `table_config` + `table_type_config`, derives `load_type`, no env filter, ordinal from `_ordinal`.


# CELL ********************

# ============================================================================
# Gold column metadata - READ ONLY
# lh_metadata.metadata_table_column_setup is derived and written by
# ntb_create_silver_tables (single run in Pipe_Main, before the fan-out).
# This notebook runs 7x in parallel and must never write to lh_metadata.
# ============================================================================
df_meta = spark.table(META_SETUP_TABLE).cache()
stamp = (df_meta.select("_mapping_file", "_derived_at").first()
         if "_mapping_file" in df_meta.columns else None)
print(f"gold column metadata rows: {df_meta.count()}"
      + (f"  (from {stamp[0]}, derived {stamp[1]:%Y-%m-%d %H:%M})" if stamp else ""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helpers — projection (rename + cast), boolean, hashing


# CELL ********************

_TYPE_MAP = {
    "string": "string", "str": "string", "text": "string",
    "varchar": "string", "nvarchar": "string", "char": "string",
    "int": "int", "integer": "int",
    "bigint": "bigint", "long": "bigint",
    "smallint": "smallint", "tinyint": "tinyint",
    "double": "double", "float": "double", "real": "double",
    "boolean": "boolean", "bool": "boolean",
    "date": "date", "timestamp": "timestamp", "datetime": "timestamp",
}

def spark_type(dt):
    if not dt:
        return "string"
    base = dt.strip().lower()
    if base.startswith(("decimal", "numeric")):
        return base.replace("numeric", "decimal")
    return _TYPE_MAP.get(base, "string")

def normalize_boolean(c):
    s = F.lower(F.trim(c.cast("string")))
    return (F.when(c.isNull(), F.lit(None).cast("boolean"))
             .when(s.isin("1", "true", "t", "yes", "y", "on"), F.lit(True))
             .when(s.isin("0", "false", "f", "no", "n", "off"), F.lit(False))
             .otherwise(F.lit(None).cast("boolean")))

def build_projection(meta_rows, available_cols=None):
    """Rows for ONE gold table (ordered) -> (select_exprs, business_key_gold_cols).
       Skips lineage columns and any attribute not physically in silver."""
    exprs, pk_cols = [], []
    for r in meta_rows:
        src = r["silver_column_name"]
        if src is None or src in LINEAGE_COLS:
            continue
        if available_cols is not None and src not in available_cols:
            print(f"    ! skip {src}: not present in silver")
            continue
        tgt = r["gold_column_name"] or src
        dt  = (r["gold_column_data_type"] or "").strip().lower()
        e = normalize_boolean(F.col(src)) if dt in ("boolean", "bool") \
            else F.col(src).cast(spark_type(dt))
        exprs.append(e.alias(tgt))
        if r["gold_column_is_pk"]:
            pk_cols.append(tgt)
    return exprs, pk_cols

def with_hashes(df, business_keys):
    bk = F.sha2(F.concat_ws("||",
        *[F.coalesce(F.col(k).cast("string"), F.lit("∅")) for k in business_keys]), 256)
    content_cols = sorted([c for c in df.columns if not c.startswith("_")])
    ch = F.sha2(F.concat_ws("||",
        *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in content_cols]), 256)
    return df.withColumn("_bk_hash", bk).withColumn("_content_hash", ch)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dimensions — UNCHANGED behavior (SCD1/SCD2 + Unknown member + full expire/delete)
# 
# Only the target lakehouse changed (writes to `GOLD_LH` = `lh_gold_append`).


# CELL ********************

def _add_gold_meta(df, sk_name, scd2):
    df = (df.withColumn("_gold_load_timestamp", F.current_timestamp())
            .withColumn("_gold_modified_timestamp", F.lit(None).cast("timestamp"))
            .withColumn("_gold_job_id", F.lit(job_id_str))
            .withColumn("_is_current", F.lit(True))
            .withColumn("_valid_from", F.current_timestamp())
            .withColumn("_valid_to", F.lit(None).cast("timestamp")))
    sk = (F.sha2(F.concat_ws("||", F.col("_bk_hash"), F.col("_valid_from").cast("string")), 256)
          if scd2 else F.col("_bk_hash"))
    return df.withColumn(sk_name, sk)

def _ensure_unknown_member(tgt, sk_name):
    t = spark.table(tgt)
    if t.filter(F.col(sk_name) == F.lit(UNKNOWN_SK)).head(1):
        return
    base = t.limit(1)
    if not base.head(1):
        return
    cols = []
    for f in t.schema.fields:
        n = f.name
        if   n == sk_name:       cols.append(F.lit(UNKNOWN_SK).cast(f.dataType).alias(n))
        elif n == "_bk_hash":    cols.append(F.lit(UNKNOWN_SK).alias(n))
        elif n == "_is_current": cols.append(F.lit(True).alias(n))
        elif n == "_valid_from": cols.append(F.current_timestamp().alias(n))
        elif n == "_valid_to":   cols.append(F.lit(None).cast("timestamp").alias(n))
        else:                    cols.append(F.lit(None).cast(f.dataType).alias(n))
    base.select(*cols).write.format("delta").mode("append").saveAsTable(tgt)

def _scd2_merge(tgt, source):
    dt = DeltaTable.forName(spark, tgt)
    (dt.alias("t").merge(source.alias("s"), "t._bk_hash = s._bk_hash AND t._is_current = true")
       .whenMatchedUpdate(condition="t._content_hash <> s._content_hash",
           set={"_is_current": "false", "_valid_to": "current_timestamp()",
                "_gold_modified_timestamp": "current_timestamp()"})
       .execute())
    current = spark.table(tgt).filter("_is_current = true").select("_bk_hash")
    to_insert = source.join(current, "_bk_hash", "left_anti")
    if to_insert.head(1):
        to_insert.write.format("delta").mode("append").saveAsTable(tgt)

def _scd1_merge(tgt, source):
    dt = DeltaTable.forName(spark, tgt)
    upd = {c: f"s.{c}" for c in source.columns if c not in ("_valid_from", "_gold_load_timestamp")}
    upd["_gold_modified_timestamp"] = "current_timestamp()"
    (dt.alias("t").merge(source.alias("s"), "t._bk_hash = s._bk_hash")
       .whenMatchedUpdate(condition="t._content_hash <> s._content_hash", set=upd)
       .whenNotMatchedInsertAll()
       .execute())

def _expire_absent(tgt, snapshot, scd2):
    """full snapshot only: current members whose key is absent from the snapshot are expired (SCD2)
       or deleted (SCD1). Unknown member (-1) is always protected."""
    dt = DeltaTable.forName(spark, tgt)
    bk = snapshot.select("_bk_hash").distinct()
    guard = "t._is_current = true AND t._bk_hash <> '-1'"
    m = dt.alias("t").merge(bk.alias("s"), "t._bk_hash = s._bk_hash AND t._is_current = true")
    if scd2:
        m = m.whenNotMatchedBySourceUpdate(condition=guard,
                set={"_is_current": "false", "_valid_to": "current_timestamp()",
                     "_gold_modified_timestamp": "current_timestamp()"})
    else:
        m = m.whenNotMatchedBySourceDelete(condition=guard)
    m.execute()

def process_dimension(gold, silver, meta_rows, scd2, load_type):
    sk_name = f"sk_{gold}"
    sdf = spark.table(f"{SILVER_LH}.{silver}")
    exprs, pk_cols = build_projection(meta_rows, set(sdf.columns))
    if not pk_cols:
        raise ValueError(f"{gold}: no PK / business key present in both metadata and silver")
    order_col = "_silver_load_timestamp" if "_silver_load_timestamp" in sdf.columns \
                else ("_batch_day" if "_batch_day" in sdf.columns else None)
    if load_type == "full" and "_batch_day" in sdf.columns:
        latest = sdf.agg(F.max("_batch_day")).first()[0]
        sdf = sdf.filter(F.col("_batch_day") == F.lit(latest))   # snapshot = latest day only
    sel = exprs + ([F.col(order_col)] if order_col else [])
    src = with_hashes(sdf.select(*sel), pk_cols)
    if order_col:
        w = Window.partitionBy("_bk_hash").orderBy(F.col(order_col).desc())
        src = src.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", order_col)
    else:
        src = src.dropDuplicates(["_bk_hash"])
    prepared = _add_gold_meta(src, sk_name, scd2)
    tgt = f"{GOLD_LH}.{gold}"
    n = prepared.count()
    if not spark.catalog.tableExists(tgt):
        prepared.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(tgt)
    else:
        _scd2_merge(tgt, prepared) if scd2 else _scd1_merge(tgt, prepared)
        if load_type == "full":
            _expire_absent(tgt, prepared, scd2)      # remove/expire members gone from the snapshot
    _ensure_unknown_member(tgt, sk_name)
    return n


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Facts — APPEND-ONLY into `lh_gold_append`
# 
# `_batch_day` = arrival day. **full** exports are delta-extracted; **delta** days are appended as events. Key-aware change detection. Rebuild only via `force_reload`.


# CELL ********************

def resolve_fact_fks(fact_gold, df):
    """Optional. Reads lh_metadata.table_relations; if absent, facts keep business keys.
       Dimensions are read from GOLD_LH (= lh_gold_append in this variant)."""
    if not spark.catalog.tableExists(RELATIONS_TABLE):
        return df
    rels = spark.table(RELATIONS_TABLE).filter(F.col("fact_gold_table") == F.lit(fact_gold)).collect()
    for r in rels:
        d = r.asDict()
        dim_tbl = d["dim_gold_table"]; sk = d["dim_sk_col"]
        fbk = [c.strip() for c in d["fact_bk_cols"].split(",")]
        dbk = [c.strip() for c in d["dim_bk_cols"].split(",")]
        date_col = d.get("fact_date_col") or None
        dim = spark.table(f"{GOLD_LH}.{dim_tbl}")
        if date_col:
            dsel = dim.select(*[F.col(c).alias(f"__d_{c}") for c in dbk], sk,
                              F.col("_valid_from").alias("__vf"), F.col("_valid_to").alias("__vt"))
            cond = [F.col(f) == F.col(f"__d_{x}") for f, x in zip(fbk, dbk)]
            cond += [F.col(date_col) >= F.col("__vf"),
                     F.coalesce(F.col("__vt"), F.lit("2999-12-31").cast("timestamp")) > F.col(date_col)]
            drop = [f"__d_{x}" for x in dbk] + ["__vf", "__vt"]
        else:
            dsel = (dim.filter(F.col("_is_current") == True)
                       .select(*[F.col(c).alias(f"__d_{c}") for c in dbk], sk))
            cond = [F.col(f) == F.col(f"__d_{x}") for f, x in zip(fbk, dbk)]
            drop = [f"__d_{x}" for x in dbk]
        df = (df.join(dsel, cond, "left")
                .withColumn(sk, F.coalesce(F.col(sk), F.lit(UNKNOWN_SK)))
                .drop(*drop))
    return df

def _content_hash_col(df):
    cols = sorted([c for c in df.columns if not c.startswith("_")])
    return F.sha2(F.concat_ws("||",
        *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols]), 256)

def _fact_meta(df):
    return (df.withColumn("_gold_load_timestamp", F.current_timestamp())
              .withColumn("_gold_job_id", F.lit(job_id_str)))

def _gold_watermark(tgt, has_bd):
    if not spark.catalog.tableExists(tgt) or not has_bd:
        return None
    return spark.table(tgt).agg(F.max("_batch_day")).first()[0]

def _gold_state(tgt):
    """Latest known state (_content_hash) per business key currently in the append fact."""
    g = spark.table(tgt)
    w = Window.partitionBy("_bk_hash").orderBy(
        F.col("_batch_day").desc(), F.col("_gold_load_timestamp").desc())
    return (g.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
             .select("_bk_hash", "_content_hash"))

def _changes_over_days(rows, seed):
    """rows carry _bk_hash, _content_hash, _batch_day (one per key/day).
       seed = current gold state (_bk_hash, _content_hash) as a synthetic earliest day, or None.
       Returns rows where a key's content changed vs the chained state."""
    dec = rows.select("_bk_hash", "_content_hash", "_batch_day")
    if seed is not None:
        dec = dec.unionByName(seed.withColumn("_batch_day", F.lit(SEED_DAY)))
    w = Window.partitionBy("_bk_hash").orderBy(F.col("_batch_day").asc())
    dec = dec.withColumn("_prev", F.lag("_content_hash").over(w))
    keys = (dec.filter(F.col("_batch_day") != F.lit(SEED_DAY))
               .filter(F.col("_prev").isNull() | (F.col("_content_hash") != F.col("_prev")))
               .select("_bk_hash", "_batch_day", "_content_hash").distinct())
    return rows.join(keys, ["_bk_hash", "_batch_day", "_content_hash"], "inner")

def process_fact(gold, silver, meta_rows, load_type, force_reload=None):
    force_reload = FORCE_FACT_RELOAD if force_reload is None else force_reload
    sdf = spark.table(f"{SILVER_LH}.{silver}")
    exprs, pk_cols = build_projection(meta_rows, set(sdf.columns))
    has_bd = "_batch_day" in sdf.columns
    keyed  = bool(pk_cols) and has_bd          # key-based needs a PK and an order (arrival day)
    tgt    = f"{GOLD_LH}.{gold}"
    exists = spark.catalog.tableExists(tgt)
    keep   = exprs + ([F.col("_batch_day")] if has_bd else [])

    def _project_hashed(df):
        p = df.select(*keep)
        if keyed:
            return with_hashes(p, pk_cols)                         # _bk_hash + _content_hash
        return p.withColumn("_content_hash", _content_hash_col(p)) # content only

    # ---- on-demand full rebuild (overwrite) -------------------------------------
    if force_reload or not exists:
        cand = _project_hashed(sdf)
        if keyed:
            cand = cand.dropDuplicates(["_bk_hash", "_batch_day"])
            out  = _changes_over_days(cand, None)                  # full change history
        elif load_type == "full":
            if has_bd:                                             # first arrival per distinct row
                w = Window.partitionBy("_content_hash").orderBy(F.col("_batch_day").asc())
                cand = cand.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
            else:
                cand = cand.dropDuplicates(["_content_hash"])
            out  = cand
        else:                                                      # delta, no PK: every event
            out  = cand.dropDuplicates(["_content_hash", "_batch_day"]) if has_bd else cand
        out = resolve_fact_fks(gold, _fact_meta(out))
        n = out.count()
        (out.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true").saveAsTable(tgt))
        print(f"    ~ {gold}: full rebuild ({'keyed' if keyed else load_type}) -> {n:,}")
        return n

    # ---- incremental append -----------------------------------------------------
    wm = _gold_watermark(tgt, has_bd)
    new_sdf = sdf.filter(F.col("_batch_day") > F.lit(wm)) if wm else sdf
    if wm and new_sdf.head(1) is None:
        print(f"    = {gold}: no _batch_day > {wm}"); return 0
    cand = _project_hashed(new_sdf)

    if keyed:
        cand = cand.dropDuplicates(["_bk_hash", "_batch_day"])
        to_add = _changes_over_days(cand, _gold_state(tgt))        # chained from current state
    elif load_type == "full":
        w = Window.partitionBy("_content_hash").orderBy(F.col("_batch_day").asc())
        cand = cand.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
        seen = spark.table(tgt).select("_content_hash").distinct()
        to_add = cand.join(seen, "_content_hash", "left_anti")     # never-seen content = delta
    else:                                                          # delta, no PK
        seen = spark.table(tgt).select("_content_hash", "_batch_day").distinct()
        to_add = cand.join(seen, ["_content_hash", "_batch_day"], "left_anti")

    to_add = resolve_fact_fks(gold, _fact_meta(to_add))
    n = to_add.count()
    if n == 0:
        print(f"    = {gold}: nothing new to append"); return 0
    (to_add.write.format("delta").mode("append")
          .option("mergeSchema", "true").saveAsTable(tgt))
    print(f"    + {gold}: appended {n:,} ({'keyed' if keyed else load_type})")
    return n


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run — dimensions first (so fact FK lookups resolve), then facts


# CELL ********************

def _log(gold, silver, op, status, rows, err=None):
    spark.createDataFrame(
        [(job_id_str, batch_day, pipeline_name, "silver", "gold",
          silver, gold, datetime.now(), status, op, int(rows), err)],
        "job_id string, batch_day string, pipeline_name string, source_layer string, "
        "target_layer string, source_table string, target_table string, log_time timestamp, "
        "status string, operation string, rows_written long, error_message string"
    ).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(LOG_TABLE)

def run_gold(source_system, batch_day, scd2_default=SCD2_DEFAULT):
    meta = spark.table(META_SETUP_TABLE)
    prefix = source_system.lower() + "_"
    tabs = (meta.select("table_type", "gold_table_name", "silver_table_name", "load_type")
                .where(F.col("silver_table_name").startswith(prefix))
                .distinct().orderBy("table_type", "gold_table_name").collect())   # DIM < FACT
    print(f"{source_system}: {len(tabs)} gold tables")
    ok, failed = [], []
    for t in tabs:
        gold, silver, ttype = t["gold_table_name"], t["silver_table_name"], t["table_type"]
        load_type = (t["load_type"] or LOAD_TYPE_DEFAULT).strip().lower()
        if load_type not in ("full", "delta"):
            load_type = LOAD_TYPE_DEFAULT
        rows_meta = meta.where(F.col("gold_table_name") == gold).orderBy("ordinal_position").collect()
        try:
            if ttype == "DIM":
                n = process_dimension(gold, silver, rows_meta, scd2_default, load_type)
                op = f"DIM_{load_type.upper()}"
            elif ttype == "FACT":
                n = process_fact(gold, silver, rows_meta, load_type)
                op = f"FACT_{load_type.upper()}"
            else:
                print(f"  skip {gold}: type {ttype}"); continue
            _log(gold, silver, op, "SUCCESS", n)
            ok.append(gold); print(f"  OK  {ttype:4} {load_type:5} {gold}: {n:,}")
        except Exception as e:
            _log(gold, silver, ttype, "FAILED", 0, str(e))
            failed.append(gold); print(f"  ERR {gold}: {e}")
    print(f"\nDone. ok={len(ok)} failed={len(failed)}")
    if failed:
        print("failed:", failed)

run_gold(source_system, batch_day)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
