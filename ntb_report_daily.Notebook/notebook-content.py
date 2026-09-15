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
# META           "id": "a13e3d64-d65b-4196-a813-3475454ce68a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # ntb_report_daily  –  v1.0  (CSV + Delta, DENNÝ beh)
# 
# Totožný processing ako `ntb_report_monthly`, len:
# 1. Parameter je **`batch_day`** (default = dnes) – žiadny `report_month`
# 2. Filter v reportoch: **`_batch_day = {batch_day}`** (presný deň)
# 3. CSV do `Files/reports_daily/{report_day}/`
# 4. Delta tabuľky particionované podľa **`report_day`** (YYYY-MM-DD)
# 
# **Prečo partition podľa dňa:** dynamic partition overwrite tak pri re-rune prepíše
# LEN daný deň, ostatné dni zóstanú nedotknuté. (Pri mesačnej partícii by denný
# zápis prepísal celý mesiac – preto deň.)
# 
# **Fabric pipeline:** denne; bunku s parametrami môže override-ovať (`batch_day`).

# PARAMETERS CELL ********************

# ============================================================================
# CELL 1 – Parametre  (tag 'parameters' → Fabric pipeline môže override-ovať)
# ============================================================================
from pyspark.sql import SparkSession, functions as F
from datetime import date, timedelta, datetime
import os
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# Dynamic partition overwrite: prepíše len partition(y) prítomné v DataFrame
# → re-run pre rovnaký deň je bezpečný, staršie dni zostanú nedotknuté
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Defaultne: včerajší deň (Fabric pipeline môže poslať iné batch_day)
#batch_day  = int((date.today() - timedelta(days=1)).strftime("%Y%m%d"))
batch_day  = int((date.today()).strftime("%Y%m%d"))
#batch_day  = 20260729

report_day = datetime.strptime(str(batch_day), "%Y%m%d").strftime("%Y-%m-%d")



print(f"batch_day  : {batch_day}")
print(f"report_day : {report_day}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 2 – Definícia reportov  (denné; filter _batch_day = {batch_day})
# ============================================================================
REPORTS = [


    # ── 22 – G2G: MessageStore Report2 (DENNE, kĺzavé okno 2 mesiacov) ──
    {
        "name": "22_g2g_messagestore_report2",
        "sql": """
            WITH cfg AS (
                SELECT to_date(CAST({batch_day} AS STRING), 'yyyyMMdd') AS ref_date  -- predošlý deň
            )
            SELECT
                g.message_id                            AS MESSAGE_ID,
                g.class                                 AS CLASS,
                CAST(g.`timestamp` AS DATE)             AS CREATION_DATE,
                DATE_FORMAT(g.`timestamp`, 'HH:mm:ss')  AS CREATION_TIME,
                g.error_code                            AS ERROR_CODE,
                ec.popis_chyby                          AS ERROR_DESCRIPTION,
                g.event_name                            AS EVENT_NAME,
                g.sender_id                             AS SENDER_ID,
                g.recipient_id                          AS RECIPIENT_ID,
                g.correlation_id                        AS CORRELATION_ID,
                g.sub_module                            AS SUB_MODULE,
                g.posp_id                               AS POSP_ID,
                g.posp_version                          AS POSP_VERSION
            FROM lh_bronze.g2g_source_dwh20_all_sklog AS g
            CROSS JOIN cfg
            LEFT JOIN (
                SELECT DISTINCT kod_chyby, popis_chyby
                FROM lh_bronze.manual_zoznam_error_ciselnik_g2g
            ) AS ec
                ON ec.kod_chyby = '0'||g.error_code
            WHERE g.event_type = 'Error'
              AND g.`timestamp` >= add_months(cfg.ref_date, -2)   -- spodná hranica (-2 mesiace)
              AND g.`timestamp` <  date_add(cfg.ref_date, 1)      -- vrátane celého predošlého dňa
            ORDER BY CAST(g.`timestamp` AS DATE) DESC
        """
    },


    # ── 32 – G2G: výsledky report validácií na vstupnom nárazníku (DENNE) ──
    {
        "name": "32_g2g_vysledky_validacii_vstupny_naraznik",
        "sql": """
            SELECT
                error_code  AS ERROR_CODE,
                sender_id   AS SENDER_ID,
                COUNT(*)    AS DOC_COUNT
            FROM lh_bronze.g2g_source_dwh20_all_sklog
            WHERE _batch_day = {batch_day}
              AND event_type = 'Error'
              and error_code is not null
            GROUP BY error_code, sender_id
            order by COUNT(*) desc
        """
    },

]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 3 – Konfigurácia postprocessingu a Delta cieľa
# ============================================================================

# Denné reporty zatiaľ nepotrebujú date-postprocessing; dict nechávam pre
# konzistenciu s v2 a budúcu rozšíriteľnosť.
DATE_POSTPROCESS = {}

DELTA_LAKEHOUSE = "lh_bronze"   # ← zmen na "lh_gold" ak existuje gold lakehouse
TABLE_PREFIX    = "rpt_"

print(f"Delta cieľ    : {DELTA_LAKEHOUSE}.{TABLE_PREFIX}<report_name>")
print(f"CSV cieľ      : /lakehouse/default/Files/reports_daily/{report_day}/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 4 – Runner: SQL → CSV + Delta  (denný grain, partition = report_day)
# ============================================================================
results  = []
out_base = f"/lakehouse/default/Files/reports_daily/{report_day}"
os.makedirs(out_base, exist_ok=True)

CSV_SEP = ";"   # <-- delimiter pre CSV export

for report in REPORTS:
    name = report["name"]
    sql  = report["sql"].format(
        batch_day  = batch_day,
        report_day = report_day
    )

    print(f"\n{'─'*70}")
    print(f"⏳  {name}")

    csv_status   = "—"
    delta_status = "—"
    row_count    = 0

    # ── 1. SQL → Spark DataFrame → Pandas ───────────────────────────────
    try:
        sdf_src = spark.sql(sql)
        pdf     = sdf_src.toPandas()
        row_count = len(pdf)
    except Exception as e:
        print(f"   ❌  SQL chyba: {e}")
        results.append({"report": name, "rows": 0, "csv": "PRESKOČENÉ", "delta": f"CHYBA SQL: {str(e)[:120]}"})
        continue

    # ── 2. Postprocessing dátumu (ak nakonfigurovaný) ───────────────────
    if name in DATE_POSTPROCESS:
        cfg     = DATE_POSTPROCESS[name]
        raw_col = cfg["raw_col"]
        pdf["mesiac"] = pd.to_datetime(
            pdf[raw_col], dayfirst=True, errors="coerce"
        ).dt.strftime("%m.%Y")
        pdf = (
            pdf.drop(columns=[raw_col])
               .groupby(cfg["group_cols"], dropna=False)
               .size()
               .reset_index(name="pocet")
        )
        row_count = len(pdf)

    # ── 3. CSV export ─────────────────────────────────────────
    try:
        csv_path = f"{out_base}/{name}_{report_day}.csv"
        pdf.to_csv(csv_path, index=False, encoding="utf-8-sig", sep=CSV_SEP)
        csv_status = "OK"
        print(f"   📄  CSV:   {csv_path}  ({row_count} riadkov)")
    except Exception as e:
        csv_status = f"CHYBA: {str(e)[:120]}"
        print(f"   ⚠️   CSV chyba: {e}")

    # ── 4. Delta write (dynamic partition overwrite → prepíše LEN report_day) ─
    try:
        table_name = f"{TABLE_PREFIX}{name}"
        table_full = f"{DELTA_LAKEHOUSE}.{table_name}" if DELTA_LAKEHOUSE else table_name

        if row_count > 0:
            # Zachovaj poradie stĺpcov presne ako v SQL SELECT / pandas
            spark_schema = spark.createDataFrame(pdf).schema
            ordered_cols = pdf.columns.tolist()            
            sdf_out = spark.createDataFrame(pdf)
        else:
            sdf_out = sdf_src

        sdf_out = sdf_out.withColumn("report_day", F.lit(report_day))

        (
            sdf_out.write
                   .format("delta")
                   .mode("overwrite")          # dynamic partition overwrite
                   .partitionBy("report_day")
                   .option("mergeSchema", "true")
                   .saveAsTable(table_full)
        )

        delta_status = "OK"
        print(f"   📊  Delta: {table_full}  (partition: {report_day}, {row_count} riadkov)")

    except Exception as e:
        delta_status = f"CHYBA: {str(e)[:120]}"
        print(f"   ⚠️   Delta chyba: {e}")

    results.append({
        "report" : name,
        "rows"   : row_count,
        "csv"    : csv_status,
        "delta"  : delta_status,
    })

# ── Log ──────────────────────────────────────────────────
summary  = pd.DataFrame(results)
log_path = f"{out_base}/_log_{report_day}.csv"
summary.to_csv(log_path, index=False, encoding="utf-8-sig", sep=CSV_SEP)

print(f"\n{'='*70}")
print(f"SÚHRN – {report_day}")
print(f"{'='*70}")
print(summary.to_string(index=False))
print(f"\nLog: {log_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 5 – Verifikácia: ukáž partície (dni) v Delta tabuľke reportu 32
# ============================================================================
print("Dostupné Delta tabuľky s prefixom rpt_:")
spark.sql(f"""
    SHOW TABLES IN {DELTA_LAKEHOUSE} LIKE '{TABLE_PREFIX}*'
""").show(50, truncate=False)

sample_table = f"{DELTA_LAKEHOUSE}.{TABLE_PREFIX}32_g2g_vysledky_validacii_vstupny_naraznik"
print(f"\nHistória dní v: {sample_table}")
spark.sql(f"""
    SELECT report_day, COUNT(*) AS pocet_zaznamov
    FROM {sample_table}
    GROUP BY report_day
    ORDER BY report_day
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
