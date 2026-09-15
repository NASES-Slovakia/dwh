-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "a13e3d64-d65b-4196-a813-3475454ce68a",
-- META       "default_lakehouse_name": "lh_bronze",
-- META       "default_lakehouse_workspace_id": "d626fea3-3116-4af5-9cfb-9196dd43cba1",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "0abf8ee5-18ac-4c6f-85f2-d828a032ff48"
-- META         },
-- META         {
-- META           "id": "a13e3d64-d65b-4196-a813-3475454ce68a"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "known_warehouses": []
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC -- Náhľad na dáta (skontroluj, či account_age_days nie sú záporné/NULL)
-- MAGIC SELECT * FROM lh_silver.ml_iam_dormancy LIMIT 20;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC WITH dedup AS (
-- MAGIC   SELECT * FROM (
-- MAGIC     SELECT u.*, ROW_NUMBER() OVER (PARTITION BY u.uri ORDER BY u.import_date DESC) AS rn
-- MAGIC     FROM lh_bronze.iam_t_usr u
-- MAGIC     WHERE u._batch_day = (SELECT MAX(_batch_day) FROM lh_bronze.iam_t_usr)
-- MAGIC   ) WHERE rn = 1
-- MAGIC )
-- MAGIC SELECT
-- MAGIC   identity_type,
-- MAGIC   COUNT(*)                                                       AS pocet,
-- MAGIC   SUM(CASE WHEN last_login IS NULL THEN 1 ELSE 0 END)            AS bez_last_login,
-- MAGIC   SUM(CASE WHEN last_login IS NOT NULL THEN 1 ELSE 0 END)        AS ma_last_login
-- MAGIC FROM dedup
-- MAGIC GROUP BY identity_type
-- MAGIC ORDER BY pocet DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC df = spark.sql("""
-- MAGIC   SELECT * FROM (
-- MAGIC     SELECT u.*, ROW_NUMBER() OVER (PARTITION BY u.uri ORDER BY u.import_date DESC) rn
-- MAGIC     FROM lh_bronze.iam_t_usr u
-- MAGIC     WHERE u._batch_day = (SELECT MAX(_batch_day) FROM lh_bronze.iam_t_usr)
-- MAGIC   ) WHERE rn = 1
-- MAGIC """)
-- MAGIC 
-- MAGIC from pyspark.sql import functions as F
-- MAGIC n = df.count()
-- MAGIC rows = []
-- MAGIC for c in df.columns:
-- MAGIC     d     = df.select(F.countDistinct(c)).first()[0]
-- MAGIC     nulls = df.select(F.sum(F.col(c).isNull().cast("int"))).first()[0] or 0
-- MAGIC     rows.append((c, d, nulls, round(100*nulls/n, 1)))
-- MAGIC spark.createDataFrame(rows, ["stlpec","distinct","nulls","null_pct"]) \
-- MAGIC      .orderBy("distinct").show(100, truncate=False)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- ============================================================
-- IAM dormantnosť v5 – vek ako PÁSMO (robustné voči null/outlierom)
-- Zmena oproti v4: age_years -> age_band; null/outliery/ne-osoby -> 'neznámy'.
-- Zachováva všetky riadky kohorty (nestrácame ~34 % kvôli chýbajúcemu veku).
-- ============================================================

CREATE OR REPLACE TABLE lh_silver.ml_iam_dormancy AS
WITH dedup AS (
    SELECT *
    FROM (
        SELECT
            u.*,
            ROW_NUMBER() OVER (PARTITION BY u.uri ORDER BY u.import_date DESC) AS rn
        FROM lh_bronze.iam_t_usr AS u
        WHERE u._batch_day = (SELECT MAX(_batch_day) FROM lh_bronze.iam_t_usr)
    )
    WHERE rn = 1
),
ref AS (
    SELECT to_date(CAST((SELECT MAX(_batch_day) FROM lh_bronze.iam_t_usr) AS STRING), 'yyyyMMdd') AS ref_date
),
base AS (
    SELECT
        d.uri,
        CASE
            WHEN d.identity_type = 1        THEN 'FO'
            WHEN d.identity_type = 2        THEN 'PO'
            WHEN d.identity_type IN (4,5,6) THEN 'OVM'
            ELSE 'OTHER'
        END                                                    AS segment,
        d.edesk_status,
        d.upvs_status,
        d.source,
        d.edesk_cuet_enabled,
        datediff(r.ref_date, to_date(d.created, 'yyyy-MM-dd')) AS account_age_days,
        -- surový vek (môže byť NULL alebo mimo rozsahu -> ošetríme v pásme nižšie)
        CAST(FLOOR(datediff(r.ref_date, to_date(d.birth_date, 'yyyy-MM-dd')) / 365.25) AS INT) AS age_raw,
        CASE
            WHEN to_date(d.last_login, 'yyyy-MM-dd') < add_months(r.ref_date, -12) THEN 1
            ELSE 0
        END                                                    AS is_dormant
    FROM dedup d
    CROSS JOIN ref r
    WHERE d.last_login IS NOT NULL
)
SELECT
    uri,          -- ID – v AutoML vyhoď zo vstupných featur
    segment,
    edesk_status,
    upvs_status,
    source,
    edesk_cuet_enabled,
    account_age_days,
    CASE
        WHEN age_raw BETWEEN 0  AND 17  THEN '0-17'
        WHEN age_raw BETWEEN 18 AND 34  THEN '18-34'
        WHEN age_raw BETWEEN 35 AND 49  THEN '35-49'
        WHEN age_raw BETWEEN 50 AND 64  THEN '50-64'
        WHEN age_raw BETWEEN 65 AND 120 THEN '65+'
        ELSE 'neznámy'   -- NULL (napr. PO/OVM), parse-fail alebo nezmysel
    END                                    AS age_band,
    is_dormant
FROM base
;



-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- ------------------------------------------------------------
-- KONTROLY
-- ------------------------------------------------------------
-- rozloženie tried
SELECT is_dormant, COUNT(*) AS pocet,
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),1) AS podiel_pct
FROM lh_silver.ml_iam_dormancy
GROUP BY is_dormant ORDER BY is_dormant;

-- naplnenie vekových pásiem
SELECT age_band, COUNT(*) AS pocet
FROM lh_silver.ml_iam_dormancy
GROUP BY age_band ORDER BY pocet DESC;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

CREATE OR REPLACE TABLE lh_silver.ml_iam_dormancy_train AS
SELECT segment, edesk_status, upvs_status, source,
       edesk_cuet_enabled, account_age_days, age_band, is_dormant
FROM lh_silver.ml_iam_dormancy;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
