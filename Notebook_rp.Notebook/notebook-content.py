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

# CELL ********************

import pandas as pd
from datetime import datetime

# ✅ jeden timestamp pre celý run (konzistentné)
run_timestamp = datetime.now()
run_timestamp_str = run_timestamp.strftime("%Y-%m-%d %H:%M:%S")
run_date_str = run_timestamp.strftime("%Y-%m-%d")
run_id_str = run_timestamp.strftime("%Y%m%d_%H%M%S")  # použijeme aj do názvu súboru

schemas_df = spark.sql("SHOW DATABASES")
schemas = [r[0] for r in schemas_df.collect() if str(r[0]).startswith("lh_")]

tables = []
for s in schemas:
    tdf = spark.sql(f"SHOW TABLES IN {s}")
    tables += [(s, r.tableName) for r in tdf.collect()]

results = []

total = len(tables)
print(f"[{run_timestamp_str}] Nájdených tabuliek na profiling: {total}")

for i, (schema_name, table_name) in enumerate(tables, start=1):
    full_table_name = f"{schema_name}.{table_name}"
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ({i}/{total}) Profilujem tabuľku: {full_table_name}")

    df = spark.table(full_table_name)

    dtype_map = {name: dtype for name, dtype in df.dtypes}

    for col in df.columns:
        stats = df.selectExpr(
            f"'{run_timestamp_str}' as run_timestamp",  # ✅ timestamp behu
            f"'{run_date_str}' as run_date",            # ✅ dátum behu (slicer)
            f"'{schema_name}' as schema_name",
            f"'{table_name}' as table_name",
            f"'{full_table_name}' as full_table_name",
            f"'{col}' as column_name",
            f"'{dtype_map[col]}' as data_type",
            "count(1) as row_count",
            f"count(`{col}`) as non_nulls",
            f"count(1) - count(`{col}`) as nulls",
            f"approx_count_distinct(`{col}`) as distinct_count",
            f"min(`{col}`) as min_value",
            f"max(`{col}`) as max_value"
        ).toPandas()

        results.append(stats)

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Profiling dokončený, spájam výsledky...")

profile_pd = pd.concat(results, ignore_index=True)

# min/max môžu byť rôznych typov (string/struct/...), zjednoť na string
profile_pd["min_value"] = profile_pd["min_value"].astype(str)
profile_pd["max_value"] = profile_pd["max_value"].astype(str)

profile_spark = spark.createDataFrame(profile_pd)

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Ukladám výsledok do Delta tabuľky: profile_report")

# ✅ export do Excelu s timestampom
output_path = f"/lakehouse/default/Files/profiling/profiling_report_{run_id_str}.xlsx"
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Exportujem do Excelu: {output_path}")
profile_pd.to_excel(output_path, index=False)

# ✅ odporúčam APPEND (kvôli trendom), ale nechám aj OVERWRITE variant
# Ak chceš historizáciu profilov v čase, daj mode("append") a nepridávaj overwriteSchema.
profile_spark.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("profile_report")


print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Hotovo ✅")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE profile_report
# MAGIC USING delta
# MAGIC AS SELECT * FROM profile_temp;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_bronze.cnm_notification LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
