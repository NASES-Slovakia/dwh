# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d0e1fd8d-fbff-45b7-bd17-08baf9ef152b",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "1c078226-a52a-40bd-8d62-4dca8f37f913",
# META       "known_lakehouses": [
# META         {
# META           "id": "d0e1fd8d-fbff-45b7-bd17-08baf9ef152b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# **Manualne ciselniky**
# 
# Vytvára tabulky z manuálneho vstupu v podobe excelu. Ten sa musi nachadzat vo foldry:
# /lakehouse/default/Files/manual_inputs/
# 
# aktualne spracovava nasledovne excely:
# - Sk-Talk_3-Zoznam_Class.xlsx
# - Sk-Talk-Class-OR_SR.xlsx
# - Zoznam URI s priradenym rezortom.xlsx
# - UPVS_Error_Codes_List_External_v1_66__MED_20251222.xlsx
# 


# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
#vytvaranie manual_zoznam_class_sktalk_3

RUN_LOAD = False   # prepneš na True keď treba znovu načítať Excel

if RUN_LOAD:
    import pandas as pd
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    # dtype=str => všetko číta ako string, žiadna implicitná inferencia
    pdf = pd.read_excel(
        "/lakehouse/default/Files/manual_inputs/Sk-Talk_3-Zoznam_Class.xlsx",
        sheet_name="Sk-Talk Class",
        dtype=str,
    ).fillna("")

    # EXPLICITNÁ schéma – kľúčové, viď nižšie
    schema = StructType([
        StructField("class",     StringType(),  True),
        StructField("popis",   StringType(),  True),
    ])

    # cast pred createDataFrame, keď máš iné typy než string
   # pdf["hodnota"] = pdf["hodnota"].replace("", None).astype("Int64")

    sdf = spark.createDataFrame(pdf, schema=schema)
    sdf.write.mode("overwrite").format("delta").saveAsTable("lh_bronze.manual_zoznam_class_sktalk_3")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#vytvaranie manual_zoznam_class_sktalk_2

RUN_LOAD = True   # prepneš na True keď treba znovu načítať Excel

if RUN_LOAD:
    import pandas as pd
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    # dtype=str => všetko číta ako string, žiadna implicitná inferencia
    pdf = pd.read_excel(
        "/lakehouse/default/Files/manual_inputs/Sk-Talk-Class-OR_SR.xlsx",
        sheet_name="Classes",
        dtype=str,
    ).fillna("")

    # EXPLICITNÁ schéma – kľúčové, viď nižšie
    schema = StructType([
        StructField("class",     StringType(),  True),
        StructField("popis",   StringType(),  True),
    ])

    # cast pred createDataFrame, keď máš iné typy než string
   # pdf["hodnota"] = pdf["hodnota"].replace("", None).astype("Int64")

    sdf = spark.createDataFrame(pdf, schema=schema)
    sdf.write.mode("overwrite").format("delta").saveAsTable("lh_bronze.manual_zoznam_class_sktalk_2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#vytvaranie manual_zoznam_uri_rezort

RUN_LOAD = True   # prepneš na True keď treba znovu načítať Excel

if RUN_LOAD:
    import pandas as pd
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    # dtype=str => všetko číta ako string, žiadna implicitná inferencia
    pdf = pd.read_excel(
        "/lakehouse/default/Files/manual_inputs/Zoznam URI s priradenym rezortom.xlsx",
        sheet_name="Hárok1",
        dtype=str,
    ).fillna("")

    # EXPLICITNÁ schéma – kľúčové, viď nižšie
    schema = StructType([
        StructField("uri",     StringType(),  True),
        StructField("rezort",   StringType(),  True),
    ])

    # cast pred createDataFrame, keď máš iné typy než string
   # pdf["hodnota"] = pdf["hodnota"].replace("", None).astype("Int64")

    sdf = spark.createDataFrame(pdf, schema=schema)
    sdf.write.mode("overwrite").format("delta").saveAsTable("lh_bronze.manual_zoznam_uri_rezort")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#vytvaranie manual_zoznam_error_ciselnik_g2g

RUN_LOAD = True   # prepneš na True keď treba znovu načítať Excel

if RUN_LOAD:
    import pandas as pd
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    # dtype=str => všetko číta ako string, žiadna implicitná inferencia
    pdf = pd.read_excel(
        "/lakehouse/default/Files/manual_inputs/UPVS_Error_Codes_List_External_v1_66__MED_20251222.xlsx",
        sheet_name="G2G",
        dtype=str,
    ).fillna("")

    # EXPLICITNÁ schéma – kľúčové, viď nižšie
    schema = StructType([
        StructField("a",     StringType(),  True),
        StructField("b",   StringType(),  True),
        StructField("c",     StringType(),  True),
        StructField("kod_chyby",     StringType(),  True),
        StructField("e",     StringType(),  True),
        StructField("f",     StringType(),  True),
        StructField("popis_chyby",     StringType(),  True),
    ])

    # cast pred createDataFrame, keď máš iné typy než string
   # pdf["hodnota"] = pdf["hodnota"].replace("", None).astype("Int64")

    sdf = spark.createDataFrame(pdf, schema=schema)
    sdf.write.mode("overwrite").format("delta").saveAsTable("lh_bronze.manual_zoznam_error_ciselnik_g2g")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
