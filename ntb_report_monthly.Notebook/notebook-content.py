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

# # ntb_report_monthly  –  v2.0  (CSV + Delta)
# 
# Tento notebook:
# 1. Vygeneruje reporty zo zdrojových tabuliek (SQL → Spark DataFrame)
# 2. Uloží **CSV súbory** do `Files/reports/{report_month}/` (zachovaná kompatibilita)
# 3. Zapíše dáta do **Delta tabuliek** particionovaných podľa `report_month`
# 
# **Power BI napojenie:** Fabric Workspace → Lakehouse → SQL Endpoint → New Report

# PARAMETERS CELL ********************

# ============================================================================
# CELL 1 – Parametre  (tag 'parameters' → Fabric pipeline môže override-ovať)
# ============================================================================
from pyspark.sql import SparkSession, functions as F
from datetime import date
from dateutil.relativedelta import relativedelta
import os
import pandas as pd

spark = SparkSession.builder.getOrCreate()

# Dynamic partition overwrite: prepíše len partition(y) prítomné v DataFrame
# → re-run pre rovnaký mesiac je bezpečný, staršie mesiace zostanú nedotknuté
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Defaultne: predchádzajúci mesiac (Fabric pipeline môže poslať iné hodnoty)
report_date  = date.today().replace(day=1) - relativedelta(months=1)
report_month = report_date.strftime("%Y-%m")

#report_month = "2026-07"   # simulovaný predchádzajúci mesiac

# posledný deň report_month ako YYYYMMDD (int)
# relativedelta(day=31) sa oreže na reálny posledný deň mesiaca
last_day  = report_date + relativedelta(day=31)
batch_day = int(last_day.strftime("%Y%m%d"))

batch_day = 20260730   # manuálny override

#najnovšia reálne načítaná dávka v bronze.
#batch_day = spark.sql(
#     "SELECT max(_batch_day) AS b FROM lh_bronze.iam_t_usr"
#).first()["b"]


print(f"report_month : {report_month}")
print(f"batch_day    : {batch_day}")


# ── Delta konfigurácia ───────────────────────────────────────────────────────
# DELTA_LAKEHOUSE: názov Lakehouse kde budú uložené reportové tabuľky
#   → Ak máš oddelenú Gold vrstvu (napr. lh_gold), zmeň tu
#   → None = použije sa default Lakehouse notebooku
DELTA_LAKEHOUSE = "lh_bronze"   # ← zmeň na "lh_gold" ak existuje gold lakehouse
TABLE_PREFIX    = "rpt_"        # prefix odlišuje reporty od zdrojových tabuliek

print(f"Delta cieľ    : {DELTA_LAKEHOUSE}.{TABLE_PREFIX}<report_name>")
print(f"CSV cieľ      : /lakehouse/default/Files/reports/{report_month}/")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 2 – Definícia reportov  (neupravovať štruktúru, len pridávať reporty)
# ============================================================================---------------
REPORTS = [

    # ── 1 ──────────────────────────────────────────────────────────────────
    {
        "name": "01_iam_zoznam_elektronickych_schranok_po_aktivovanych_na_dorucovanie", 
        "sql": """
            WITH latest AS (
                SELECT edesk_id, MAX(import_date) AS max_import_date
                FROM lh_bronze.iam_t_usr
                WHERE _batch_day = {batch_day}
                GROUP BY edesk_id
            )
            SELECT distinct
                iam_t_usr.edesk_id         AS CISLO_SCHRANKY,
                iam_t_usr.ico,
                iam_t_usr.uri,
                iam_t_usr.display_name     AS ZOBRAZOVANY_NAZOV,
                iam_t_usr.city_id as OBEC_SUSR_0025
            FROM lh_bronze.iam_t_usr AS iam_t_usr
            JOIN latest
                ON iam_t_usr.edesk_id = latest.edesk_id
                AND iam_t_usr.import_date = latest.max_import_date
            WHERE iam_t_usr._batch_day = {batch_day}
            AND iam_t_usr.identity_type IN (2, 6)
            AND iam_t_usr.edesk_status = 'DELIVERABLE'
            AND iam_t_usr.upvs_status = 'ACTIVATED'
        """
    },

    # ── 2 ──────────────────────────────────────────────────────────────────
    {
        "name": "02_iam_zoznam_elektronickych_schranok_fo_aktivovanych_na_dorucovanie",
        "sql": """
            WITH latest AS (
                SELECT edesk_id, MAX(import_date) AS max_import_date
                FROM lh_bronze.iam_t_usr
                WHERE _batch_day = {batch_day}
                GROUP BY edesk_id
            )
            SELECT distinct
                iam_t_usr.edesk_id         AS CISLO_SCHRANKY,
                iam_t_usr.city_id as OBEC_SUSR_0025
            FROM lh_bronze.iam_t_usr AS iam_t_usr
            JOIN latest
                ON iam_t_usr.edesk_id = latest.edesk_id
                AND iam_t_usr.import_date = latest.max_import_date
            WHERE iam_t_usr._batch_day = {batch_day}
            AND iam_t_usr.identity_type IN (1)
            AND iam_t_usr.edesk_status = 'DELIVERABLE'
            AND iam_t_usr.upvs_status = 'ACTIVATED'
        """
    },

    # ── 3 ──────────────────────────────────────────────────────────────────
    {
        "name": "03_iam_zoznam_ovm_so_schrankou_ktore_maju_matersku_identitu",
        "sql": """
            WITH latest AS (
                SELECT edesk_id, MAX(import_date) AS max_import_date
                FROM lh_bronze.iam_t_usr
                WHERE _batch_day = {batch_day}
                GROUP BY edesk_id
            )
            SELECT distinct
                iam_t_usr.ico,
                iam_t_usr.suffix,
                iam_t_usr.display_name AS nazov,
                iam_t_usr.uri
            FROM lh_bronze.iam_t_usr AS iam_t_usr
            JOIN latest
                ON iam_t_usr.edesk_id = latest.edesk_id
                AND iam_t_usr.import_date = latest.max_import_date
            LEFT JOIN lh_bronze.metais_upvsiam_001 AS metais_upvsiam_001
                ON iam_t_usr.identity_type = metais_upvsiam_001.code
                AND metais_upvsiam_001._batch_day = {batch_day}
            WHERE iam_t_usr._batch_day = {batch_day}
            AND iam_t_usr.identity_type IN (4, 5, 6)
            AND iam_t_usr.edesk_status = 'DELIVERABLE'
            AND iam_t_usr.parent_id IS NOT NULL
            AND iam_t_usr.upvs_status = 'ACTIVATED'
        """
    },

    # ── 4 ──────────────────────────────────────────────────────────────────
    {
        "name": "04_iam_zoznam_ovm_s_aktivovanou_elektronickou_schrankou",
        "sql": """
            WITH latest AS (
                SELECT edesk_id, MAX(import_date) AS max_import_date
                FROM lh_bronze.iam_t_usr
                WHERE _batch_day = {batch_day}
                GROUP BY edesk_id
            )
            SELECT distinct
                iam_t_usr.ico,
                iam_t_usr.uri,
                iam_t_usr.display_name AS nazov
            FROM lh_bronze.iam_t_usr AS iam_t_usr
            JOIN latest
                ON iam_t_usr.edesk_id = latest.edesk_id
                AND iam_t_usr.import_date = latest.max_import_date
            LEFT JOIN lh_bronze.metais_upvsiam_001 AS metais_upvsiam_001
                ON iam_t_usr.identity_type = metais_upvsiam_001.code
                AND metais_upvsiam_001._batch_day = {batch_day}
            WHERE iam_t_usr._batch_day = {batch_day}
            AND iam_t_usr.identity_type IN (4, 5, 6)
            AND iam_t_usr.edesk_status = 'DELIVERABLE'
            AND iam_t_usr.upvs_status = 'ACTIVATED'
        """
    },

    # ── 5 ──────────────────────────────────────────────────────────────────
    {
        "name": "05_iam_zoznam_vsetkych_elektronickych_schranok_ovm",
        "sql": """
            WITH latest AS (
                SELECT edesk_id, MAX(import_date) AS max_import_date
                FROM lh_bronze.iam_t_usr
                WHERE _batch_day = {batch_day}
                GROUP BY edesk_id
            )
            SELECT distinct
                iam_t_usr.ico,
                iam_t_usr.uri,
                iam_t_usr.display_name AS nazov
            FROM lh_bronze.iam_t_usr AS iam_t_usr
            JOIN latest
                ON iam_t_usr.edesk_id = latest.edesk_id
                AND iam_t_usr.import_date = latest.max_import_date
            LEFT JOIN lh_bronze.metais_upvsiam_001 AS metais_upvsiam_001
                ON iam_t_usr.identity_type = metais_upvsiam_001.code
                AND metais_upvsiam_001._batch_day = {batch_day}
            WHERE iam_t_usr._batch_day = {batch_day}
            AND iam_t_usr.identity_type IN (4, 5, 6)
            AND iam_t_usr.first_login IS NOT NULL
        """
    },

    # ── 6 ──────────────────────────────────────────────────────────────────
    {
        "name": "06_iam_pocet_zriadenych_elektronickych_schranok",
        "sql": """
            WITH latest AS (
                SELECT edesk_id, MAX(import_date) AS max_import_date
                FROM lh_bronze.iam_t_usr
                WHERE _batch_day = {batch_day}
                GROUP BY edesk_id
            )
            SELECT 
                CASE
                    WHEN iam_t_usr.identity_type = 1        THEN 'FO'
                    WHEN iam_t_usr.identity_type IN (2)     THEN 'PO'
                    WHEN iam_t_usr.identity_type IN (4,5,6) THEN 'OVM'
                    ELSE 'OTHER'
                END AS TYP_IDENTITY,
                COUNT(*) AS POCET
            FROM lh_bronze.iam_t_usr AS iam_t_usr
            JOIN latest
                ON iam_t_usr.edesk_id = latest.edesk_id
                AND iam_t_usr.import_date = latest.max_import_date
            LEFT JOIN lh_bronze.metais_upvsiam_001 AS metais_upvsiam_001
                ON iam_t_usr.identity_type = metais_upvsiam_001.code
                AND metais_upvsiam_001._batch_day = {batch_day}
            WHERE iam_t_usr._batch_day = {batch_day}
            AND iam_t_usr.identity_type IN (1, 2, 4, 5, 6)
            AND iam_t_usr.edesk_status = 'DELIVERABLE'
            AND iam_t_usr.upvs_status = 'ACTIVATED'
            GROUP BY
                CASE
                    WHEN iam_t_usr.identity_type = 1        THEN 'FO'
                    WHEN iam_t_usr.identity_type IN (2)     THEN 'PO'
                    WHEN iam_t_usr.identity_type IN (4,5,6) THEN 'OVM'
                    ELSE 'OTHER'
                END
        """
    },

    # ── 7 ──────────────────────────────────────────────────────────────────
    {
        "name": "07_iam_pocet_schranok_po_podla_zdrojovych_registrov",
        "sql": """
            WITH latest AS (
                SELECT edesk_id, MAX(import_date) AS max_import_date
                FROM lh_bronze.iam_t_usr
                WHERE _batch_day = {batch_day}
                GROUP BY edesk_id
            )
            SELECT
                iam_t_usr.source AS ZDROJ,
                COUNT(*) AS POCET
            FROM lh_bronze.iam_t_usr AS iam_t_usr
            JOIN latest
                ON iam_t_usr.edesk_id = latest.edesk_id
                AND iam_t_usr.import_date = latest.max_import_date
            LEFT JOIN lh_bronze.metais_upvsiam_001 AS metais_upvsiam_001
                ON iam_t_usr.identity_type = metais_upvsiam_001.code
                AND metais_upvsiam_001._batch_day = {batch_day}
            WHERE iam_t_usr._batch_day = {batch_day}
            AND iam_t_usr.identity_type IN (2, 6)
            AND iam_t_usr.edesk_status = 'DELIVERABLE'
            AND iam_t_usr.upvs_status = 'ACTIVATED'
            GROUP BY iam_t_usr.source
        """
    },

    # ── 8 ──────────────────────────────────────────────────────────────────
    {
        "name": "08_iam_pocet_aktivovanych_schranok_na_dorucovanie",
        "sql": """
            WITH latest AS (
                SELECT edesk_id, MAX(import_date) AS max_import_date
                FROM lh_bronze.iam_t_usr
                WHERE _batch_day = {batch_day}
                GROUP BY edesk_id
            )
            SELECT 
                CASE
                    WHEN iam_t_usr.identity_type = 1        THEN 'FO'
                    WHEN iam_t_usr.identity_type IN (2)     THEN 'PO'
                    WHEN iam_t_usr.identity_type IN (4, 6)  THEN 'OVM'
                    ELSE 'OTHER'
                END AS TYP_IDENTITY,
                COUNT(*) AS POCET
            FROM lh_bronze.iam_t_usr AS iam_t_usr
            JOIN latest
                ON iam_t_usr.edesk_id = latest.edesk_id
                AND iam_t_usr.import_date = latest.max_import_date
            LEFT JOIN lh_bronze.metais_upvsiam_001 AS metais_upvsiam_001
                ON iam_t_usr.identity_type = metais_upvsiam_001.code
                AND metais_upvsiam_001._batch_day = {batch_day}
            WHERE iam_t_usr._batch_day = {batch_day}
            AND iam_t_usr.identity_type IN (1, 2, 4, 5, 6)
            AND iam_t_usr.edesk_status = 'DELIVERABLE'
            AND iam_t_usr.upvs_status = 'ACTIVATED'
            GROUP BY
                CASE
                    WHEN iam_t_usr.identity_type = 1        THEN 'FO'
                    WHEN iam_t_usr.identity_type IN (2)     THEN 'PO'
                    WHEN iam_t_usr.identity_type IN (4, 6)  THEN 'OVM'
                    ELSE 'OTHER'
                END
        """
    },

    # ── 9 ──────────────────────────────────────────────────────────────────
    {
        "name": "09_iam_advokati",
        "sql": """
            SELECT URI, EDESKID, DISPLAY_NAME, EDESKSTATUS
            FROM (
                SELECT
                    uri          AS URI,
                    edesk_id     AS EDESKID,
                    display_name AS DISPLAY_NAME,
                    edesk_status AS EDESKSTATUS,
                    ROW_NUMBER() OVER (PARTITION BY edesk_id ORDER BY import_date DESC) AS rn
                FROM lh_bronze.iam_t_usr
                WHERE LOWER(display_name) LIKE '%advok%'
                AND _batch_day = {batch_day}
            ) t
            WHERE rn = 1
        """
    },

# ── 10 ─────────────────────────────────────────────────────────────────
    {
        "name": "10_cud_zoznam_sprav_s_chybou_pri_tlaci",
        "sql": """
            SELECT
                cud_report_printinfo.Id                    AS id_spravy,
                cud_report_printinfo.PrintInfoReceivedTime AS datum_prijatia_na_tlac,
                cud_report_final.SenderUri                 AS uri,
                iam_usr.display_name                       AS Nazov,
                cud_report_printinfo.Type                  AS stav,
                cud_report_final.State,
                cud_report_final.ErrorCode                 AS kod_chyby,
                cud_report_final.ErrorDetails              AS chybova_sprava,
                cud_report_printinfo.RejectedDetails       AS detail_chyby
            FROM lh_bronze.cud_report_printinfo AS cud_report_printinfo
            LEFT JOIN lh_bronze.cud_report_final AS cud_report_final
                ON cud_report_printinfo.Id = cud_report_final.Id
                and cud_report_printinfo._batch_day = cud_report_final._batch_day
            LEFT JOIN (
                SELECT uri, display_name
                FROM (
                    SELECT
                        uri,
                        display_name,
                        ROW_NUMBER() OVER (PARTITION BY uri ORDER BY import_date DESC) AS rn
                    FROM lh_bronze.iam_t_usr
                    WHERE _batch_day = {batch_day}                          -- IAM: 1 denný snapshot
                )
                WHERE rn = 1
            ) AS iam_usr
                ON iam_usr.uri = cud_report_final.SenderUri
            WHERE cud_report_final.ErrorCode IN (
                '22000001','22000004','21100999','21000018','22000006'
            )
              AND cud_report_printinfo.Type = 'rejected'
              AND FLOOR(cud_report_printinfo._batch_day / 100) = FLOOR({batch_day} / 100)    -- CUD: celý mesiac inkrementov
        """
    },

# ── 11 ─────────────────────────────────────────────────────────────────
    {
        "name": "11_cud_zoznam_chybnych_sprav",
        "sql": """
            SELECT
                cud_report_final.Id              AS ID_spravy,
                cud_report_final.State              AS Stav,
                cud_report_final.FinalNotificationSentTime AS datum_prijatia_do_cud,
                cud_report_final.SenderUri          AS URI,
                iam_usr.display_name                AS Nazov,
                cud_report_final.ErrorInState       AS Chybovy_stav,
                cud_report_final.ErrorCode          AS Kod_chyby,
                cud_report_final.ErrorDetails       AS Chybova_sprava
            FROM lh_bronze.cud_report_final AS cud_report_final
            LEFT JOIN (
                SELECT uri, display_name
                FROM (
                    SELECT
                        uri,
                        display_name,
                        ROW_NUMBER() OVER (PARTITION BY uri ORDER BY import_date DESC) AS rn
                    FROM lh_bronze.iam_t_usr
                    WHERE _batch_day = {batch_day}                          -- IAM: 1 denný snapshot
                )
                WHERE rn = 1
            ) AS iam_usr
                ON iam_usr.uri = cud_report_final.SenderUri
            WHERE cud_report_final.State = 101
            AND FLOOR(cud_report_final._batch_day / 100) = FLOOR({batch_day} / 100)    -- CUD: celý mesiac inkrementov
        """
    },

# ── 12 ─────────────────────────────────────────────────────────────────
    {
        "name": "12_cud_pocet_sprav_zaslanych_na_tlac_podla_sender",
        "sql": """
            SELECT
                FLOOR({batch_day} / 100)           AS Mesiac,
                cud_report_senttosp.SenderUri      AS URI,
                iam_usr.display_name               AS Nazov,
                count(*)                           AS Pocet
            FROM lh_bronze.cud_report_senttosp AS cud_report_senttosp
            LEFT JOIN (
                SELECT uri, display_name
                FROM (
                    SELECT
                        uri,
                        display_name,
                        ROW_NUMBER() OVER (PARTITION BY uri ORDER BY import_date DESC) AS rn
                    FROM lh_bronze.iam_t_usr
                    WHERE _batch_day = {batch_day}                          -- IAM: 1 denný snapshot
                )
                WHERE rn = 1
            ) AS iam_usr
                ON iam_usr.uri = cud_report_senttosp.SenderUri
            where FLOOR(cud_report_senttosp._batch_day / 100) = FLOOR({batch_day} / 100)    -- CUD: celý mesiac inkrementov 
            group by  FLOOR({batch_day} / 100), cud_report_senttosp.SenderUri, iam_usr.display_name
            order by Pocet
        """
    },

# ── 13 ─────────────────────────────────────────────────────────────────
    {
        "name": "13_cud_pocet_sprav_vytlacenych_podla_sender",
        "sql": """
            WITH SENDER_URI AS (
                SELECT Id, SenderUri FROM lh_bronze.cud_report_final
                UNION
                SELECT Id, SenderUri FROM lh_bronze.cud_report_senttosp
                    )
            SELECT
                FLOOR({batch_day} / 100)                   AS Mesiac,
                SENDER_URI.SenderUri                       AS URI,
                iam_usr.display_name                       AS Nazov,
                cud_report_printinfo.Type                  AS Stav,
                count(*)                                   AS Pocet
            FROM lh_bronze.cud_report_printinfo AS cud_report_printinfo
            LEFT JOIN SENDER_URI
                ON cud_report_printinfo.Id = SENDER_URI.Id
            LEFT JOIN (
                SELECT uri, display_name
                FROM (
                    SELECT
                        uri,
                        display_name,
                        ROW_NUMBER() OVER (PARTITION BY uri ORDER BY import_date DESC) AS rn
                    FROM lh_bronze.iam_t_usr
                    WHERE _batch_day = {batch_day}                          -- IAM: 1 denný snapshot
                )
                WHERE rn = 1
            ) AS iam_usr
                ON iam_usr.uri = SENDER_URI.SenderUri
            where FLOOR(cud_report_printinfo._batch_day / 100) = FLOOR({batch_day} / 100)    -- CUD: celý mesiac inkrementov 
            group by  FLOOR({batch_day} / 100), SENDER_URI.SenderUri, iam_usr.display_name, cud_report_printinfo.Type
            order by Pocet            
        """
    },

    # ── 19 ─────────────────────────────────────────────────────────────────
    {
        "name": "19_edesk_pocet_prihlaseni_do_schranok_za_mesiac",
        "sql": """
            WITH edesk_logins AS (
                SELECT
                    edesk.edesk_owner_upvs_id,
                    CAST(edesk.created_at AS date) AS created_at
                FROM lh_bronze.edesk_source_edesk_dwh AS edesk
                WHERE edesk.type = 100                                              -- 100 = Prihlásenie do schránky
                  AND FLOOR(edesk._batch_day / 100) = FLOOR({batch_day} / 100)      -- eDesk: celý mesiac denných prírastkov
            ),

            iam_dedup AS (
                SELECT sifo, identity_type
                FROM (
                    SELECT
                        sifo,
                        identity_type,
                        ROW_NUMBER() OVER (PARTITION BY sifo ORDER BY import_date DESC) AS rn
                    FROM lh_bronze.iam_t_usr
                    WHERE _batch_day = {batch_day}                                  -- IAM: 1 denný snapshot
                ) a
                WHERE rn = 1
            ),

            joined AS (
                SELECT
                    edesk.created_at,
                    edesk.edesk_owner_upvs_id,
                    iam.identity_type
                FROM edesk_logins AS edesk
                LEFT JOIN iam_dedup AS iam
                    ON iam.sifo = edesk.edesk_owner_upvs_id
            )

            SELECT
                date_format(created_at, 'd.M.yyyy') AS datum,
                COUNT(DISTINCT CASE WHEN identity_type = 1        THEN edesk_owner_upvs_id END)   AS FO,
                COUNT(DISTINCT CASE WHEN identity_type = 2        THEN edesk_owner_upvs_id END)    AS PO,
                COUNT(DISTINCT CASE WHEN identity_type IN (4,6) THEN edesk_owner_upvs_id END)   AS IVS,
                COUNT(CASE WHEN identity_type = 1        THEN 1 END)                              AS unikatne_prih_spolu_FO, 
                COUNT(CASE WHEN identity_type = 2        THEN 1 END)                             AS unikatne_prih_spolu_PO,
                COUNT(CASE WHEN identity_type IN (4,6) THEN 1 END)                              AS unikatne_prih_spolu_IVS
            FROM joined
            GROUP BY created_at
            ORDER BY created_at
        """
    },

    # ── 20 ─────────────────────────────────────────────────────────────────
    # Lokátor služieb  →  DataSet/ZoznamESaF_{report_month}_utf8
    {
        "name": "20_ls_zoznam_esaf",
        "sql": """
            SELECT
                ls_serviceinstance.id                  AS IdServiceInstanceId,
                ls_serviceinstance.external_code       AS ExternalCode,
                ls_servicelocalization.name            AS ServiceName,
                ls_service.service_type_id             AS ServiceType,
                ls_institution.uri                     AS ExtId,
                ls_institutionlocalization.name        AS InstitutionName,
                ls_service.published_from              AS ValidFrom,
                ls_service.published_to                AS ValidTo,
                ls_servicelocalization.service_url     AS ServiceUrl,
                ls_servicelocalization.information_url AS ServiceInfoURL,
                ls_service.last_modified_at            AS LastUpdated,
                ls_form.form_identifier                AS FormURL
            FROM lh_bronze.ls_serviceinstance AS ls_serviceinstance
            LEFT JOIN lh_bronze.ls_institutionlocalization AS ls_institutionlocalization
                ON ls_serviceinstance.institution_id = ls_institutionlocalization.institution_id
                AND ls_serviceinstance._batch_day = ls_institutionlocalization._batch_day
            LEFT JOIN lh_bronze.ls_institution AS ls_institution
                ON ls_institutionlocalization.institution_id = ls_institution.id
                AND ls_institutionlocalization._batch_day = ls_institution._batch_day
            LEFT JOIN lh_bronze.ls_service AS ls_service
                ON ls_serviceinstance.service_id = ls_service.id
                AND ls_serviceinstance._batch_day = ls_service._batch_day
            LEFT JOIN lh_bronze.ls_servicelocalization AS ls_servicelocalization
                ON ls_serviceinstance.id = ls_servicelocalization.service_instance_id
                AND ls_servicelocalization.language = 1
                AND ls_serviceinstance._batch_day = ls_servicelocalization._batch_day
            LEFT JOIN lh_bronze.ls_assignedform AS ls_assignedform
                ON ls_assignedform.service_instance_id = ls_serviceinstance.id
                AND ls_serviceinstance._batch_day = ls_assignedform._batch_day
            LEFT JOIN lh_bronze.ls_form AS ls_form
                ON ls_form.id = ls_assignedform.form_id
                AND ls_assignedform._batch_day = ls_form._batch_day
            WHERE ls_serviceinstance._batch_day = {batch_day}
        """
    },

    # ── 21_ls_zoznam_institucii_so_zriadenou_sluzbou_vseobecna_agenda
    {
        "name": "21_ls_zoznam_institucii_so_zriadenou_sluzbou_vseobecna_agenda",
        "sql": """
            SELECT
                ls_institutionlocalization.name    AS Nazov,
                ls_institution.uri                 AS Uri,
                ls_servicelocalization.service_url AS ServiceUrl
            FROM lh_bronze.ls_serviceinstance AS ls_serviceinstance
            LEFT JOIN lh_bronze.ls_institutionlocalization AS ls_institutionlocalization
                ON ls_serviceinstance.institution_id = ls_institutionlocalization.institution_id
                AND ls_serviceinstance._batch_day = ls_institutionlocalization._batch_day
            LEFT JOIN lh_bronze.ls_institution AS ls_institution
                ON ls_institutionlocalization.institution_id = ls_institution.id
                AND ls_institutionlocalization._batch_day = ls_institution._batch_day
            LEFT JOIN lh_bronze.ls_servicelocalization AS ls_servicelocalization
                ON ls_serviceinstance.id = ls_servicelocalization.service_instance_id
                AND ls_servicelocalization.language = 1
                AND ls_serviceinstance._batch_day = ls_servicelocalization._batch_day
            WHERE ls_serviceinstance.external_code = 'App.GeneralAgenda'
              AND ls_serviceinstance._batch_day = {batch_day}
        """
    },

    # ── 23 – UPVS: počty transakcií podľa ostatných class (mimo SKTALK2/SKTALK3) ─
    {
        "name": "23_upvs_pocty_transakcii_ostatne_class",
        "sql": """
            SELECT
                g.class                          AS CLASS,
                COUNT(DISTINCT g.message_id)     AS count_distinct_message_id
            FROM lh_bronze.g2g_source_dwh20_g2g_messages_received AS g
            LEFT JOIN (SELECT DISTINCT class FROM lh_bronze.manual_zoznam_class_sktalk_2) AS s2
                   ON g.class = s2.class
            LEFT JOIN (SELECT DISTINCT class FROM lh_bronze.manual_zoznam_class_sktalk_3) AS s3
                   ON g.class = s3.class
            WHERE FLOOR(g._batch_day / 100) = FLOOR({batch_day} / 100)
              AND g.class IS NOT NULL
              AND s2.class IS NULL      -- nie je v SKTALK2
              AND s3.class IS NULL      -- nie je v SKTALK3
            GROUP BY g.class
            ORDER BY 1
        """
    },

    # ── 24 – UPVS: počty transakcií podľa main class
    {
        "name": "24_upvs_pocty_transakcii_podla_class",
        "sql": """
            SELECT
                CASE
                    WHEN g.class IN (
                        'EDESK_SAVE_APPLICATION_TO_DRAFTS',
                        'EDESK_SAVE_APPLICATION_TO_OUTBOX',
                        'EGOV_APPLICATION',
                        'EGOV_DOCUMENT',
                        'EGOV_NOTIFICATION') THEN g.class
                    WHEN s2.class IS NOT NULL THEN 'SKTALK2'
                    WHEN s3.class IS NOT NULL THEN 'Ostatne_SKTALK3'
                    ELSE g.class
                END                                     AS CLASS,
                COUNT(DISTINCT g.message_id)            AS count_distinct_message_id
            FROM lh_bronze.g2g_source_dwh20_g2g_messages_received AS g
            LEFT JOIN (SELECT DISTINCT class FROM lh_bronze.manual_zoznam_class_sktalk_2) AS s2
                   ON g.class = s2.class
            LEFT JOIN (SELECT DISTINCT class FROM lh_bronze.manual_zoznam_class_sktalk_3) AS s3
                   ON g.class = s3.class
            WHERE FLOOR(g._batch_day / 100) = FLOOR({batch_day} / 100)
              AND (
                    g.class IN (
                        'EDESK_SAVE_APPLICATION_TO_DRAFTS',
                        'EDESK_SAVE_APPLICATION_TO_OUTBOX',
                        'EGOV_APPLICATION',
                        'EGOV_DOCUMENT',
                        'EGOV_NOTIFICATION')
                    OR s2.class IS NOT NULL
                    OR s3.class IS NOT NULL
                  )
            GROUP BY
                CASE
                    WHEN g.class IN (
                        'EDESK_SAVE_APPLICATION_TO_DRAFTS',
                        'EDESK_SAVE_APPLICATION_TO_OUTBOX',
                        'EGOV_APPLICATION',
                        'EGOV_DOCUMENT',
                        'EGOV_NOTIFICATION') THEN g.class
                    WHEN s2.class IS NOT NULL THEN 'SKTALK2'
                    WHEN s3.class IS NOT NULL THEN 'Ostatne_SKTALK3'
                    ELSE g.class
                END
            order by 1
        """
    },

    # ── 25 – UPVS: počty transakcií podľa senderov ──────────────────
    {
        "name": "25_upvs_pocty_transakcii_podla_senderov",
        "sql": """
            SELECT
                case when s2.rezort is null then u.display_name else s2.rezort   end  AS rezort,
                COUNT(DISTINCT g.message_id)         AS count_distinct_message_id
            FROM lh_bronze.g2g_source_dwh20_g2g_messages_received AS g
            LEFT JOIN (SELECT DISTINCT rezort, uri FROM lh_bronze.manual_zoznam_uri_rezort) AS s2
                   ON g.sender_id = s2.uri
            INNER JOIN (
                SELECT uri, display_name
                FROM (
                    SELECT
                        uri,
                        display_name, 
                        identity_type,
                        ROW_NUMBER() OVER (PARTITION BY uri ORDER BY import_date DESC) AS rn
                    FROM lh_bronze.iam_t_usr
                    WHERE _batch_day = {batch_day}          -- IAM: 1 denný snapshot
                ) t
                WHERE rn = 1
                  AND identity_type IN (2, 4, 6)
            ) AS u
                ON u.uri = g.sender_id
            WHERE FLOOR(g._batch_day / 100) = FLOOR({batch_day} / 100)   -- G2G: celý mesiac
            GROUP BY case when s2.rezort is null then u.display_name else s2.rezort   end 
			having COUNT(DISTINCT g.message_id) > 199
            order by 2 desc 
        """
    },

    # ── 26 – UPVS: počty transakcií podľa MEP class 
    {
        "name": "26_upvs_pocty_transakcii_podla_mep_class",
        "sql": """
            SELECT
                class      AS CLASS,
                count(distinct message_id) as count_distinct_message_id
            FROM lh_bronze.g2g_source_dwh20_g2g_messages_received
            WHERE FLOOR(_batch_day / 100) = FLOOR({batch_day} / 100)
              AND (class LIKE 'MEP_%' OR class LIKE 'PEP_%')
            GROUP BY class
            order by 1
        """
    },

    # ── 35 – UPVS: využívanie formulárov EGOV_DOC + APPLICATION podľa PospID/MsgType ─
    {
        "name": "35_upvs_vyuzivanie_formularov_egov",
        "sql": """
            SELECT
                class         AS CLASS,
                posp_id       AS POSP_ID,
                message_type  AS MESSAGE_TYPE,
                count(distinct message_id) as count_distinct_message_id
            FROM lh_bronze.g2g_source_dwh20_g2g_messages_received
            WHERE FLOOR(_batch_day / 100) = FLOOR({batch_day} / 100)
              AND class IN ('EGOV_DOCUMENT', 'EGOV_APPLICATION', 'EGOV_NOTIFICATION')
            GROUP BY class, posp_id, message_type
        """
    },

    # ── 45 ─────────────────────────────────────────────────────────────────
    {
        "name": "45_iam_heatmap_lokalita_obcana",
        "sql": """
            WITH base AS (
                SELECT
                    CASE
                        WHEN iam_t_usr.identity_type = 1        THEN 'FO'   -- fyzické osoby
                        WHEN iam_t_usr.identity_type IN (2)     THEN 'PO'   -- právnické osoby
                        WHEN iam_t_usr.identity_type IN (4,5,6) THEN 'OVM'  -- orgány verejnej moci
                        ELSE 'OTHER'
                    END AS typ_identity,                          -- SLICER v Power BI
                    iam_t_usr.city_id  AS obec_id,                -- kód obce (SUSR_0025), napr. SK0316512036
                    iam_t_usr.city     AS obec                   -- názov obce
                    --to_date(iam_t_usr.last_login, 'dd-MMM-yy') AS last_login_date
                FROM lh_bronze.iam_t_usr AS iam_t_usr
                WHERE iam_t_usr._batch_day = {batch_day}          -- IAM: 1 denný snapshot
                  AND iam_t_usr.last_login IS NOT NULL            -- len identity, ktoré sa prihlásili
                  AND iam_t_usr.country_id = 703                  -- SK
                  -- AND to_date(iam_t_usr.last_login,'dd-MMM-yy')
                  --     >= add_months(last_day(to_date(concat('{report_month}','-01'),'yyyy-MM-dd')), -24)  -- príp. obmedz na 24 m
            )
            SELECT
                typ_identity,                                                    -- vyberač FO/PO/OVM/OTHER
                obec_id,
                obec,
                --date_format(last_login_date, 'yyyy-MM') AS mesiac_prihlasenia,   -- slicer obdobia (podľa last_login)
                COUNT(*)                                AS pocet_pouzivatelov     -- unikátne identity v obci/mesiaci/type
            FROM base
            GROUP BY typ_identity, obec_id, obec --, date_format(last_login_date, 'yyyy-MM')
            ORDER BY pocet_pouzivatelov DESC
            --ORDER BY mesiac_prihlasenia, pocet_pouzivatelov DESC

            -- LAT/LON: 
            --   LEFT JOIN lh_bronze.<geonames_obce> g ON g.<kluc> = m.<kluc>   -> g.lat, g.lon
        """
    },

     # ── 47 ─────────────────────────────────────────────────────────────────
    {
        "name": "47_iam_segmentacia_pouzivatelov",
        "sql": """
            WITH cfg AS (
                SELECT
                    /* PRAHY SEGMENTACIE (dni) – tu sa ladia */
                    30  AS p_recent_days,        -- nedávno aktívny (dni od last_login)
                    365 AS p_power_span_days,     -- min. dĺžka aktívneho obdobia pre power usera
                    90  AS p_newcomer_age_days,   -- identita mladšia ako X dní = nováčik
                    last_day(to_date(concat('{report_month}', '-01'), 'yyyy-MM-dd')) AS ref_date
            ),
            base AS (
                SELECT
                    iam_t_usr.uri           AS uri,   
                    iam_t_usr.login         AS login,
                    CASE
                        WHEN iam_t_usr.identity_type = 1        THEN 'FO'   -- fyzické osoby
                        WHEN iam_t_usr.identity_type IN (2)     THEN 'PO'   -- právnické osoby
                        WHEN iam_t_usr.identity_type IN (4,5,6) THEN 'OVM'  -- orgány verejnej moci
                        ELSE 'OTHER'
                    END AS segment,
                    to_date(iam_t_usr.created,     'yyyy-MM-dd') AS created_date,
                    to_date(iam_t_usr.first_login, 'yyyy-MM-dd') AS first_login_date,
                    to_date(iam_t_usr.last_login,  'yyyy-MM-dd') AS last_login_date
                FROM lh_bronze.iam_t_usr AS iam_t_usr
                WHERE iam_t_usr._batch_day = {batch_day}            -- IAM: 1 denný snapshot
                  -- AND iam_t_usr.identity_type = 1                -- FO (občan);
            ),
            metrics AS (
                SELECT
                    base.*,
                    cfg.p_recent_days,
                    cfg.p_power_span_days,
                    cfg.p_newcomer_age_days,
                    datediff(cfg.ref_date, base.last_login_date)          AS dni_od_posl_prihlasenia,
                    datediff(base.last_login_date, base.first_login_date) AS dlzka_aktivity_dni,
                    datediff(cfg.ref_date, base.created_date)             AS vek_identity_dni
                FROM base
                CROSS JOIN cfg
            )
            SELECT
                uri,
                login,
                segment,
                created_date,
                first_login_date,
                last_login_date,
                dni_od_posl_prihlasenia,
                dlzka_aktivity_dni,
                vek_identity_dni,
                CASE
                    WHEN first_login_date IS NULL                       THEN 'NOVACIK'
                    WHEN vek_identity_dni <= p_newcomer_age_days         THEN 'NOVACIK'
                    WHEN dni_od_posl_prihlasenia <= p_recent_days
                         AND dlzka_aktivity_dni  >= p_power_span_days    THEN 'POWER_USER'
                    ELSE 'OBCASNY'
                END AS cd_segment,
                CASE
                    WHEN first_login_date IS NULL                       THEN 'Nováčik'
                    WHEN vek_identity_dni <= p_newcomer_age_days         THEN 'Nováčik'
                    WHEN dni_od_posl_prihlasenia <= p_recent_days
                         AND dlzka_aktivity_dni  >= p_power_span_days    THEN 'Power používateľ'
                    ELSE 'Občasný používateľ'
                END AS nazov_segment
            FROM metrics
        """
    },


        # ── 48 ─────────────────────────────────────────────────────────────────
    {
        "name": "48_iam_kpi_prihlasenie_do_schranok",
        "sql": """
            WITH cfg AS (
                SELECT
                    last_day(to_date(concat('{report_month}', '-01'), 'yyyy-MM-dd'))            AS ref_date,
                    add_months(last_day(to_date(concat('{report_month}', '-01'), 'yyyy-MM-dd')), -12) AS od_datum  -- posledný rok
            ),
            base AS (
                SELECT
                    CASE
                        WHEN iam_t_usr.identity_type = 1        THEN 'FO'   -- fyzické osoby
                        WHEN iam_t_usr.identity_type IN (2)     THEN 'PO'   -- právnické osoby
                        WHEN iam_t_usr.identity_type IN (4,5,6) THEN 'OVM'  -- orgány verejnej moci
                        ELSE 'OTHER'
                    END AS segment,
                    to_date(iam_t_usr.last_login, 'dd-MMM-yy') AS last_login_date,
                    cfg.od_datum
                FROM lh_bronze.iam_t_usr AS iam_t_usr
                CROSS JOIN cfg
                WHERE iam_t_usr._batch_day = {batch_day}                  -- IAM: 1 denný snapshot
                  AND iam_t_usr.identity_type IN (1, 2, 4, 5, 6)
                  AND iam_t_usr.edesk_status = 'DELIVERABLE'              -- elektronická schránka (doručovacia)
                  AND iam_t_usr.upvs_status  = 'ACTIVATED'
            )
            SELECT
                segment,
                COUNT(*)                                                           AS pocet_vsetkych_schranok,
                SUM(CASE WHEN last_login_date >= od_datum THEN 1 ELSE 0 END)        AS pocet_unik_prihlaseni_12m,
                ROUND(
                    100.0 * SUM(CASE WHEN last_login_date >= od_datum THEN 1 ELSE 0 END)
                          / NULLIF(COUNT(*), 0)
                , 1)                                                               AS podiel_prihlaseni_pct
            FROM base
            WHERE segment <> 'OTHER'
            GROUP BY segment
            ORDER BY segment
        """
    },


    # ── 49 – ŠM: KPI report – Štátny messenger ─
    {
        "name": "49_sm_kpi_statny_messenger",
        "sql": """
            SELECT
                COUNT(*)                                                AS POCET_VSETKYCH,
                SUM(CASE WHEN message_winning_intent IS NOT NULL
                          AND message_winning_intent_confidence > 0
                         THEN 1 ELSE 0 END)                             AS POCET_USPESNYCH,
                ROUND(
                    SUM(CASE WHEN message_winning_intent IS NOT NULL
                              AND message_winning_intent_confidence > 0
                             THEN 1 ELSE 0 END) * 1.0
                    / NULLIF(COUNT(*), 0), 4)                           AS USPESNOST
            FROM lh_bronze.sm_event
            WHERE event_type = 'user_message'
            AND sm_event._batch_day = {batch_day}
        """
    },

    # ── 51 – CNM: simulácia zmeny podielu kanálov SMS (What-If) ─────
    {
        "name": "51_cnm_simulacia_podiel_kanalov_sms",
        "sql": """
            SELECT
                CASE CAST(h.sending_method AS INT)
                    WHEN 0 THEN 'Email'
                    WHEN 1 THEN 'SMS'
                    WHEN 2 THEN 'Push'
                    WHEN 3 THEN 'eDesk'
                    WHEN 4 THEN 'In-App'
                    ELSE 'Unknown'
                END                                        AS KANAL,
                CAST(h.sending_method AS INT)              AS SENDING_METHOD,
                COUNT(*)                                   AS POCET_CELKOM,
                SUM(CASE WHEN lower(trim(h.success)) = 'true' THEN 1 ELSE 0 END) AS POCET_DORUCENYCH,
                CAST(ROUND(SUM(CASE WHEN lower(trim(h.success)) = 'true' THEN 1 ELSE 0 END) * 1.0
                    / COUNT(*), 4) AS DOUBLE)             AS DELIVERY_RATE
            FROM lh_bronze.cnm_notification_history AS h
            WHERE h._batch_day = {batch_day}
            GROUP BY CAST(h.sending_method AS INT)
        """
    },

    # ── 57 – CNM: optimalizácia kanálov – report efektivity ────────
    {
        "name": "57_cnm_optimalizacia_kanalov_efektivita",
        "sql": """
            WITH att AS (
                SELECT
                    a.history_key,
                    COUNT(*)                                                        AS n_attempts,
                    SUM(CASE WHEN lower(trim(a.success)) = 'true' THEN 0 ELSE 1 END) AS n_failed
                FROM lh_bronze.cnm_notification_history_attempt AS a
                WHERE a._batch_day = {batch_day}
                GROUP BY a.history_key
            ),
            iam AS (                                    -- dedup: 1 riadok na uri, najnovší import
                SELECT uri, identity_type
                FROM (
                    SELECT
                        u.uri,
                        u.identity_type,
                        ROW_NUMBER() OVER (
                            PARTITION BY u.uri
                            ORDER BY u.import_date DESC          
                        ) AS rn
                    FROM lh_bronze.iam_t_usr AS u
                    WHERE u._batch_day = {batch_day}
                )
                WHERE rn = 1
            )
            SELECT
                CASE CAST(h.sending_method AS INT)
                    WHEN 0 THEN 'Email'
                    WHEN 1 THEN 'SMS'
                    WHEN 2 THEN 'Push'
                    WHEN 3 THEN 'eDesk'
                    WHEN 4 THEN 'In-App'
                    ELSE 'Unknown'
                END                                        AS KANAL,
                CASE
                    WHEN u.identity_type = 1        THEN 'FO'
                    WHEN u.identity_type = 2        THEN 'PO'
                    WHEN u.identity_type IN (4,6) THEN 'OVM'
                    ELSE 'OTHER'
                END                                        AS TYP_IDENTITY,
                COUNT(*)                                   AS POCET_CELKOM,
                SUM(CASE WHEN lower(trim(h.success)) = 'true' THEN 1 ELSE 0 END) AS POCET_DORUCENYCH,
                CAST(ROUND(SUM(CASE WHEN lower(trim(h.success)) = 'true' THEN 1 ELSE 0 END) * 1.0
                    / COUNT(*), 4) AS DOUBLE)             AS DELIVERY_RATE,
                SUM(COALESCE(att.n_attempts, 0))           AS POCET_POKUSOV,
                SUM(COALESCE(att.n_failed,   0))           AS POCET_ZLYHANYCH_POKUSOV,
                SUM(CASE WHEN lower(trim(h.success)) = 'true' AND COALESCE(att.n_attempts, 0) = 1
                        THEN 1 ELSE 0 END)                AS POCET_NA_PRVY_POKUS
            FROM lh_bronze.cnm_notification_history AS h
            LEFT JOIN iam AS u
                ON h.uri = u.uri
            LEFT JOIN att
                ON att.history_key = h._key
            WHERE h._batch_day = {batch_day}
            GROUP BY
                CAST(h.sending_method AS INT),
                CASE
                    WHEN u.identity_type = 1        THEN 'FO'
                    WHEN u.identity_type = 2        THEN 'PO'
                    WHEN u.identity_type IN (4,6) THEN 'OVM'
                    ELSE 'OTHER'
                END
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
# CELL 3 – Runner: SQL → CSV + Delta
# ============================================================================
results  = []
out_base = f"/lakehouse/default/Files/reports/{report_month}"
os.makedirs(out_base, exist_ok=True)

CSV_SEP = ";"   # <-- delimiter pre CSV export

for report in REPORTS:
    name = report["name"]
    sql  = report["sql"].format(
        batch_day    = batch_day,
        report_month = report_month
    )

    print(f"\n{'─'*70}")
    print(f"⏳  {name}")

    csv_status   = "—"
    delta_status = "—"
    row_count    = 0

    # ── 1. SQL → Spark DataFrame → Pandas ────────────────────────────────────
    try:
        sdf_src = spark.sql(sql).cache()
        pdf     = sdf_src.toPandas()
        row_count = len(pdf)
    except Exception as e:
        print(f"   ❌  SQL chyba: {e}")
        results.append({"report": name, "rows": 0, "csv": "PRESKOČENÉ", "delta": f"CHYBA SQL: {str(e)[:120]}"})
        continue


    # ── 3. CSV export (zachovaná pôvodná funkcionalita) ───────────────────────
    try:
        csv_path = f"{out_base}/{name}_{report_month}.csv"
        pdf.to_csv(csv_path, index=False, encoding="utf-8-sig", sep=CSV_SEP)
        csv_status = "OK"
        print(f"   📄  CSV:   {csv_path}  ({row_count} riadkov)")
    except Exception as e:
        csv_status = f"CHYBA: {str(e)[:120]}"
        print(f"   ⚠️   CSV chyba: {e}")

    # ── 4. Delta write ────────────────────────────────────────────────────────
    # Konvertujeme späť na Spark DataFrame a pridáme partition stĺpec.
    # Pre prázdne reporty (row_count=0) použijeme pôvodný sdf_src (zachovaná schema).
    # Dynamic partition overwrite prepíše LEN partition report_month='{report_month}'
    # → starší mesiace zostanú nedotknuté, re-run je bezpečný.
    try:
        table_name = f"{TABLE_PREFIX}{name}"
        table_full = f"{DELTA_LAKEHOUSE}.{table_name}" if DELTA_LAKEHOUSE else table_name

        if row_count > 0:
            # Zachovaj poradie stĺpcov presne ako v SQL SELECT / pandas
            spark_schema = spark.createDataFrame(pdf).schema
            ordered_cols = pdf.columns.tolist()
            sdf_out = spark.createDataFrame(pdf, schema=spark_schema).select(ordered_cols)
        else:
            # Prázdny DF: použijeme pôvodnú Spark schema (bez postprocessingu)
            sdf_out = sdf_src

        sdf_out = sdf_out.withColumn("report_month", F.lit(report_month))

        (
            sdf_out.write
                   .format("delta")
                   .mode("overwrite")          # dynamic partition overwrite
                   .partitionBy("report_month")
                   .option("mergeSchema", "true")  # bezpečné pri zmene schémy
                   .saveAsTable(table_full)
        )

        delta_status = "OK"
        print(f"   📊  Delta: {table_full}  (partition: {report_month}, {row_count} riadkov)")

    except Exception as e:
        delta_status = f"CHYBA: {str(e)[:120]}"
        print(f"   ⚠️   Delta chyba: {e}")

    results.append({
        "report" : name,
        "rows"   : row_count,
        "csv"    : csv_status,
        "delta"  : delta_status,
    })
    
sdf_src.unpersist()

# ── Log ───────────────────────────────────────────────────────────────────────
summary  = pd.DataFrame(results)
log_path = f"{out_base}/_log_{report_month}.csv"
summary.to_csv(log_path, index=False, encoding="utf-8-sig", sep=CSV_SEP)

print(f"\n{'='*70}")
print(f"SÚHRN – {report_month}")
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
# CELL 5 – Verifikácia: ukáž aktuálne partície v Delta tabuľkách
# ============================================================================
# Spusti iba ak chceš skontrolovať stav tabuliek po zápise.

print("Dostupné Delta tabuľky s prefixom rpt_:")
spark.sql(f"""
    SHOW TABLES IN {DELTA_LAKEHOUSE} LIKE '{TABLE_PREFIX}*'
""").show(50, truncate=False)

# Ukáž históriu mesiacov pre vybraný report (napr. report 02)
sample_table = f"{DELTA_LAKEHOUSE}.{TABLE_PREFIX}02_iam_zoznam_elektronickych_schranok_fo_aktivovanych_na_dorucovanie"
print(f"\nHistória partícií v: {sample_table}")
spark.sql(f"""
    SELECT report_month, COUNT(*) AS pocet_zaznamov
    FROM {sample_table}
    GROUP BY report_month
    ORDER BY report_month
""").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# CELL 6 – Jednorazový setup pre Report 51 (What-If)   ── SPUSTI RAZ ──
# ----------------------------------------------------------------------------
# NIE je súčasťou mesačného runnera. Referenčné/parameter tabuľky pre PBI model.
# RUN_SETUP=False -> cez "Run all" / Fabric pipeline sa bunka preskočí.
# Pri prvom behu prepni na True (alebo bunku odmrazni a spusti ručne).
# ============================================================================
RUN_SETUP = False   # ← True IBA pri jednorazovom vytvorení dimenzií
 
if RUN_SETUP:
    # Číselník kanálov + jednotková cena za doručenie (UPRAV ceny podľa reálu)
    spark.sql(f"""
        CREATE OR REPLACE TABLE lh_bronze.rpt_51_dim_cnm_channel AS
        SELECT * FROM VALUES
            (0, 'Email',  0.0010),
            (1, 'SMS',    0.0500),
            (2, 'Push',   0.0005),
            (3, 'eDesk',  0.0100),
            (4, 'In-App', 0.0000)
        AS t(SENDING_METHOD, KANAL, UNIT_COST)
    """)
 
    # What-If slider: zmena podielu SMS 0–50 %
    spark.sql(f"""
        CREATE OR REPLACE TABLE lh_bronze.rpt_51_dim_sim_sms_share_change AS
        SELECT CAST(id AS INT) AS SMS_SHARE_CHANGE_PCT
        FROM range(0, 51)
    """)
 
    # What-If alternatíva: cena za 1 SMS (uprav rozsah/krok podľa reálu)
    spark.sql(f"""
        CREATE OR REPLACE TABLE lh_bronze.rpt_51_dim_sim_sms_price AS
        SELECT ROUND(id * 0.005, 3) AS SMS_UNIT_PRICE
        FROM range(0, 41)
    """)
 
    print(f"✅ rpt_51_dim_cnm_channel, rpt_51_dim_sim_sms_share_change, rpt_51_dim_sim_sms_price vytvorené v lh_bronze")
else:
    print("⏭️  Setup preskočený (RUN_SETUP=False). Pre vytvorenie dimenzií nastav RUN_SETUP=True.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
