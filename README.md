# dwh — Microsoft Fabric Data Warehouse

Microsoft Fabric workspace implementing a **medallion (Landing → Bronze → Silver → Gold)** data
warehouse. Raw data is delivered as Avro files into the `lh_bronze` lakehouse by an upstream NiFi
flow; a chain of Spark notebooks then promotes it through the layers, and Power BI reporting is
built on top of the Gold layer.

> **Scope of this repository & README:** the **Fabric** side only — pipelines, notebooks,
> lakehouses, the variable library, the semantic model and the report. Ingestion (NiFi) and the
> operational source database are upstream and out of scope here; this document assumes Avro files
> already arrive in `lh_bronze/Files/<SOURCE_SYSTEM>/<table>/`.

This repo is the **Git-integrated definition of the Fabric workspace**. It is deployed by
connecting a Fabric workspace to this repo via *Workspace → Git integration* and syncing.

---

## Table of contents

1. [Architecture](#architecture)
2. [Repository contents](#repository-contents)
3. [How data flows (end to end)](#how-data-flows-end-to-end)
4. [Pipelines](#pipelines)
5. [Notebook parameter reference](#notebook-parameter-reference)
6. [Configuration (Variable Library & environments)](#configuration-variable-library--environments)
7. [Capacities, workspaces & access (RBAC)](#capacities-workspaces--access-rbac)
8. [Metadata, logging & data-quality tables](#metadata-logging--data-quality-tables)
9. [Operational runbooks](#operational-runbooks)
10. [Redeploying to another account / tenant](#redeploying-to-another-account--tenant)
11. [Monitoring](#monitoring)
12. [Known limitations](#known-limitations)
13. [Operational checklist](#operational-checklist)

---

## Architecture

```
   Avro files (upstream NiFi)          Microsoft Fabric workspace (this repo)
   ┌───────────────────────┐   ┌──────────────────────────────────────────────────────────┐
   │ lh_bronze/Files/       │   │  lh_bronze    lh_silver     lh_gold           lh_metadata │
   │   <SOURCE>/<table>/    │──▶│  (Files +     (typed &      (dims + facts,    (mapping,   │
   │   *.avro               │   │   Delta)      validated)     SCD/append)       logs, DQ)  │
   └───────────────────────┘   │     │             │              │                        │
                               │     ▼             ▼              ▼                        │
                               │  ntb_landing → ntb_bronze → ntb_silver_2_gold  →  Power BI│
                               │  _2_bronze     _2_silver                          reports │
                               └──────────────────────────────────────────────────────────┘
```

**Layers (lakehouses):**

| Lakehouse | Purpose |
|-----------|---------|
| `lh_bronze` | Landing (`Files/<SOURCE>/<table>/*.avro`) **and** raw Bronze Delta tables. Append-only, immutable, all columns stored as `STRING`. |
| `lh_silver` | Typed & validated tables. Bronze strings cast to the mapping's target types; bad rows rejected. |
| `lh_gold` | Business layer: dimensions (SCD1/SCD2 + Unknown member) and append-only facts. |
| `lh_metadata` | Control plane: the mapping Excel, generated config tables, run logs, rejection log and data-quality findings. |

**Source systems in scope** (`KNOWN_SOURCES` in `ntb_create_silver_tables`):
`CNM`, `CUD`, `EDESK`, `G2G`, `IAM`, `LS`, `METAIS`, `SM`.

---

## Repository contents

| Item | Type | What it is |
|------|------|------------|
| `Pipe_Main.DataPipeline` | Data Pipeline | Top-level daily orchestrator. Sets `batch_day`/`job_id`, runs `ntb_create_silver_tables` once, then fans out `Pipe_Source_process` per source system, then (optionally) reports. |
| `Pipe_Source_process.DataPipeline` | Data Pipeline | Per-source-system pipeline. Runs the three core notebooks in sequence for one `source_system`. |
| `ntb_landing_2_bronze.Notebook` | Notebook (PySpark) | Landing Avro → Bronze Delta (append-only, immutable, all-string). |
| `ntb_bronze_2_silver.Notebook` | Notebook (PySpark) | Bronze → Silver: cast to typed contract, validate, reject bad rows. |
| `ntb_silver_2_gold.Notebook` | Notebook (PySpark) | Silver → Gold: dimensions (SCD1/SCD2) + append-only facts. |
| `ntb_create_silver_tables.Notebook` | Notebook (PySpark) | Reads the mapping Excel, builds the metadata config tables, runs the consistency check, generates the Silver DDL. Runs **once** per `Pipe_Main` run, before the fan-out. |
| `ntb_report_daily.Notebook` | Notebook (PySpark) | Daily reports → CSV + Delta (partitioned by `report_day`). |
| `ntb_report_monthly.Notebook` | Notebook (PySpark) | Monthly reports → CSV + Delta (partitioned by `report_month`). |
| `ntb_ml_priprava.Notebook` | Notebook (Spark SQL) | ML data-prep / exploratory queries on Silver (e.g. `ml_iam_dormancy`). |
| `lh_bronze` / `lh_silver` / `lh_gold` / `lh_metadata` `.Lakehouse` | Lakehouse | The four medallion lakehouses. |
| `variable_library.VariableLibrary` | Variable Library | Environment IDs (`WorkspaceID`, `MetadataLakehouseID`) with `DEV` / `TEST` / `PROD` value sets. |
| `sm_reporty.SemanticModel` | Semantic Model | Power BI semantic model over the reporting tables. |
| `BI/rpt_mesacne_PBI.Report` | Power BI Report | Monthly Power BI report. |

---

## How data flows (end to end)

1. Avro files arrive in `lh_bronze/Files/<SOURCE_SYSTEM>/<table_name>/`. Filenames follow the
   pattern:
   ```
   <table>_<YYYYMMDD>_<uuid_with_underscores>[_part_N_of_M].avro
   ```
   `<YYYYMMDD>` is the **batch day**; the UUID groups all parts of one export.
2. **`Pipe_Main`** (scheduled daily) computes `batch_day` + `job_id`, runs
   **`ntb_create_silver_tables`** once, then invokes **`Pipe_Source_process`** once per source.
3. **`Pipe_Source_process`** runs, per `source_system`, the three notebooks in order:
   `ntb_landing_2_bronze` → `ntb_bronze_2_silver` → `ntb_silver_2_gold`.
4. Optionally, **`ntb_report_daily`** / **`ntb_report_monthly`** produce the reporting outputs
   consumed by Power BI.

Everything is keyed by **`batch_day`** (YYYYMMDD) and tagged with **`job_id`** so a run is
traceable end-to-end in `lh_metadata.log_table_loads`.

---

## Pipelines

### `Pipe_Main` (orchestrator)

1. **Set `batch_day`** = today in `Central European Standard Time`, `yyyyMMdd`.
2. **Set `job_id`** = now in CET, `yyyyMMddHHmmss`.
3. **`ntb_create_silver_tables`** (`source_system='ALL'`) — refresh mapping/config, run the
   consistency check, generate missing Silver tables. Must succeed before the fan-out.
4. **Fan-out** — one `InvokePipeline` activity per source, each calling `Pipe_Source_process`
   with a fixed `source_system` (`CNM, CUD, EDESK, G2G, IAM, LS, METAIS, SM`) and passing through
   `job_id` + `batch_day` (`waitOnCompletion = true`).
5. **`If First Day in Month`** (currently **Inactive**) — would run `ntb_report_daily` and, on
   the 1st, `ntb_report_monthly`.

> **Schedule:** a daily schedule at **06:00 CET** exists in `.schedules` but is currently
> `enabled: false`. Enable it in *Fabric → Pipe_Main → Schedule*.

### `Pipe_Source_process` (per source system)

Pipeline parameters: `job_id`, `batch_day`, `source_system`. Each activity `dependsOn` the
previous with condition **`Succeeded`**, so a failure stops the chain for that source.

| Order | Activity | Notebook | Parameters the pipeline passes |
|-------|----------|----------|--------------------------------|
| 1 | `ntb_landing_2_bronze` | landing → bronze | `source_system`, `batch_day`, `job_id`, `pipeline_name='ntb_landing_2_bronze'`, `debug_mode='false'`, `catch_up_mode='true'`, `debug_day_from=''`, `debug_day_to=''`, `debug_full_overwrite='false'`, `force_reload='false'`, `raise_on_partial='true'`, `max_workers='1'` |
| 2 | `ntb_bronze_2_silver` | bronze → silver | `source_system`, `batch_day`, `job_id`, `pipeline_name='ntb_bronze_2_silver'`, `load_mode='catch_up'`, `batch_days=''`, `day_from=''`, `reject_threshold='1.0'`, `only_tables=''` |
| 3 | `ntb_silver_2_gold` | silver → gold | `source_system`, `batch_day`, `job_id`, `pipeline_name='ntb_silver_2_gold_append'` |

---

## Notebook parameter reference

> All parameters live in each notebook's `parameters`-tagged first cell and are passed as
> **strings** by the pipeline (parsed inside the notebook). The value in the cell is only a
> fallback default for manual runs. To run a notebook manually, edit the parameters cell (or, in a
> pipeline, override in the activity's *Settings → Base parameters*). Boolean-style flags accept
> `true/1/yes` (case-insensitive) as true; anything else is false.

### 1. `ntb_landing_2_bronze` — Landing → Bronze

**Contract:** Bronze is append-only / immutable. A `(table, batch_day)` is written exactly once
and never rewritten; already-loaded days are skipped before any I/O. All payload columns are
stored as `STRING` so source schema/type drift never breaks the load (typing happens in Silver).

**Run mode is chosen by three flags** (exactly one applies; precedence `debug > catch_up > single`):
BULK (`debug_mode`) > CATCH_UP (`catch_up_mode`) > SINGLE_DAY (both false).

| Parameter | Values | Default (cell) | Pipeline value | Description |
|-----------|--------|----------------|----------------|-------------|
| `job_id` | string | `'20260714060023'` | run's `job_id` | Run correlation id, stamped on every row (`_job_id`) and log entry. Empty → auto-generated `yyyyMMddHHmmss`. |
| `source_system` | one of the 8 sources | `'G2G'` | per fan-out | Selects the landing folder `Files/<source_system>` and the bronze table prefix. |
| `batch_day` | `YYYYMMDD` | `'20260713'` | run's `batch_day` | The target day. CATCH_UP treats it as the inclusive **upper bound**; SINGLE_DAY as the one day; ignored in BULK. |
| `pipeline_name` | string | `'ntb_landing_2_bronze'` | same | Label written to `log_table_loads.pipeline_name`. |
| `debug_mode` | `true`/`false` | `'false'` | `'false'` | **BULK.** Ignore `batch_day`; discover every distinct batch_day in landing and load all per table in one Spark job. Initial/historical load. |
| `catch_up_mode` | `true`/`false` | `'true'` | `'true'` | **CATCH_UP.** Load every available-but-unloaded day up to and including `batch_day`. Self-heals missed runs. |
| `debug_day_from` | `YYYYMMDD` or `''` | `''` | `''` | BULK only: inclusive **lower** bound to chunk a large initial load. |
| `debug_day_to` | `YYYYMMDD` or `''` | `''` | `''` | BULK only: inclusive **upper** bound to chunk a large initial load. |
| `debug_full_overwrite` | `true`/`false` | `'false'` | `'false'` | **Destructive.** BULK only, and ignored when a day range is set: truncate + reload the whole table in one atomic overwrite. The only sanctioned way to destroy Bronze history. |
| `force_reload` | `true`/`false` | `'false'` | `'false'` | Rewrite a single already-loaded day (`replaceWhere` on that `batch_day`). Honoured **only** on the SINGLE_DAY path (auto-disabled in BULK/CATCH_UP). |
| `raise_on_partial` | `true`/`false` | `'true'` | `'true'` | If any day was blocked (incomplete/unreadable export), raise at the end so the activity is **FAILED** and the gap is visible. `false` → activity succeeds; gap visible only in the logs. |
| `max_workers` | integer ≥ 1 | `'1'` | `'1'` | Driver-side threads for OneLake file listing and the per-table loop (tables are independent). Days within a table are never parallelised. `1` = fully sequential. |

**File selection per day:** parse filenames → group by UUID → validate each UUID's part set
(no zero-byte files; all-or-nothing on `_part_N_of_M`; consistent `M`; complete `{1..M}`) → pick
the latest **complete** UUID. If no complete UUID exists, the day is **blocked** (logged FAILED),
every other day still loads. **Write modes** (priority): `FULL_OVERWRITE` → initial `CREATE` (table
missing) → `FORCE_RELOAD` → otherwise `APPEND`.

### 2. `ntb_bronze_2_silver` — Bronze → Silver

Casts all-string Bronze to the **typed Silver contract** (the Silver schema generated by
`ntb_create_silver_tables`). Builds a `_violations` array per row: `TYPE_CAST` (value present but
won't cast) or `NOT_NULL` (missing on a `NOT NULL` column). Rejected rows go to
`lh_metadata.rejection_log`; valid rows are written with a scoped `replaceWhere` over the days that
produced valid rows, so an all-rejected day never wipes existing good data. Datetime/date parsing
is tolerant (tries Slovak `d.M.yyyy H:mm:ss` etc. before a plain cast).

| Parameter | Values | Default (cell) | Pipeline value | Description |
|-----------|--------|----------------|----------------|-------------|
| `source_system` | one of the 8 sources | `'G2G'` | per fan-out | Restricts to bronze tables prefixed `<source>_`. |
| `batch_day` | `YYYYMMDD` | `'20260728'` | run's `batch_day` | catch_up/bulk: inclusive **upper** bound. single: the one day. |
| `job_id` | string or `''` | `''` | run's `job_id` | Run correlation id. Empty → auto-generated `yyyyMMdd_HHMMSS`. |
| `pipeline_name` | string | `'ntb_bronze_2_silver'` | same | Log label. |
| `load_mode` | `catch_up`/`bulk`/`single`/`explicit` | `'batch'` → falls back to `catch_up` | `'catch_up'` | **catch_up:** days new to Silver **or** whose bronze `_load_timestamp` is newer than Silver's watermark (re-ingested), bounded by `[day_from..batch_day]`. **bulk:** every bronze day in `[day_from..batch_day]`, authoritative reload. **single:** just `batch_day`. **explicit:** the days in `batch_days`. |
| `batch_days` | comma-separated `YYYYMMDD` | `''` | `''` | Used only by `load_mode='explicit'`. |
| `day_from` | `YYYYMMDD` or `''` | `''` | `''` | Optional inclusive **lower** bound for catch_up / bulk. |
| `reject_threshold` | `0.0`–`1.0` | `'1.0'` | `'1.0'` | Quality gate. If `rejected/read` **exceeds** it, the table is `FAILED` and Silver is left untouched. `1.0` = never fail on rejections. |
| `only_tables` | comma-separated bronze table names | `''` | `''` | Restrict the run to specific tables. Empty = all in-scope bronze tables that have a Silver target. |

### 3. `ntb_silver_2_gold` — Silver → Gold

Runs **once per source in the fan-out** (≈7 in parallel) and **only reads** `lh_metadata` (it must
never write there — the config is built earlier by `ntb_create_silver_tables`). Dimensions are
processed before facts so fact FK lookups resolve.

- **Dimensions:** hash surrogate keys, SCD1 (overwrite) or SCD2 (history), an Unknown member
  (`sk = -1`), full-snapshot expire/delete for `load_type='full'`.
- **Facts:** append-only. `_batch_day` = arrival day. `full` exports are delta-extracted; `delta`
  days are appended as events. Key-aware change detection (chains `_content_hash` per PK). No
  updates/deletes; corrections only via a full rebuild.

**Parameters (parameters cell):**

| Parameter | Values | Default (cell) | Pipeline value | Description |
|-----------|--------|----------------|----------------|-------------|
| `job_id` | string | `'20260714060023'` | run's `job_id` | Run correlation id. **Not** regenerated — it flows from `Pipe_Main`. |
| `source_system` | one of the 8 sources | `'METAIS'` | per fan-out | Selects the gold tables via the `<source>_` silver prefix. |
| `batch_day` | `YYYYMMDD` | `'20260727'` | run's `batch_day` | **Log label only** — Gold computes its own slice from Silver (fact watermark). |
| `pipeline_name` | string | `'ntb_silver_2_gold'` | `'ntb_silver_2_gold_append'` | Log label. |

**Code-level switches (module constants, not pipeline parameters — edit the notebook to change):**

| Constant | Default | Description |
|----------|---------|-------------|
| `GOLD_LH` | `'lh_gold'` | Target Gold lakehouse for both dims and facts. |
| `SCD2_DEFAULT` | `True` | Dimension default: `True` = keep history (SCD2); `False` = overwrite current (SCD1). |
| `LOAD_TYPE_DEFAULT` | `'delta'` | Fallback when the mapping doesn't specify `load_type` (`full`/`delta`). |
| `UNKNOWN_SK` | `'-1'` | Surrogate key of the Unknown / late-arriving dimension member. |
| `FORCE_FACT_RELOAD` | `False` | Set `True` for an on-demand full fact rebuild (overwrite). Append-only otherwise. |
| `SEED_DAY` | `'00000000'` | Synthetic earliest day used when chaining per-key change state. |

> Per-table `table_type` (DIM/FACT), `gold_table_name` and `load_type` come from
> `lh_metadata.metadata_table_column_setup`. Optional fact→dim FK resolution reads
> `lh_metadata.table_relations` if present. Gold runs log to `lh_metadata.gold_load_log`.

### 4. `ntb_create_silver_tables` — mapping, config & Silver DDL

The single writer of the config tables. Runs once per `Pipe_Main` run (`source_system='ALL'`),
before the fan-out. Reads the mapping Excel (sheets **`Atribúty`** = column contract, **`Tabuľky`**
= table-level DIM/FACT + gold names) into `table_config` / `table_type_config`, derives
`metadata_table_column_setup`, runs the consistency check, and generates missing Silver tables.

| Parameter | Values | Default (cell) | Pipeline value | Description |
|-----------|--------|----------------|----------------|-------------|
| `source_system` | `ALL` or one source | `'ALL'` | `'ALL'` | `ALL` = all known sources (pipeline default). A single source = manual/debug runs only. |
| `mapping_filename` | filename | `'DWH_Systemy_IF_1.5.xlsx'` | `'DWH_Systemy_IF_1.5.xlsx'` | Excel (the **IF / interface** mapping file) under `lh_metadata/Files/Mapping/`. Path is built from the Variable Library IDs. **⚠️ The filename is version-stamped (`_IF_1.5`). When a new IF version is delivered you must (1) upload it to `lh_metadata/Files/Mapping/` and (2) update this parameter — see the callout below.** |
| `reload_metadata` | `auto`/`true`/`false` | `'auto'` | `'auto'` | **auto:** reload the Excel into the config tables only when it changed (filename or OneLake modifyTime differs from the stored stamp). **true:** always. **false:** never. |
| `rebuild_gold_metadata` | `true`/`false` | `'false'` | `'false'` | Force re-derivation of `metadata_table_column_setup` (otherwise rebuilt automatically when the Excel reloaded or the table is missing). |
| `run_consistency` | `true`/`false` | `'true'` | `'true'` | Run the bronze↔mapping column-coverage check → `schema_consistency_log`. |
| `generate_tables` | `true`/`false` | `'true'` | `'true'` | (Re)generate the Silver DDL and create missing Silver tables. |
| `drop_and_recreate` | `true`/`false` | `'false'` | `'false'` | **Destructive.** DROP each target Silver table before CREATE. **Refused when `source_system='ALL'`.** Manual, single-source only — do not expose as a `Pipe_Main` parameter. |
| `block_on_unmapped` | `true`/`false` | `'false'` | `'false'` | Mark `BRONZE_COLUMN_NOT_IN_MAPPING` findings as **BLOCKER** (vs WARNING). |
| `silver_layout` | `cluster`/`none`/`month`/`day` | `'cluster'` | `'cluster'` | Physical layout of generated Silver tables. **cluster** = liquid clustering (recommended). `month`/`day` = partitioned; `none` = flat. |
| `cluster_columns` | comma-separated columns | `'_batch_day'` | `'_batch_day'` | CLUSTER BY keys when `silver_layout='cluster'`. Hoisted into the first 32 columns automatically. |
| `job_id` | string or `''` | `''` | run's `job_id` | Run correlation id. Empty → auto-generated. |
| `pipeline_name` | string | `'ntb_create_silver_tables'` | same | Log label. |

> 📌 **Mapping Excel (IF file) — update checklist.** `ntb_create_silver_tables` reads the
> interface mapping from `lh_metadata/Files/Mapping/<mapping_filename>` (currently
> `DWH_Systemy_IF_1.5.xlsx`, sheets `Atribúty` + `Tabuľky`). The filename carries the IF version,
> so when a new version arrives:
> 1. **Upload** the new file to `lh_metadata/Files/Mapping/` (don't delete the old one until the run succeeds).
> 2. **Update the `mapping_filename` parameter** — in the notebook's parameters cell **and** in the
>    `Pipe_Main → ntb_create_silver_tables` activity's base parameters (both point at the old name today).
> 3. With `reload_metadata='auto'` (the default) the change is detected automatically; the config
>    tables and Silver DDL are rebuilt on the next run. Use `reload_metadata='true'` to force it.
> 4. New/changed columns appear as new Silver columns; check `schema_consistency_log` afterwards.
>
> **Note:** the `Files/` area is **not** versioned in Git — the Excel lives only in the lakehouse,
> so re-upload it after any redeploy (see *Redeploying to another account / tenant*).

### 5. `ntb_report_daily`

| Parameter | Values | Default (cell) | Description |
|-----------|--------|----------------|-------------|
| `batch_day` | `YYYYMMDD` (int) | today | Report day. Filters reports to the exact `_batch_day`; writes CSV to `Files/reports_daily/{report_day}/` and Delta partitioned by `report_day` (YYYY-MM-DD). Dynamic partition overwrite → re-run only rewrites that day. |

### 6. `ntb_report_monthly`

| Parameter | Values | Default (cell) | Description |
|-----------|--------|----------------|-------------|
| `report_month` | `YYYY-MM` | previous month | Month to report. Writes CSV to `Files/reports/{report_month}/` and Delta partitioned by `report_month`. |
| `batch_day` | `YYYYMMDD` (int) | last day of `report_month` | Derived from `report_month`; used for filtering/labelling. |

### 7. `ntb_ml_priprava`

No parameters — Spark SQL exploration / ML prep over Silver (e.g. `lh_silver.ml_iam_dormancy`).

---

## Configuration (Variable Library & environments)

Environment-specific IDs, resolved in notebooks via
`notebookutils.variableLibrary.getLibrary("variable_library")`:

| Variable | DEV | TEST | PROD |
|----------|-----|------|------|
| `WorkspaceID` | `d626fea3-3116-4af5-9cfb-9196dd43cba1` | `1c078226-a52a-40bd-8d62-4dca8f37f913` | `4698536c-5dae-4a6b-abfa-8f120194ac8a` |
| `MetadataLakehouseID` | `4c73f3f2-9c1c-45d3-b6be-3607c949dc6d` | `44b604c2-1d97-436c-8b4a-4eede529bf23` | `d885e9ae-ee76-4b05-9e96-bdbaa86a8b4e` |

Value sets: **`DEV`**, **`TEST`**, **`PROD`** (`valueSets/*.json`, ordered in `settings.json`).
Select the active value set per workspace in *Fabric → Variable Library*. These IDs build the
mapping Excel path in `ntb_create_silver_tables`. The base defaults in `variables.json` mirror the
**DEV** value set.

**Vault parameters** are self-explanatory and belong to the upstream ingestion side; no Fabric-side
action is required.

---

## Capacities, workspaces & access (RBAC)

### Capacities

| Capacity | SKU | Hosts workspaces |
|----------|-----|------------------|
| `fcnasesdwh2dev` | F8 | NASES DWH 2.0 - DEV, NASES DWH 2.0 - TEST |
| `fcnasesdwh2prod` | F8 | NASES DWH 2.0 - PROD |

### Workspaces ↔ environments

Each workspace maps to a Variable Library value set (see
[Configuration](#configuration-variable-library--environments)):

| Workspace | Capacity | Value set | `WorkspaceID` |
|-----------|----------|-----------|---------------|
| NASES DWH 2.0 - DEV | `fcnasesdwh2dev` (F8) | `DEV` | `d626fea3-3116-4af5-9cfb-9196dd43cba1` |
| NASES DWH 2.0 - TEST | `fcnasesdwh2dev` (F8) | `TEST` | `1c078226-a52a-40bd-8d62-4dca8f37f913` |
| NASES DWH 2.0 - PROD | `fcnasesdwh2prod` (F8) | `PROD` | `4698536c-5dae-4a6b-abfa-8f120194ac8a` |

### Access (RBAC)

Workspace access is granted through **Entra ID security groups** mapped to the four Fabric
workspace roles. **PROD is isolated** (its own groups, `-prod` suffix); **DEV and TEST share** one
set of groups (no suffix).

| Fabric role | DEV + TEST (shared) | PROD (isolated) |
|-------------|---------------------|-----------------|
| Admin | `grp-nases-dwh2-fabric-ws-admin` | `grp-nases-dwh2-fabric-ws-admin-prod` |
| Member | `grp-nases-dwh2-fabric-ws-member` | `grp-nases-dwh2-fabric-ws-member-prod` |
| Contributor | `grp-nases-dwh2-fabric-ws-contributor` | `grp-nases-dwh2-fabric-ws-contributor-prod` |
| Viewer | `grp-nases-dwh2-fabric-ws-viewer` | `grp-nases-dwh2-fabric-ws-viewer-prod` |

### Service principal / pipeline connection

`Pipe_Main`'s `InvokePipeline` activities run through a connection authenticated with a **service
principal**:

| Environment | Service principal |
|-------------|-------------------|
| DEV / TEST | `sp-nases-dwh20-fabric` |
| PROD | `sp-nases-dwh20-fabric-prod` |

The service principal is a member of the corresponding **admin** group, so it has the rights to
invoke `Pipe_Source_process`. The connection is workspace-specific (id
`ff11b3d2-e9ec-4e5d-ab11-a3c3cd2ef0e7` in the DEV workspace) and must be re-pointed to the correct
SP after a redeploy — see [Redeploying to another account / tenant](#redeploying-to-another-account--tenant).

---

## Metadata, logging & data-quality tables

All in `lh_metadata`:

| Table | Written by | Purpose |
|-------|-----------|---------|
| `table_config` | `ntb_create_silver_tables` | Column-level contract (Excel `Atribúty`). |
| `table_type_config` | `ntb_create_silver_tables` | Table-level DIM/FACT + gold names (Excel `Tabuľky`). |
| `metadata_table_column_setup` | `ntb_create_silver_tables` | Derived Gold column contract (read by `ntb_silver_2_gold`). |
| `log_table_loads` | `ntb_landing_2_bronze`, `ntb_bronze_2_silver` | One terminal row per `(table, batch_day)`: status, operation, rows read/written/rejected, errors. |
| `rejection_log` | `ntb_bronze_2_silver` | One row per rejected Silver row (reason, column, value, expected type, full row JSON). |
| `schema_consistency_log` | `ntb_create_silver_tables` | Bronze↔mapping coverage findings (BLOCKER/WARNING/INFO). |
| `gold_load_log` | `ntb_silver_2_gold` | Gold load results (separate from `log_table_loads`). |
| `table_relations` (optional) | manual | Fact→dim FK mapping for surrogate-key resolution. |

**Trace one run:**
```sql
SELECT * FROM lh_metadata.log_table_loads WHERE job_id = '<job_id>';
SELECT * FROM lh_metadata.rejection_log   WHERE job_id = '<job_id>';
SELECT * FROM lh_metadata.gold_load_log   WHERE job_id = '<job_id>';
```

---

## Operational runbooks

### Reprocessing data for a selected period (Fabric)

Run notebooks with the parameters below (manually, or via a one-off pipeline run overriding base
parameters). Bronze is immutable, so reprocessing is always explicit.

**One day (rewrite Bronze for a single day):** `ntb_landing_2_bronze`
`debug_mode='false'`, `catch_up_mode='false'`, `batch_day='YYYYMMDD'`, `force_reload='true'`.
→ scoped `replaceWhere` on that day; all other days untouched.

**A range / full history (Bronze):** `ntb_landing_2_bronze`
`debug_mode='true'`, `debug_day_from='YYYYMMDD'`, `debug_day_to='YYYYMMDD'`.
→ loads every day in range in one job. Use `debug_full_overwrite='true'` (no range) **only** to
truncate + reload an entire table (destructive).

**Fill gaps (Bronze):** the daily default — `catch_up_mode='true'`, `batch_day` = upper bound.
Self-healing; re-blocks any still-incomplete day until the source is fixed.

**Silver for a period:** `ntb_bronze_2_silver`
- range: `load_mode='bulk'`, `day_from='YYYYMMDD'`, `batch_day='YYYYMMDD'`.
- specific days: `load_mode='explicit'`, `batch_days='YYYYMMDD,YYYYMMDD,...'`.
- one day: `load_mode='single'`, `batch_day='YYYYMMDD'`.
`replaceWhere` keeps it idempotent.

**Gold for a period:** facts are append-only — a normal re-run appends only new arrivals. For a
clean rebuild, set `FORCE_FACT_RELOAD = True` in `ntb_silver_2_gold` and re-run (overwrites the
fact). Dimensions self-correct via SCD merge on re-run.

> **Note:** after any reprocessing run, confirm the outcome in `log_table_loads` (one row per
> `(table, batch_day)`) for the days you targeted.

### Re-running a failed daily run

Re-run `Pipe_Main` (or a single `Pipe_Source_process` for the affected source). Because Bronze is
immutable and Silver/Gold are watermark/append based, a re-run is safe: already-loaded days are
skipped, only the missing/failed work is redone.

### Recovery & troubleshooting

**Step 1 — diagnose.** Every failure is logged in `lh_metadata`. The `operation` column tells you
the failure class (and therefore the fix):

```sql
-- what failed in this run
SELECT target_table, batch_day, status, operation, rows_read, rows_written,
       rows_rejected, error_message
FROM   lh_metadata.log_table_loads
WHERE  job_id = '<job_id>' AND status IN ('FAILED','WARNING')
ORDER  BY target_table, batch_day;

-- why silver rows were rejected
SELECT failed_column, expected_type, failed_value, COUNT(*) AS n
FROM   lh_metadata.rejection_log WHERE job_id = '<job_id>'
GROUP  BY failed_column, expected_type, failed_value ORDER BY n DESC;

-- gold failures
SELECT target_table, status, operation, error_message
FROM   lh_metadata.gold_load_log WHERE job_id = '<job_id>' AND status = 'FAILED';
```

| Layer | `operation` | Meaning | Fix |
|-------|-------------|---------|-----|
| Bronze | `VALIDATION` | Incomplete / zero-byte export (missing parts) | Source problem — file(s) re-delivered, then re-run (catch-up auto-loads the day). Day stays blocked & re-logged until fixed. |
| Bronze | `UNREADABLE` | Corrupt Avro (bad header/body) | Source problem — re-deliver, re-run. |
| Bronze | `BATCH_DAY_MISMATCH` | Day parsed from rows ≠ selected day | Bad filenames at source; refuses to write (immutability). |
| Silver | FAILED (`REPLACE_WHERE`) | Quality gate exceeded (`rejected/read > reject_threshold`) **or** Silver table missing | Inspect `rejection_log`; fix IF mapping types (re-run `ntb_create_silver_tables`) **or** temporarily raise `reject_threshold`; if "table does not exist" run `ntb_create_silver_tables`. |
| Gold | FAILED | See `gold_load_log.error_message` | Usually a plain re-run; for facts use a full rebuild. |

**Step 2 — recover.** A plain **re-run is always safe to try first** (Bronze immutable, Silver
`replaceWhere`, Gold facts append-only → completed work is skipped). If that isn't enough, use the
recipes below (run the notebook manually or as a one-off pipeline run overriding base parameters).

| Goal | Notebook | Parameters |
|------|----------|------------|
| Re-load **one day** into Bronze (corrected re-delivery) | `ntb_landing_2_bronze` | `debug_mode='false'`, `catch_up_mode='false'`, `batch_day='YYYYMMDD'`, `force_reload='true'` |
| **Gap-fill** Bronze (self-heal missed days) | `ntb_landing_2_bronze` | `catch_up_mode='true'`, `batch_day='<upper bound>'` (the daily default) |
| **Full Bronze rebuild** (truncate + reload) ⚠️ destructive | `ntb_landing_2_bronze` | `debug_mode='true'`, `debug_full_overwrite='true'` (leave `debug_day_from/to` empty) |
| Reload Bronze **history range** (gap-fill within bounds) | `ntb_landing_2_bronze` | `debug_mode='true'`, `debug_day_from='YYYYMMDD'`, `debug_day_to='YYYYMMDD'` |
| **Full Silver reload** for a period | `ntb_bronze_2_silver` | `load_mode='bulk'`, `day_from='YYYYMMDD'`, `batch_day='YYYYMMDD'` |
| Reload Silver for **specific days** | `ntb_bronze_2_silver` | `load_mode='explicit'`, `batch_days='YYYYMMDD,YYYYMMDD,...'` |
| **Recreate a broken Silver table** ⚠️ destructive | `ntb_create_silver_tables` | `source_system='<SRC>'` (never `ALL`), `drop_and_recreate='true'` — then reload Silver in `bulk` |
| **Full Fact rebuild** | `ntb_silver_2_gold` | set `FORCE_FACT_RELOAD = True` in the notebook, re-run (dims self-heal via SCD merge) |

**Full end-to-end rebuild (nuclear), per source, in this order:**
1. `ntb_create_silver_tables` — `source_system='<SRC>'`, `drop_and_recreate='true'` (empty Silver).
2. `ntb_landing_2_bronze` — `debug_mode='true'`, `debug_full_overwrite='true'` (rebuild Bronze).
3. `ntb_bronze_2_silver` — `load_mode='bulk'`, `day_from`/`batch_day` covering all history.
4. `ntb_silver_2_gold` — `FORCE_FACT_RELOAD=True`.

> Notes: `force_reload` is honoured **only** on the single-day path (ignored in bulk/catch-up).
> `debug_full_overwrite` is ignored when a day range is set (a full overwrite would wipe the other
> chunks). `drop_and_recreate` is **refused** for `source_system='ALL'`.

---

## Redeploying to another account / tenant

Redeploy is done entirely through **this Git repo** (Fabric Git integration) — no manual export.

1. In the target tenant/account, create a workspace and connect it to this repo
   (*Workspace → Git integration → Connect*), pick this branch, and **sync** to import all items.
2. In *Fabric → Variable Library → variable_library*, select the value set for the environment
   (`DEV`/`TEST`/`PROD`) and update `WorkspaceID` / `MetadataLakehouseID` to the new workspace's
   ids (add a new value set if it's a brand-new environment).
3. Upload the mapping Excel to `lh_metadata/Files/Mapping/` (Files are not versioned in Git).
4. Fix pipeline **connections** — the `Pipe_Main` `InvokePipeline` connection
   (`ff11b3d2-e9ec-4e5d-ab11-a3c3cd2ef0e7` in the source workspace) is workspace-specific and must
   be re-pointed to the correct service principal (`sp-nases-dwh20-fabric` for DEV/TEST,
   `sp-nases-dwh20-fabric-prod` for PROD) after import. See
   [Capacities, workspaces & access](#capacities-workspaces--access-rbac).
5. Confirm the notebooks' default lakehouse bindings resolved to the new lakehouses.
6. Run `ntb_create_silver_tables` once, then `Pipe_Main`; enable the `Pipe_Main` schedule.

> **Note:** items that do **not** carry over through Git and must be set up manually after import:
> workspace connections, lakehouse `Files/` content (e.g. the mapping Excel) and the Spark
> environment.

---

## Monitoring

- **Fabric run status:** *Fabric → Monitor* and the pipeline run history.
- **Data-level status:** the `lh_metadata` log tables (`log_table_loads`, `rejection_log`,
  `schema_consistency_log`, `gold_load_log`).
- Any monitoring beyond the above is a **Change Request** (not in the original project scope).

---

## Known limitations

- **Stream processing** was not part of the delivery; the current (batch) design was approved.
- The `Pipe_Main` reporting block (`If First Day in Month`) is currently **Inactive**.

---

## Operational checklist

Configuration that lives **outside** this repo and must be set up / verified in the workspace:

- **Spark environment / pool & connections** — configured in the workspace, not in Git: the Spark
  environment used by the notebooks and the connection(s) used by `Pipe_Main`.
- **`Pipe_Main` schedule** — enable it (and the reporting `If First Day in Month` block) if
  daily/monthly reports should run automatically.
- **Mapping IF Excel** — re-upload it after a redeploy (Files aren't in Git) and bump the
  `mapping_filename` parameter (notebook cell **and** the `Pipe_Main` activity) whenever a new IF
  version is delivered.
</content>
