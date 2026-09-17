# dwh — Dátový sklad v Microsoft Fabric

Microsoft Fabric workspace implementujúci dátový sklad podľa **medailónovej architektúry
(Landing → Bronze → Silver → Gold)**. Surové dáta sú doručované ako Avro súbory do lakehouse
`lh_bronze` cez nadväzujúci NiFi tok; reťaz Spark notebookov ich následne posúva jednotlivými
vrstvami a nad Gold vrstvou je postavený Power BI reporting.

> **Rozsah tohto repozitára a README:** iba **Fabric** časť — pipelines, notebooky, lakehouses,
> variable library, sémantický model a report. Ingescia (NiFi) a prevádzková zdrojová databáza sú
> nadväzujúce a nie sú predmetom tohto dokumentu; predpokladá sa, že Avro súbory už prichádzajú do
> `lh_bronze/Files/<SOURCE_SYSTEM>/<table>/`.
>
> _(Anglická verzia: [README.md](README.md).)_

Tento repozitár je **Git-integrovaná definícia Fabric workspace**. Nasadzuje sa pripojením Fabric
workspace k tomuto repozitáru cez *Workspace → Git integration* a synchronizáciou.

---

## Obsah

1. [Architektúra](#architektúra)
2. [Obsah repozitára](#obsah-repozitára)
3. [Tok dát (od začiatku do konca)](#tok-dát-od-začiatku-do-konca)
4. [Pipelines](#pipelines)
5. [Referencia parametrov notebookov](#referencia-parametrov-notebookov)
6. [Konfigurácia (Variable Library a prostredia)](#konfigurácia-variable-library-a-prostredia)
7. [Kapacity, workspaces a prístup (RBAC)](#kapacity-workspaces-a-prístup-rbac)
8. [Metadátové, logovacie a data-quality tabuľky](#metadátové-logovacie-a-data-quality-tabuľky)
9. [Prevádzkové postupy (runbooky)](#prevádzkové-postupy-runbooky)
10. [Nasadenie do iného účtu / tenantu](#nasadenie-do-iného-účtu--tenantu)
11. [Monitoring](#monitoring)
12. [Známe obmedzenia](#známe-obmedzenia)
13. [Prevádzkový kontrolný zoznam](#prevádzkový-kontrolný-zoznam)

---

## Architektúra

```
   Avro súbory (nadväzujúci NiFi)      Microsoft Fabric workspace (tento repozitár)
   ┌───────────────────────┐   ┌──────────────────────────────────────────────────────────┐
   │ lh_bronze/Files/       │   │  lh_bronze    lh_silver     lh_gold           lh_metadata │
   │   <SOURCE>/<table>/    │──▶│  (Files +     (typované &   (dim + fakty,     (mapovanie, │
   │   *.avro               │   │   Delta)      validované)    SCD/append)       logy, DQ)  │
   └───────────────────────┘   │     │             │              │                        │
                               │     ▼             ▼              ▼                        │
                               │  ntb_landing → ntb_bronze → ntb_silver_2_gold  →  Power BI│
                               │  _2_bronze     _2_silver                          reporty │
                               └──────────────────────────────────────────────────────────┘
```

**Vrstvy (lakehouses):**

| Lakehouse | Účel |
|-----------|------|
| `lh_bronze` | Landing (`Files/<SOURCE>/<table>/*.avro`) **a** surové Bronze Delta tabuľky. Append-only, nemenné (immutable), všetky stĺpce ako `STRING`. |
| `lh_silver` | Typované a validované tabuľky. Bronze reťazce sú pretypované na cieľové typy z mapovania; chybné riadky sú odmietnuté. |
| `lh_gold` | Biznis vrstva: dimenzie (SCD1/SCD2 + Unknown člen) a append-only fakty. |
| `lh_metadata` | Riadiaca vrstva: mapovací Excel, generované konfiguračné tabuľky, logy behov, log odmietnutí a data-quality zistenia. |

**Zdrojové systémy v rozsahu** (`KNOWN_SOURCES` v `ntb_create_silver_tables`):
`CNM`, `CUD`, `EDESK`, `G2G`, `IAM`, `LS`, `METAIS`, `SM`.

---

## Obsah repozitára

| Položka | Typ | Čo to je |
|---------|-----|----------|
| `Pipe_Main.DataPipeline` | Data Pipeline | Hlavný denný orchestrátor. Nastaví `batch_day`/`job_id`, raz spustí `ntb_create_silver_tables`, potom rozvetví `Pipe_Source_process` pre každý zdrojový systém a (voliteľne) reporty. |
| `Pipe_Source_process.DataPipeline` | Data Pipeline | Pipeline pre jeden zdrojový systém. Spúšťa tri hlavné notebooky v poradí pre jeden `source_system`. |
| `ntb_landing_2_bronze.Notebook` | Notebook (PySpark) | Landing Avro → Bronze Delta (append-only, nemenné, všetko ako string). |
| `ntb_bronze_2_silver.Notebook` | Notebook (PySpark) | Bronze → Silver: pretypovanie na typovaný kontrakt, validácia, odmietnutie chybných riadkov. |
| `ntb_silver_2_gold.Notebook` | Notebook (PySpark) | Silver → Gold: dimenzie (SCD1/SCD2) + append-only fakty. |
| `ntb_create_silver_tables.Notebook` | Notebook (PySpark) | Načíta mapovací Excel, vytvorí metadátové konfiguračné tabuľky, spustí kontrolu konzistencie, vygeneruje Silver DDL. Beží **raz** za beh `Pipe_Main`, pred rozvetvením. |
| `ntb_report_daily.Notebook` | Notebook (PySpark) | Denné reporty → CSV + Delta (particionované podľa `report_day`). |
| `ntb_report_monthly.Notebook` | Notebook (PySpark) | Mesačné reporty → CSV + Delta (particionované podľa `report_month`). |
| `ntb_ml_priprava.Notebook` | Notebook (Spark SQL) | Príprava dát pre ML / prieskumové dotazy nad Silver (napr. `ml_iam_dormancy`). |
| `lh_bronze` / `lh_silver` / `lh_gold` / `lh_metadata` `.Lakehouse` | Lakehouse | Štyri medailónové lakehouses. |
| `variable_library.VariableLibrary` | Variable Library | ID prostredia (`WorkspaceID`, `MetadataLakehouseID`) s value setmi `DEV` / `TEST` / `PROD`. |
| `sm_reporty.SemanticModel` | Semantic Model | Power BI sémantický model nad reportovacími tabuľkami. |
| `BI/rpt_mesacne_PBI.Report` | Power BI Report | Mesačný Power BI report. |

---

## Tok dát (od začiatku do konca)

1. Avro súbory prichádzajú do `lh_bronze/Files/<SOURCE_SYSTEM>/<table_name>/`. Názvy súborov majú
   vzor:
   ```
   <table>_<YYYYMMDD>_<uuid_s_podtrznikmi>[_part_N_of_M].avro
   ```
   `<YYYYMMDD>` je **batch deň**; UUID zoskupuje všetky časti (parts) jedného exportu.
2. **`Pipe_Main`** (denný plán) vypočíta `batch_day` + `job_id`, raz spustí
   **`ntb_create_silver_tables`**, potom pre každý zdroj vyvolá **`Pipe_Source_process`**.
3. **`Pipe_Source_process`** spustí pre daný `source_system` tri notebooky v poradí:
   `ntb_landing_2_bronze` → `ntb_bronze_2_silver` → `ntb_silver_2_gold`.
4. Voliteľne **`ntb_report_daily`** / **`ntb_report_monthly`** vytvoria reportovacie výstupy, ktoré
   konzumuje Power BI.

Všetko je kľúčované cez **`batch_day`** (YYYYMMDD) a označené **`job_id`**, takže každý beh je
sledovateľný od začiatku do konca v `lh_metadata.log_table_loads`.

---

## Pipelines

### `Pipe_Main` (orchestrátor)

1. **Nastav `batch_day`** = dnes v `Central European Standard Time`, `yyyyMMdd`.
2. **Nastav `job_id`** = teraz v CET, `yyyyMMddHHmmss`.
3. **`ntb_create_silver_tables`** (`source_system='ALL'`) — obnov mapovanie/konfiguráciu, spusti
   kontrolu konzistencie, vygeneruj chýbajúce Silver tabuľky. Musí prebehnúť úspešne pred
   rozvetvením.
4. **Rozvetvenie (fan-out)** — jedna `InvokePipeline` aktivita na každý zdroj, každá volá
   `Pipe_Source_process` s pevným `source_system` (`CNM, CUD, EDESK, G2G, IAM, LS, METAIS, SM`) a
   posiela ďalej `job_id` + `batch_day` (`waitOnCompletion = true`).
5. **`If First Day in Month`** (aktuálne **neaktívne**) — spustilo by `ntb_report_daily` a v 1. deň
   mesiaca aj `ntb_report_monthly`.

> **Plán (schedule):** denný plán o **06:00 CET** existuje v `.schedules`, ale je aktuálne
> `enabled: false`. Zapni ho v *Fabric → Pipe_Main → Schedule*.

### `Pipe_Source_process` (pre jeden zdrojový systém)

Parametre pipeline: `job_id`, `batch_day`, `source_system`. Každá aktivita `dependsOn` predošlú s
podmienkou **`Succeeded`**, takže zlyhanie zastaví reťaz pre daný zdroj.

| Poradie | Aktivita | Notebook | Parametre, ktoré pipeline posiela |
|---------|----------|----------|-----------------------------------|
| 1 | `ntb_landing_2_bronze` | landing → bronze | `source_system`, `batch_day`, `job_id`, `pipeline_name='ntb_landing_2_bronze'`, `debug_mode='false'`, `catch_up_mode='true'`, `debug_day_from=''`, `debug_day_to=''`, `debug_full_overwrite='false'`, `force_reload='false'`, `raise_on_partial='true'`, `max_workers='1'` |
| 2 | `ntb_bronze_2_silver` | bronze → silver | `source_system`, `batch_day`, `job_id`, `pipeline_name='ntb_bronze_2_silver'`, `load_mode='catch_up'`, `batch_days=''`, `day_from=''`, `reject_threshold='1.0'`, `only_tables=''` |
| 3 | `ntb_silver_2_gold` | silver → gold | `source_system`, `batch_day`, `job_id`, `pipeline_name='ntb_silver_2_gold_append'` |

---

## Referencia parametrov notebookov

> Všetky parametre sú v prvej bunke notebooku označenej tagom `parameters` a pipeline ich posiela
> ako **reťazce** (parsujú sa vnútri notebooku). Hodnota v bunke je iba záložný default pre manuálne
> behy. Pri manuálnom spustení uprav bunku s parametrami (alebo v pipeline prepíš v *Settings →
> Base parameters* danej aktivity). Boolean prepínače berú `true/1/yes` (bez ohľadu na veľkosť
> písmen) ako true; čokoľvek iné je false.

### 1. `ntb_landing_2_bronze` — Landing → Bronze

**Kontrakt:** Bronze je append-only / nemenné. Dvojica `(table, batch_day)` sa zapíše práve raz a
už sa neprepisuje; už načítané dni sa preskočia ešte pred akýmkoľvek I/O. Všetky dátové stĺpce sú
uložené ako `STRING`, aby drift schémy/typov v zdroji nikdy nerozbil load (typovanie prebieha v
Silver).

**Režim behu určujú tri prepínače** (platí práve jeden; priorita `debug > catch_up > single`):
BULK (`debug_mode`) > CATCH_UP (`catch_up_mode`) > SINGLE_DAY (oba false).

| Parameter | Hodnoty | Default (bunka) | Hodnota v pipeline | Popis |
|-----------|---------|-----------------|--------------------|-------|
| `job_id` | reťazec | `'20260714060023'` | `job_id` behu | Korelačné ID behu, zapísané na každom riadku (`_job_id`) aj v logu. Prázdne → auto `yyyyMMddHHmmss`. |
| `source_system` | jeden z 8 zdrojov | `'G2G'` | podľa fan-out | Vyberá landing priečinok `Files/<source_system>` a prefix bronze tabuliek. |
| `batch_day` | `YYYYMMDD` | `'20260713'` | `batch_day` behu | Cieľový deň. CATCH_UP ho berie ako inkluzívnu **hornú hranicu**; SINGLE_DAY ako jeden deň; v BULK sa ignoruje. |
| `pipeline_name` | reťazec | `'ntb_landing_2_bronze'` | rovnaká | Označenie zapísané do `log_table_loads.pipeline_name`. |
| `debug_mode` | `true`/`false` | `'false'` | `'false'` | **BULK.** Ignoruj `batch_day`; nájdi každý distinct batch_day v landing a načítaj všetky na jednu tabuľku v jednom Spark jobe. Prvotný/historický load. |
| `catch_up_mode` | `true`/`false` | `'true'` | `'true'` | **CATCH_UP.** Načítaj každý dostupný, ešte nenačítaný deň až po `batch_day` vrátane. Samo-doháňa zmeškané behy. |
| `debug_day_from` | `YYYYMMDD` alebo `''` | `''` | `''` | Iba BULK: inkluzívna **dolná** hranica na rozdelenie veľkého prvotného loadu na časti. |
| `debug_day_to` | `YYYYMMDD` alebo `''` | `''` | `''` | Iba BULK: inkluzívna **horná** hranica na rozdelenie veľkého prvotného loadu na časti. |
| `debug_full_overwrite` | `true`/`false` | `'false'` | `'false'` | **Deštruktívne.** Iba BULK a ignoruje sa pri zadanom rozsahu dní: truncate + reload celej tabuľky jedným atomickým prepisom. Jediný povolený spôsob, ako zmazať Bronze históriu. |
| `force_reload` | `true`/`false` | `'false'` | `'false'` | Prepíš jeden už načítaný deň (`replaceWhere` na daný `batch_day`). Rešpektované **iba** na SINGLE_DAY ceste (v BULK/CATCH_UP sa automaticky vypne). |
| `raise_on_partial` | `true`/`false` | `'true'` | `'true'` | Ak bol nejaký deň zablokovaný (neúplný/nečitateľný export), na konci vyvolaj chybu, aby aktivita bola **FAILED** a medzera bola viditeľná. `false` → aktivita uspeje; medzera je viditeľná iba v logoch. |
| `max_workers` | celé číslo ≥ 1 | `'1'` | `'1'` | Vlákna na strane drivera pre výpis súborov v OneLake a slučku cez tabuľky (tabuľky sú nezávislé). Dni v rámci jednej tabuľky sa nikdy neparalelizujú. `1` = plne sekvenčne. |

**Výber súborov na deň:** parsuj názvy → zoskup podľa UUID → validuj sadu častí každého UUID
(žiadne 0-bajtové súbory; buď všetky, alebo žiadne `_part_N_of_M`; konzistentné `M`; kompletné
`{1..M}`) → vyber najnovšie **kompletné** UUID. Ak žiadne kompletné UUID neexistuje, deň je
**zablokovaný** (log FAILED), ostatné dni sa aj tak načítajú. **Režimy zápisu** (priorita):
`FULL_OVERWRITE` → prvotné `CREATE` (tabuľka chýba) → `FORCE_RELOAD` → inak `APPEND`.

### 2. `ntb_bronze_2_silver` — Bronze → Silver

Pretypuje Bronze (samé stringy) na **typovaný Silver kontrakt** (Silver schéma vygenerovaná cez
`ntb_create_silver_tables`). Na každom riadku zostaví pole `_violations`: `TYPE_CAST` (hodnota je
prítomná, ale nedá sa pretypovať) alebo `NOT_NULL` (chýba na `NOT NULL` stĺpci). Odmietnuté riadky
idú do `lh_metadata.rejection_log`; platné riadky sa zapisujú so scopovaným `replaceWhere` cez dni,
ktoré vyprodukovali platné riadky, takže deň, kde je všetko odmietnuté, nikdy nezmaže existujúce
dobré dáta. Parsovanie dátumu/času je tolerantné (skúša slovenské `d.M.yyyy H:mm:ss` atď. pred
obyčajným castom).

| Parameter | Hodnoty | Default (bunka) | Hodnota v pipeline | Popis |
|-----------|---------|-----------------|--------------------|-------|
| `source_system` | jeden z 8 zdrojov | `'G2G'` | podľa fan-out | Obmedzí sa na bronze tabuľky s prefixom `<source>_`. |
| `batch_day` | `YYYYMMDD` | `'20260728'` | `batch_day` behu | catch_up/bulk: inkluzívna **horná** hranica. single: daný jeden deň. |
| `job_id` | reťazec alebo `''` | `''` | `job_id` behu | Korelačné ID behu. Prázdne → auto `yyyyMMdd_HHMMSS`. |
| `pipeline_name` | reťazec | `'ntb_bronze_2_silver'` | rovnaká | Označenie v logu. |
| `load_mode` | `catch_up`/`bulk`/`single`/`explicit` | `'batch'` → padá na `catch_up` | `'catch_up'` | **catch_up:** dni nové pre Silver **alebo** tie, ktorých bronze `_load_timestamp` je novší než watermark v Silver (znovu-naimportované), v rozsahu `[day_from..batch_day]`. **bulk:** každý bronze deň v `[day_from..batch_day]`, autoritatívny reload. **single:** iba `batch_day`. **explicit:** dni v `batch_days`. |
| `batch_days` | čiarkami oddelené `YYYYMMDD` | `''` | `''` | Používa sa iba pri `load_mode='explicit'`. |
| `day_from` | `YYYYMMDD` alebo `''` | `''` | `''` | Voliteľná inkluzívna **dolná** hranica pre catch_up / bulk. |
| `reject_threshold` | `0.0`–`1.0` | `'1.0'` | `'1.0'` | Kvalitatívna brána. Ak `odmietnuté/prečítané` túto hodnotu **prekročí**, tabuľka je `FAILED` a Silver zostane nedotknutá. `1.0` = nikdy nezlyhať kvôli odmietnutiam. |
| `only_tables` | čiarkami oddelené názvy bronze tabuliek | `''` | `''` | Obmedz beh na konkrétne tabuľky. Prázdne = všetky bronze tabuľky v rozsahu, ktoré majú Silver cieľ. |

### 3. `ntb_silver_2_gold` — Silver → Gold

Beží **raz na každý zdroj vo fan-oute** (≈7 paralelne) a **iba číta** `lh_metadata` (nikdy tam
nesmie zapisovať — konfiguráciu vytvára skôr `ntb_create_silver_tables`). Dimenzie sa spracujú pred
faktami, aby sa vyriešili FK lookupy faktov.

- **Dimenzie:** hash surogátne kľúče, SCD1 (prepísanie) alebo SCD2 (história), Unknown člen
  (`sk = -1`), full-snapshot expirácia/mazanie pri `load_type='full'`.
- **Fakty:** append-only. `_batch_day` = deň príchodu. `full` exporty sa delta-extrahujú; `delta`
  dni sa pripájajú ako udalosti. Detekcia zmien je kľúčovo-orientovaná (reťazí `_content_hash` na
  PK). Žiadne update/delete; opravy iba cez plný rebuild.

**Parametre (bunka parameters):**

| Parameter | Hodnoty | Default (bunka) | Hodnota v pipeline | Popis |
|-----------|---------|-----------------|--------------------|-------|
| `job_id` | reťazec | `'20260714060023'` | `job_id` behu | Korelačné ID behu. **Nere-generuje sa** — prichádza z `Pipe_Main`. |
| `source_system` | jeden z 8 zdrojov | `'METAIS'` | podľa fan-out | Vyberá gold tabuľky cez silver prefix `<source>_`. |
| `batch_day` | `YYYYMMDD` | `'20260727'` | `batch_day` behu | **Iba označenie v logu** — Gold si vlastný výrez zo Silver počíta sám (watermark faktov). |
| `pipeline_name` | reťazec | `'ntb_silver_2_gold'` | `'ntb_silver_2_gold_append'` | Označenie v logu. |

**Prepínače na úrovni kódu (konštanty modulu, nie parametre pipeline — mení sa úpravou notebooku):**

| Konštanta | Default | Popis |
|-----------|---------|-------|
| `GOLD_LH` | `'lh_gold'` | Cieľový Gold lakehouse pre dimenzie aj fakty. |
| `SCD2_DEFAULT` | `True` | Default pre dimenzie: `True` = uchovávaj históriu (SCD2); `False` = prepíš aktuálne (SCD1). |
| `LOAD_TYPE_DEFAULT` | `'delta'` | Záloha, keď mapovanie neurčí `load_type` (`full`/`delta`). |
| `UNKNOWN_SK` | `'-1'` | Surogátny kľúč Unknown / oneskorene prichádzajúceho člena dimenzie. |
| `FORCE_FACT_RELOAD` | `False` | Nastav `True` pre plný rebuild faktu na požiadanie (prepis). Inak append-only. |
| `SEED_DAY` | `'00000000'` | Syntetický najskorší deň pri reťazení stavu zmien na kľúč. |

> `table_type` (DIM/FACT), `gold_table_name` a `load_type` pre každú tabuľku pochádzajú z
> `lh_metadata.metadata_table_column_setup`. Voliteľné riešenie FK fakt→dimenzia číta
> `lh_metadata.table_relations`, ak existuje. Gold behy logujú do `lh_metadata.gold_load_log`.

### 4. `ntb_create_silver_tables` — mapovanie, konfigurácia a Silver DDL

Jediný zapisovateľ konfiguračných tabuliek. Beží raz za beh `Pipe_Main` (`source_system='ALL'`),
pred rozvetvením. Načíta mapovací Excel (hárky **`Atribúty`** = kontrakt stĺpcov, **`Tabuľky`** =
DIM/FACT na úrovni tabuľky + gold názvy) do `table_config` / `table_type_config`, odvodí
`metadata_table_column_setup`, spustí kontrolu konzistencie a vygeneruje chýbajúce Silver tabuľky.

| Parameter | Hodnoty | Default (bunka) | Hodnota v pipeline | Popis |
|-----------|---------|-----------------|--------------------|-------|
| `source_system` | `ALL` alebo jeden zdroj | `'ALL'` | `'ALL'` | `ALL` = všetky známe zdroje (default pipeline). Jeden zdroj = iba manuálne/debug behy. |
| `mapping_filename` | názov súboru | `'DWH_Systemy_IF_1.5.xlsx'` | `'DWH_Systemy_IF_1.5.xlsx'` | Excel (**IF / interface** mapovací súbor) v `lh_metadata/Files/Mapping/`. Cesta sa skladá z ID z Variable Library. **⚠️ Názov obsahuje verziu (`_IF_1.5`). Keď príde nová verzia IF, musíš (1) nahrať ju do `lh_metadata/Files/Mapping/` a (2) upraviť tento parameter — viď rámček nižšie.** |
| `reload_metadata` | `auto`/`true`/`false` | `'auto'` | `'auto'` | **auto:** načítaj Excel do konfiguračných tabuliek iba keď sa zmenil (názov alebo OneLake modifyTime sa líši od uloženej pečiatky). **true:** vždy. **false:** nikdy. |
| `rebuild_gold_metadata` | `true`/`false` | `'false'` | `'false'` | Vynúť opätovné odvodenie `metadata_table_column_setup` (inak sa prebuduje automaticky pri reloade Excelu alebo keď tabuľka chýba). |
| `run_consistency` | `true`/`false` | `'true'` | `'true'` | Spusti kontrolu pokrytia stĺpcov bronze↔mapovanie → `schema_consistency_log`. |
| `generate_tables` | `true`/`false` | `'true'` | `'true'` | (Znovu)vygeneruj Silver DDL a vytvor chýbajúce Silver tabuľky. |
| `drop_and_recreate` | `true`/`false` | `'false'` | `'false'` | **Deštruktívne.** DROP každej cieľovej Silver tabuľky pred CREATE. **Odmietnuté pri `source_system='ALL'`.** Iba manuálne, jeden zdroj — nevystavuj ako parameter `Pipe_Main`. |
| `block_on_unmapped` | `true`/`false` | `'false'` | `'false'` | Označ zistenia `BRONZE_COLUMN_NOT_IN_MAPPING` ako **BLOCKER** (namiesto WARNING). |
| `silver_layout` | `cluster`/`none`/`month`/`day` | `'cluster'` | `'cluster'` | Fyzické usporiadanie generovaných Silver tabuliek. **cluster** = liquid clustering (odporúčané). `month`/`day` = particionované; `none` = ploché. |
| `cluster_columns` | čiarkami oddelené stĺpce | `'_batch_day'` | `'_batch_day'` | CLUSTER BY kľúče pri `silver_layout='cluster'`. Automaticky sa presunú medzi prvých 32 stĺpcov. |
| `job_id` | reťazec alebo `''` | `''` | `job_id` behu | Korelačné ID behu. Prázdne → auto. |
| `pipeline_name` | reťazec | `'ntb_create_silver_tables'` | rovnaká | Označenie v logu. |

> 📌 **Mapovací Excel (IF súbor) — kontrolný zoznam pri aktualizácii.** `ntb_create_silver_tables`
> číta interface mapovanie z `lh_metadata/Files/Mapping/<mapping_filename>` (aktuálne
> `DWH_Systemy_IF_1.5.xlsx`, hárky `Atribúty` + `Tabuľky`). Názov súboru nesie verziu IF, takže keď
> príde nová verzia:
> 1. **Nahraj** nový súbor do `lh_metadata/Files/Mapping/` (starý nemaž, kým beh neprebehne úspešne).
> 2. **Uprav parameter `mapping_filename`** — v bunke parameters notebooku **aj** v base parametroch
>    aktivity `Pipe_Main → ntb_create_silver_tables` (obe dnes ukazujú na starý názov).
> 3. Pri `reload_metadata='auto'` (default) sa zmena zistí automaticky; konfiguračné tabuľky a Silver
>    DDL sa prebudujú pri ďalšom behu. Použi `reload_metadata='true'` na vynútenie.
> 4. Nové/zmenené stĺpce sa objavia ako nové Silver stĺpce; potom skontroluj `schema_consistency_log`.
>
> **Poznámka:** oblasť `Files/` **nie je** verzovaná v Git — Excel je iba v lakehouse, takže po
> každom nasadení ho treba znova nahrať (viď *Nasadenie do iného účtu / tenantu*).

### 5. `ntb_report_daily`

| Parameter | Hodnoty | Default (bunka) | Popis |
|-----------|---------|-----------------|-------|
| `batch_day` | `YYYYMMDD` (int) | dnes | Deň reportu. Filtruje reporty na presný `_batch_day`; zapíše CSV do `Files/reports_daily/{report_day}/` a Delta particionovanú podľa `report_day` (YYYY-MM-DD). Dynamic partition overwrite → opätovný beh prepíše iba daný deň. |

### 6. `ntb_report_monthly`

| Parameter | Hodnoty | Default (bunka) | Popis |
|-----------|---------|-----------------|-------|
| `report_month` | `YYYY-MM` | predošlý mesiac | Mesiac na report. Zapíše CSV do `Files/reports/{report_month}/` a Delta particionovanú podľa `report_month`. |
| `batch_day` | `YYYYMMDD` (int) | posledný deň `report_month` | Odvodené z `report_month`; slúži na filtrovanie/označovanie. |

### 7. `ntb_ml_priprava`

Bez parametrov — Spark SQL prieskum / príprava dát pre ML nad Silver (napr. `lh_silver.ml_iam_dormancy`).

---

## Konfigurácia (Variable Library a prostredia)

ID špecifické pre prostredie, získavané v notebookoch cez
`notebookutils.variableLibrary.getLibrary("variable_library")`:

| Premenná | DEV | TEST | PROD |
|----------|-----|------|------|
| `WorkspaceID` | `d626fea3-3116-4af5-9cfb-9196dd43cba1` | `1c078226-a52a-40bd-8d62-4dca8f37f913` | `4698536c-5dae-4a6b-abfa-8f120194ac8a` |
| `MetadataLakehouseID` | `4c73f3f2-9c1c-45d3-b6be-3607c949dc6d` | `44b604c2-1d97-436c-8b4a-4eede529bf23` | `d885e9ae-ee76-4b05-9e96-bdbaa86a8b4e` |

Value sety: **`DEV`**, **`TEST`**, **`PROD`** (`valueSets/*.json`, poradie v `settings.json`).
Aktívny value set vyber pre daný workspace v *Fabric → Variable Library*. Tieto ID skladajú cestu k
mapovaciemu Excelu v `ntb_create_silver_tables`. Základné defaulty v `variables.json` zodpovedajú
value setu **DEV**.

**Vault parametre** sú samo-vysvetliteľné a patria na stranu nadväzujúcej ingescie; na strane Fabric
nie je potrebná žiadna akcia.

---

## Kapacity, workspaces a prístup (RBAC)

### Kapacity

| Kapacita | SKU | Hostí workspaces |
|----------|-----|------------------|
| `fcnasesdwh2dev` | F8 | NASES DWH 2.0 - DEV, NASES DWH 2.0 - TEST |
| `fcnasesdwh2prod` | F8 | NASES DWH 2.0 - PROD |

### Workspaces ↔ prostredia

Každý workspace zodpovedá jednému value setu vo Variable Library (viď
[Konfigurácia](#konfigurácia-variable-library-a-prostredia)):

| Workspace | Kapacita | Value set | `WorkspaceID` |
|-----------|----------|-----------|---------------|
| NASES DWH 2.0 - DEV | `fcnasesdwh2dev` (F8) | `DEV` | `d626fea3-3116-4af5-9cfb-9196dd43cba1` |
| NASES DWH 2.0 - TEST | `fcnasesdwh2dev` (F8) | `TEST` | `1c078226-a52a-40bd-8d62-4dca8f37f913` |
| NASES DWH 2.0 - PROD | `fcnasesdwh2prod` (F8) | `PROD` | `4698536c-5dae-4a6b-abfa-8f120194ac8a` |

### Prístup (RBAC)

Prístup do workspace sa udeľuje cez **Entra ID security skupiny** namapované na štyri roly Fabric
workspace. **PROD je izolovaný** (vlastné skupiny, prípona `-prod`); **DEV a TEST zdieľajú** jednu
sadu skupín (bez prípony).

| Rola Fabric | DEV + TEST (zdieľané) | PROD (izolované) |
|-------------|-----------------------|------------------|
| Admin | `grp-nases-dwh2-fabric-ws-admin` | `grp-nases-dwh2-fabric-ws-admin-prod` |
| Member | `grp-nases-dwh2-fabric-ws-member` | `grp-nases-dwh2-fabric-ws-member-prod` |
| Contributor | `grp-nases-dwh2-fabric-ws-contributor` | `grp-nases-dwh2-fabric-ws-contributor-prod` |
| Viewer | `grp-nases-dwh2-fabric-ws-viewer` | `grp-nases-dwh2-fabric-ws-viewer-prod` |

### Service principal / connection pipeline

Aktivity `InvokePipeline` v `Pipe_Main` bežia cez connection autentifikovaný **service
principalom**:

| Prostredie | Service principal |
|------------|-------------------|
| DEV / TEST | `sp-nases-dwh20-fabric` |
| PROD | `sp-nases-dwh20-fabric-prod` |

Service principal je členom príslušnej **admin** skupiny, takže má práva vyvolať
`Pipe_Source_process`. Connection je špecifický pre workspace (id
`ff11b3d2-e9ec-4e5d-ab11-a3c3cd2ef0e7` v DEV workspace) a po nasadení ho treba prepojiť na správny
SP — viď [Nasadenie do iného účtu / tenantu](#nasadenie-do-iného-účtu--tenantu).

---

## Metadátové, logovacie a data-quality tabuľky

Všetky v `lh_metadata`:

| Tabuľka | Zapisuje | Účel |
|---------|----------|------|
| `table_config` | `ntb_create_silver_tables` | Kontrakt na úrovni stĺpcov (Excel `Atribúty`). |
| `table_type_config` | `ntb_create_silver_tables` | DIM/FACT na úrovni tabuľky + gold názvy (Excel `Tabuľky`). |
| `metadata_table_column_setup` | `ntb_create_silver_tables` | Odvodený Gold kontrakt stĺpcov (číta `ntb_silver_2_gold`). |
| `log_table_loads` | `ntb_landing_2_bronze`, `ntb_bronze_2_silver` | Jeden finálny riadok na `(table, batch_day)`: status, operácia, počty riadkov prečítané/zapísané/odmietnuté, chyby. |
| `rejection_log` | `ntb_bronze_2_silver` | Jeden riadok na každý odmietnutý Silver riadok (dôvod, stĺpec, hodnota, očakávaný typ, celý riadok ako JSON). |
| `schema_consistency_log` | `ntb_create_silver_tables` | Zistenia pokrytia bronze↔mapovanie (BLOCKER/WARNING/INFO). |
| `gold_load_log` | `ntb_silver_2_gold` | Výsledky Gold loadov (oddelené od `log_table_loads`). |
| `table_relations` (voliteľné) | manuálne | Mapovanie FK fakt→dimenzia pre riešenie surogátnych kľúčov. |

**Sledovanie jedného behu:**
```sql
SELECT * FROM lh_metadata.log_table_loads WHERE job_id = '<job_id>';
SELECT * FROM lh_metadata.rejection_log   WHERE job_id = '<job_id>';
SELECT * FROM lh_metadata.gold_load_log   WHERE job_id = '<job_id>';
```

---

## Prevádzkové postupy (runbooky)

### Opätovné spracovanie dát za vybrané obdobie (Fabric)

Spusti notebooky s parametrami nižšie (manuálne alebo jednorazovým behom pipeline s prepísaním base
parametrov). Bronze je nemenné, takže opätovné spracovanie je vždy explicitné.

**Jeden deň (prepis Bronze pre jeden deň):** `ntb_landing_2_bronze`
`debug_mode='false'`, `catch_up_mode='false'`, `batch_day='YYYYMMDD'`, `force_reload='true'`.
→ scopovaný `replaceWhere` na daný deň; ostatné dni ostanú nedotknuté.

**Rozsah / celá história (Bronze):** `ntb_landing_2_bronze`
`debug_mode='true'`, `debug_day_from='YYYYMMDD'`, `debug_day_to='YYYYMMDD'`.
→ načíta každý deň v rozsahu v jednom jobe. `debug_full_overwrite='true'` (bez rozsahu) použi
**iba** na truncate + reload celej tabuľky (deštruktívne).

**Doplnenie medzier (Bronze):** denný default — `catch_up_mode='true'`, `batch_day` = horná hranica.
Samo-doháňa; opäť zablokuje každý stále neúplný deň, kým sa zdroj neopraví.

**Silver za obdobie:** `ntb_bronze_2_silver`
- rozsah: `load_mode='bulk'`, `day_from='YYYYMMDD'`, `batch_day='YYYYMMDD'`.
- konkrétne dni: `load_mode='explicit'`, `batch_days='YYYYMMDD,YYYYMMDD,...'`.
- jeden deň: `load_mode='single'`, `batch_day='YYYYMMDD'`.
`replaceWhere` zaručuje idempotenciu.

**Gold za obdobie:** fakty sú append-only — bežný opätovný beh pripojí iba nové príchody. Pre čistý
rebuild nastav `FORCE_FACT_RELOAD = True` v `ntb_silver_2_gold` a spusti znova (prepíše fakt).
Dimenzie sa samy opravia cez SCD merge pri opätovnom behu.

> **Poznámka:** po každom opätovnom spracovaní over výsledok v `log_table_loads` (jeden riadok na
> `(table, batch_day)`) pre dni, ktoré si cielil.

### Opätovné spustenie zlyhaného denného behu

Spusti znova `Pipe_Main` (alebo jednu `Pipe_Source_process` pre postihnutý zdroj). Keďže Bronze je
nemenné a Silver/Gold sú založené na watermark/append, opätovný beh je bezpečný: už načítané dni sa
preskočia, zopakuje sa iba chýbajúca/zlyhaná práca.

### Obnova a riešenie problémov (recovery & troubleshooting)

**Krok 1 — diagnostika.** Každé zlyhanie je zalogované v `lh_metadata`. Stĺpec `operation` ti povie
triedu zlyhania (a teda aj opravu):

```sql
-- čo v tomto behu zlyhalo
SELECT target_table, batch_day, status, operation, rows_read, rows_written,
       rows_rejected, error_message
FROM   lh_metadata.log_table_loads
WHERE  job_id = '<job_id>' AND status IN ('FAILED','WARNING')
ORDER  BY target_table, batch_day;

-- prečo boli silver riadky odmietnuté
SELECT failed_column, expected_type, failed_value, COUNT(*) AS n
FROM   lh_metadata.rejection_log WHERE job_id = '<job_id>'
GROUP  BY failed_column, expected_type, failed_value ORDER BY n DESC;

-- zlyhania v gold
SELECT target_table, status, operation, error_message
FROM   lh_metadata.gold_load_log WHERE job_id = '<job_id>' AND status = 'FAILED';
```

| Vrstva | `operation` | Význam | Oprava |
|--------|-------------|--------|--------|
| Bronze | `VALIDATION` | Neúplný / 0-bajtový export (chýbajúce časti) | Problém zdroja — súbory znovu doručené, potom opätovný beh (catch-up deň automaticky načíta). Deň ostáva zablokovaný a re-logovaný, kým sa neopraví. |
| Bronze | `UNREADABLE` | Poškodený Avro (zlá hlavička/telo) | Problém zdroja — znovu doručiť, spustiť znova. |
| Bronze | `BATCH_DAY_MISMATCH` | Deň parsovaný z riadkov ≠ vybraný deň | Zlé názvy súborov v zdroji; odmietne zápis (immutability). |
| Silver | FAILED (`REPLACE_WHERE`) | Prekročená kvalitatívna brána (`odmietnuté/prečítané > reject_threshold`) **alebo** Silver tabuľka chýba | Skontroluj `rejection_log`; oprav typy v IF mapovaní (spusti znova `ntb_create_silver_tables`) **alebo** dočasne zvýš `reject_threshold`; ak "table does not exist", spusti `ntb_create_silver_tables`. |
| Gold | FAILED | Viď `gold_load_log.error_message` | Zvyčajne stačí opätovný beh; pre fakty použi plný rebuild. |

**Krok 2 — obnova.** Obyčajný **opätovný beh je vždy bezpečné skúsiť ako prvé** (Bronze nemenné,
Silver `replaceWhere`, Gold fakty append-only → hotová práca sa preskočí). Ak to nestačí, použi
recepty nižšie (spusti notebook manuálne alebo ako jednorazový beh pipeline s prepísaním base
parametrov).

| Cieľ | Notebook | Parametre |
|------|----------|-----------|
| Znovu načítať **jeden deň** do Bronze (opravené doručenie) | `ntb_landing_2_bronze` | `debug_mode='false'`, `catch_up_mode='false'`, `batch_day='YYYYMMDD'`, `force_reload='true'` |
| **Doplniť medzery** v Bronze (samo-doháňanie zmeškaných dní) | `ntb_landing_2_bronze` | `catch_up_mode='true'`, `batch_day='<horná hranica>'` (denný default) |
| **Plný rebuild Bronze** (truncate + reload) ⚠️ deštruktívne | `ntb_landing_2_bronze` | `debug_mode='true'`, `debug_full_overwrite='true'` (nechaj `debug_day_from/to` prázdne) |
| Reload rozsahu **histórie Bronze** (doplnenie v rámci hraníc) | `ntb_landing_2_bronze` | `debug_mode='true'`, `debug_day_from='YYYYMMDD'`, `debug_day_to='YYYYMMDD'` |
| **Plný reload Silver** za obdobie | `ntb_bronze_2_silver` | `load_mode='bulk'`, `day_from='YYYYMMDD'`, `batch_day='YYYYMMDD'` |
| Reload Silver za **konkrétne dni** | `ntb_bronze_2_silver` | `load_mode='explicit'`, `batch_days='YYYYMMDD,YYYYMMDD,...'` |
| **Znovu vytvoriť poškodenú Silver tabuľku** ⚠️ deštruktívne | `ntb_create_silver_tables` | `source_system='<SRC>'` (nikdy `ALL`), `drop_and_recreate='true'` — potom reload Silver v `bulk` |
| **Plný rebuild faktu** | `ntb_silver_2_gold` | nastav `FORCE_FACT_RELOAD = True` v notebooku, spusti znova (dimenzie sa opravia cez SCD merge) |

**Plný rebuild od začiatku do konca (nuclear), na jeden zdroj, v tomto poradí:**
1. `ntb_create_silver_tables` — `source_system='<SRC>'`, `drop_and_recreate='true'` (prázdny Silver).
2. `ntb_landing_2_bronze` — `debug_mode='true'`, `debug_full_overwrite='true'` (rebuild Bronze).
3. `ntb_bronze_2_silver` — `load_mode='bulk'`, `day_from`/`batch_day` pokrývajúce celú históriu.
4. `ntb_silver_2_gold` — `FORCE_FACT_RELOAD=True`.

> Poznámky: `force_reload` je rešpektované **iba** na single-day ceste (v bulk/catch-up ignorované).
> `debug_full_overwrite` sa ignoruje pri zadanom rozsahu dní (plný prepis by zmazal ostatné časti).
> `drop_and_recreate` je **odmietnuté** pri `source_system='ALL'`.

---

## Nasadenie do iného účtu / tenantu

Nasadenie prebieha celé cez **tento Git repozitár** (Fabric Git integration) — žiadny manuálny
export.

1. V cieľovom tenante/účte vytvor workspace a pripoj ho k tomuto repozitáru
   (*Workspace → Git integration → Connect*), vyber túto vetvu a **synchronizuj**, aby sa
   naimportovali všetky položky.
2. V *Fabric → Variable Library → variable_library* vyber value set pre dané prostredie
   (`DEV`/`TEST`/`PROD`) a nastav `WorkspaceID` / `MetadataLakehouseID` na ID nového workspace
   (ak je to úplne nové prostredie, pridaj nový value set).
3. Nahraj mapovací Excel do `lh_metadata/Files/Mapping/` (Files nie sú verzované v Git).
4. Oprav **connections** pipeline — connection `InvokePipeline` v `Pipe_Main`
   (`ff11b3d2-e9ec-4e5d-ab11-a3c3cd2ef0e7` v zdrojovom workspace) je špecifický pre workspace a po
   importe ho treba prepojiť na správny service principal (`sp-nases-dwh20-fabric` pre DEV/TEST,
   `sp-nases-dwh20-fabric-prod` pre PROD). Viď
   [Kapacity, workspaces a prístup](#kapacity-workspaces-a-prístup-rbac).
5. Over, že default lakehouse väzby notebookov sa vyriešili na nové lakehouses.
6. Spusti raz `ntb_create_silver_tables`, potom `Pipe_Main`; zapni plán `Pipe_Main`.

> **Poznámka:** položky, ktoré sa cez Git **neprenesú** a treba ich po importe nastaviť manuálne:
> connections workspace, obsah `Files/` v lakehouse (napr. mapovací Excel) a Spark environment.

---

## Monitoring

- **Stav behov Fabric:** *Fabric → Monitor* a história behov pipeline.
- **Stav na úrovni dát:** logovacie tabuľky v `lh_metadata` (`log_table_loads`, `rejection_log`,
  `schema_consistency_log`, `gold_load_log`).
- Akýkoľvek monitoring nad rámec vyššie uvedeného je predmetom **Change Requestu** (nebolo v pôvodnom
  rozsahu projektu).

---

## Známe obmedzenia

- **Stream spracovanie** nebolo predmetom dodávky; aktuálny (dávkový) návrh bol schválený.
- Reportovací blok `Pipe_Main` (`If First Day in Month`) je aktuálne **neaktívny**.

---

## Prevádzkový kontrolný zoznam

Konfigurácia, ktorá je **mimo** tohto repozitára a treba ju v workspace nastaviť / overiť:

- **Spark environment / pool a connections** — konfiguruje sa vo workspace, nie v Git: Spark
  environment používaný notebookmi a connection(y) používané v `Pipe_Main`.
- **Plán `Pipe_Main`** — zapni ho (a reportovací blok `If First Day in Month`), ak sa majú
  denné/mesačné reporty spúšťať automaticky.
- **Mapovací IF Excel** — po nasadení ho znova nahraj (Files nie sú v Git) a uprav parameter
  `mapping_filename` (bunka notebooku **aj** aktivita `Pipe_Main`), keď príde nová verzia IF.
</content>
