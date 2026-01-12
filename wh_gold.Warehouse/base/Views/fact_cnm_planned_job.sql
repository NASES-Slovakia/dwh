-- Auto Generated (Do not modify) 57AB493CFF8017319FFE48A9505513F98DF008B96FA214C8CAC9EB92DB10C529



-- source [lh_gold].[dbo].[fact_cnm_planned_job]
-- target [base].[fact_cnm_planned_job]
CREATE   VIEW [base].[fact_cnm_planned_job]
AS
SELECT
    [key] AS [id_key],
    [last_execution] AS [dt_last_execution],
    [last_started] AS [dt_last_started],
    [last_count] AS [desc_last_count],
    [last_index] AS [desc_last_index],
    [paused] AS [is_paused],
    [last_id] AS [id_last_id],
    [finished] AS [is_finished],
    [_source_file],
    [_batch_day],
    [_row_hash],
    [_load_timestamp],
    [_job_id],
    [_silver_load_timestamp],
    [_silver_job_id],
    [_gold_load_timestamp],
    [_gold_job_id]
FROM [lh_gold].[dbo].[fact_cnm_planned_job];