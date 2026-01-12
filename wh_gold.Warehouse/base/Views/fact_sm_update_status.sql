-- Auto Generated (Do not modify) EFC92EEE58395FC25D6A4D509F138D10CAC8D750C76CD1D085D2A328237C867A



-- source [lh_gold].[dbo].[fact_sm_update_status]
-- target [base].[fact_sm_update_status]
CREATE   VIEW [base].[fact_sm_update_status]
AS
SELECT
    [table_name] AS [desc_table_name],
    [update_in_progress] AS [is_update_in_progress],
    [last_update_started_on] AS [dt_last_update_started_on],
    [last_update_finished_on] AS [dt_last_update_finished_on],
    [_source_file],
    [_batch_day],
    [_row_hash],
    [_load_timestamp],
    [_job_id],
    [_silver_load_timestamp],
    [_silver_job_id],
    [_gold_load_timestamp],
    [_gold_job_id]
FROM [lh_gold].[dbo].[fact_sm_update_status];