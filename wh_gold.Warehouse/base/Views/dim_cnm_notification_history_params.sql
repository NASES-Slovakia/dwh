-- Auto Generated (Do not modify) 95AB1AF896A0E744C03221914C25E5937D694BC4D1E0F21F551E685EC3B7A0CF
-- source [lh_gold].[dbo].[dim_cnm_notification_history_params]
-- target [base].[dim_cnm_notification_history_params]
CREATE   VIEW [base].[dim_cnm_notification_history_params]
AS
SELECT
    [history_key] AS [id_history_key],
    [name] AS [desc_name],
    [value] AS [desc_value],
    [_source_file],
    [_batch_day],
    [_row_hash],
    [_load_timestamp],
    [_job_id],
    [_silver_load_timestamp],
    [_silver_job_id],
    [_content_hash],
    [_gold_load_timestamp],
    [_gold_modified_timestamp],
    [_gold_job_id],
    [_is_current],
    [_valid_from],
    [_valid_to]
FROM [lh_gold].[dbo].[dim_cnm_notification_history_params];