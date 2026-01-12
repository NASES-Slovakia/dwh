-- Auto Generated (Do not modify) 85421232D93A8520ACB31035E50B2F5E6600FF50DB36E062B981E135827E2BF8



-- source [lh_gold].[dbo].[fact_cnm_notification_queue_params]
-- target [base].[fact_cnm_notification_queue_params]
CREATE   VIEW [base].[fact_cnm_notification_queue_params]
AS
SELECT
    [queue_key] AS [id_queue_key],
    [name] AS [desc_name],
    [value] AS [desc_value],
    [_source_file],
    [_batch_day],
    [_row_hash],
    [_load_timestamp],
    [_job_id],
    [_content_hash],
    [_gold_load_timestamp],
    [_gold_modified_timestamp],
    [_gold_job_id],
    [_is_current],
    [_valid_from],
    [_valid_to]
FROM [lh_gold].[dbo].[fact_cnm_notification_queue_params];