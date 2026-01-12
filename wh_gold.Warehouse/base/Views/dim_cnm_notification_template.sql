-- Auto Generated (Do not modify) FD55002D2E38B213119CCB744C5F845B23331360F46E42E385342E05200EDA1E



-- source [lh_gold].[dbo].[dim_cnm_notification_template]
-- target [base].[dim_cnm_notification_template]
CREATE   VIEW [base].[dim_cnm_notification_template]
AS
SELECT
    [_key] AS [id_key],
    [_rev] AS [desc_rev],
    [owner] AS [id_owner],
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
FROM [lh_gold].[dbo].[dim_cnm_notification_template];