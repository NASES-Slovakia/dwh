-- Auto Generated (Do not modify) 88CC473C0910CFA8BF85B9395AF93FC31A2E04E3328CF28144F609472C21B6F1



-- source [lh_gold].[dbo].[dim_cnm_notification_template_params]
-- target [base].[dim_cnm_notification_template_params]
CREATE   VIEW [base].[dim_cnm_notification_template_params]
AS
SELECT
    [notification_key] AS [id_notification_key],
    [name] AS [desc_name],
    [type] AS [type_type],
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
FROM [lh_gold].[dbo].[dim_cnm_notification_template_params];