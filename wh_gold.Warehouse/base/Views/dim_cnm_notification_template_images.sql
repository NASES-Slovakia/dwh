-- Auto Generated (Do not modify) 4AC4E61CE1005A64D4E4B6B1604D5711328C485DC2F59D3E49C63AAA341BDF31



-- source [lh_gold].[dbo].[dim_cnm_notification_template_images]
-- target [base].[dim_cnm_notification_template_images]
CREATE   VIEW [base].[dim_cnm_notification_template_images]
AS
SELECT
    [notification_key] AS [id_notification_key],
    [filename] AS [desc_filename],
    [base64data] AS [desc_base64data],
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
FROM [lh_gold].[dbo].[dim_cnm_notification_template_images];