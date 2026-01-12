-- Auto Generated (Do not modify) 00B16B8CF41D14F5AED1F296C91879DCF3F329CAB486687C8A1FBFE7387D717F



-- source [lh_gold].[dbo].[dim_cnm_notification_template_text]
-- target [base].[dim_cnm_notification_template_text]
CREATE   VIEW [base].[dim_cnm_notification_template_text]
AS
SELECT
    [notification_key] AS [id_notification_key],
    [type] AS [type_type],
    [notification_type] AS [type_notification_type],
    [locale] AS [type_locale],
    [text] AS [desc_text],
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
FROM [lh_gold].[dbo].[dim_cnm_notification_template_text];