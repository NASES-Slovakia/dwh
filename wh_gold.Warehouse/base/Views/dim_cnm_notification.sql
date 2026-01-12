-- Auto Generated (Do not modify) D92543E3FFDCE882FDB1B2CAA09ABD297F30438073AF4C77C8FB34213D44F7B4



-- source [lh_gold].[dbo].[dim_cnm_notification]
-- target [base].[dim_cnm_notification]
CREATE   VIEW [base].[dim_cnm_notification]
AS
SELECT
    [_key] AS [id_key],
    [_rev] AS [desc_rev],
    [template_key] AS [id_template_key],
    [flags] AS [is_flags],
    [frequency] AS [cnt_frequency],
    [hour] AS [cnt_hour],
    [day_of_week] AS [cnt_day_of_week],
    [day_of_month] AS [cnt_day_of_month],
    [month] AS [cnt_month],
    [deactivated] AS [is_deactivated],
    [owner] AS [id_owner],
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
FROM [lh_gold].[dbo].[dim_cnm_notification];