-- Auto Generated (Do not modify) A1E4C261A6951387BCF7D7395E5976B1EFEA3D5EAA124F5B132827428E7B9EA9



-- source [lh_gold].[dbo].[fact_cnm_notification_queue]
-- target [base].[fact_cnm_notification_queue]
CREATE   VIEW [base].[fact_cnm_notification_queue]
AS
SELECT
    [_key] AS [id_key],
    [notification_key] AS [id_notification_key],
    [user_id] AS [id_user_id],
    [email] AS [desc_email],
    [channel_override] AS [cnt_channel_override],
    [locale_override] AS [id_locale_override],
    [post_time] AS [dt_post_time],
    [app_name] AS [desc_app_name],
    [reference_id] AS [id_reference_id],
    [attempts] AS [cnt_attempts],
    [app_token] AS [desc_app_token],
    [last_send_attempt] AS [dt_last_send_attempt],
    [phone_no] AS [desc_phone_no],
    [uri] AS [id_uri],
    [dispatch_override] AS [desc_dispatch_override],
    [_source_file],
    [_batch_day],
    [_row_hash],
    [_load_timestamp],
    [_job_id],
    [_gold_load_timestamp],
    [_gold_job_id]
FROM [lh_gold].[dbo].[fact_cnm_notification_queue];