-- Auto Generated (Do not modify) 67DE5F121063BD4573318D4C209A8FE686168A20BA1ADFC7A738DB9549F02EC1



-- source [lh_gold].[dbo].[fact_cnm_notification_history]
-- target [base].[fact_cnm_notification_history]
CREATE   VIEW [base].[fact_cnm_notification_history]
AS
SELECT
    [_key] AS [id_key],
    [notification_key] AS [id_notification_key],
    [user_id] AS [id_user_id],
    [locale] AS [type_locale],
    [email] AS [desc_email],
    [post_time] AS [dt_post_time],
    [send_time] AS [dt_send_time],
    [app_name] AS [desc_app_name],
    [reference_id] AS [id_reference_id],
    [success] AS [is_success],
    [app_token] AS [desc_app_token],
    [phone_no] AS [desc_phone_no],
    [queue_key] AS [cnt_queue_key],
    [removal_source] AS [cnt_removal_source],
    [sending_method] AS [cnt_sending_method],
    [uri] AS [id_uri],
    [dispatch_override] AS [dt_dispatch_override],
    [_source_file],
    [_batch_day],
    [_row_hash],
    [_load_timestamp],
    [_job_id],
    [_gold_load_timestamp],
    [_gold_job_id]
FROM [lh_gold].[dbo].[fact_cnm_notification_history];