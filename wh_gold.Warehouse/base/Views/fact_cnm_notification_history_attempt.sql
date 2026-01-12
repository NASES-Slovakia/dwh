-- Auto Generated (Do not modify) BB87A742E21CC7EC0A84EE20E92B30E28DB26968476930EFA82CAFD4BB959EB3



-- source [lh_gold].[dbo].[fact_cnm_notification_history_attempt]
-- target [base].[fact_cnm_notification_history_attempt]
CREATE   VIEW [base].[fact_cnm_notification_history_attempt]
AS
SELECT
    [_key] AS [id_key],
    [history_key] AS [cnt_history_key],
    [attempt] AS [dt_attempt],
    [success] AS [is_success],
    [_source_file],
    [_batch_day],
    [_row_hash],
    [_load_timestamp],
    [_job_id],
    [_silver_load_timestamp],
    [_silver_job_id],
    [_gold_load_timestamp],
    [_gold_job_id]
FROM [lh_gold].[dbo].[fact_cnm_notification_history_attempt];