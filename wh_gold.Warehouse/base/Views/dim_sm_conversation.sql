-- Auto Generated (Do not modify) 3AA28C8F3A4F9A66A3EC0D544EFFC7EA75F948565884F5EFE528ABDC25C98795



-- source [lh_gold].[dbo].[dim_sm_conversation]
-- target [base].[dim_sm_conversation]
CREATE   VIEW [base].[dim_sm_conversation]
AS
SELECT
    [id] AS [cnt_id],
    [conversation_id] AS [id_conversation_id],
    [user_id] AS [id_user_id],
    [mukc_chat_id] AS [id_mukc_chat_id],
    [archived_on] AS [dt_archived_on],
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
FROM [lh_gold].[dbo].[dim_sm_conversation];