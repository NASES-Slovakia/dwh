-- Auto Generated (Do not modify) C08D6DBDFB1782BD0D30577D09DAC35AF2497547F4F16B34D7666CFA686315DF



-- source [lh_gold].[dbo].[dim_sm_conversation_user]
-- target [base].[dim_sm_conversation_user]
CREATE   VIEW [base].[dim_sm_conversation_user]
AS
SELECT
    [id] AS [id_id],
    [conversation_id] AS [id_conversation_id],
    [user_id] AS [id_user_id],
    [created_on] AS [dt_created_on],
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
FROM [lh_gold].[dbo].[dim_sm_conversation_user];