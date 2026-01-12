-- Auto Generated (Do not modify) 0876700899CC088CD4390B8C71A36B640376C7E38446D125A9E84B26B9605817



-- source [lh_gold].[dbo].[dim_sm_conversation_mukc_chat]
-- target [base].[dim_sm_conversation_mukc_chat]
CREATE   VIEW [base].[dim_sm_conversation_mukc_chat]
AS
SELECT
    [id] AS [id_id],
    [conversation_id] AS [id_conversation_id],
    [mukc_chat_id] AS [id_mukc_chat_id],
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
FROM [lh_gold].[dbo].[dim_sm_conversation_mukc_chat];