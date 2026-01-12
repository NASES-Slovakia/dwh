-- Auto Generated (Do not modify) 1E6C54ADB7A3C8B42465B3FC38E8FA97D88CA3402C11FB2F8396595509E360B2



-- source [lh_gold].[dbo].[fact_sm_event]
-- target [base].[fact_sm_event]
CREATE   VIEW [base].[fact_sm_event]
AS
SELECT
    [id] AS [cnt_id],
    [conversation_id] AS [id_conversation_id],
    [event_timestamp] AS [dt_event_timestamp],
    [event_type] AS [type_event_type],
    [message_type] AS [type_message_type],
    [message_content] AS [desc_message_content],
    [message_winning_intent] AS [desc_message_winning_intent],
    [message_winning_intent_confidence] AS [rate_message_winning_intent_confidence],
    [message_invalid_intent] AS [is_message_invalid_intent],
    [action_name] AS [desc_action_name],
    [action_data] AS [desc_action_data],
    [slot_name] AS [desc_slot_name],
    [slot_value] AS [desc_slot_value],
    [operator_id] AS [id_operator_id],
    [operator_display_name] AS [name_operator_display_name],
    [created_on] AS [dt_created_on],
    [_source_file],
    [_batch_day],
    [_row_hash],
    [_load_timestamp],
    [_job_id],
    [_silver_load_timestamp],
    [_silver_job_id],
    [_gold_load_timestamp],
    [_gold_job_id]
FROM [lh_gold].[dbo].[fact_sm_event];