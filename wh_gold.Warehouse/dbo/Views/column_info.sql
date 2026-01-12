-- Auto Generated (Do not modify) 57648963141C093A5A9FDF694FF980531CF37E547B99528504D29D477C4D6D3E
create   view column_info as 
    SELECT * 
    FROM [lh_gold].[INFORMATION_SCHEMA].[COLUMNS]
    WHERE TABLE_SCHEMA = 'base' -- Adjusted schema to 'base' as per schema listing