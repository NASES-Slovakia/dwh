-- Auto Generated (Do not modify) 7E84D86B163E0230254ADA1498EC1834EE6338CD4B81D5EEA3277CCFE87C614C
CREATE   VIEW [base].[vw_generate_views_sql]
AS
SELECT
    'CREATE OR ALTER VIEW [base].[' + gold_table_name + ']' + CHAR(13) + CHAR(10) +
    'AS' + CHAR(13) + CHAR(10) +
    'SELECT' + CHAR(13) + CHAR(10) +
    STRING_AGG(
        '    ' + 
        CASE 
            WHEN silver_column_name <> gold_column_name
                THEN '[' + silver_column_name + '] AS [' + gold_column_name + ']'
            ELSE '[' + silver_column_name + ']'
        END,
        ',' + CHAR(13) + CHAR(10)
    ) WITHIN GROUP (ORDER BY COALESCE(ordinal_position, 999)) +
    CHAR(13) + CHAR(10) +
    'FROM [lh_gold].[dbo].[' + gold_table_name + '];'
    AS create_view_sql
FROM [base].[metadata_table_column_setup_cache]
GROUP BY gold_table_name;