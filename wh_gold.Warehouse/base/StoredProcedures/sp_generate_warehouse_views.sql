CREATE   PROCEDURE [base].[sp_generate_warehouse_views]
    @SchemaName NVARCHAR(128) = 'base'
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SQL NVARCHAR(MAX) = '';
    
    -- Query the Warehouse table instead of Lakehouse shortcut
    SELECT @SQL = @SQL + 
        'CREATE OR ALTER VIEW [base].[' + gold_table_name + '] AS SELECT ' +
        column_list + ' FROM [lh_gold].[dbo].[' + gold_table_name + ']; '
    FROM (
        SELECT 
            gold_table_name,
            STRING_AGG(
                CASE 
                    WHEN silver_column_name <> gold_column_name
                    THEN '[' + silver_column_name + '] AS [' + gold_column_name + ']'
                    ELSE '[' + silver_column_name + ']'
                END,
                ', '
            ) WITHIN GROUP (ORDER BY COALESCE(ordinal_position, 999)) as column_list
        FROM [base].[metadata_table_column_setup_cache]  -- Warehouse table, not shortcut
        GROUP BY gold_table_name
    ) views;
    
    --EXEC sp_executesql @SQL;
    print(@SQL);
    PRINT 'Views created in schema: [' + @SchemaName + ']';
END;