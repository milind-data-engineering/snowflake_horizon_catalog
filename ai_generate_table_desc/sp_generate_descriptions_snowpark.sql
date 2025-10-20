


-- Steps:  

--1. Create the SP
create procedure sp_generate_descriptions_snowpark(databases array, apply_descriptions_to_table_and_columns varchar)
    returns String
    language python
    runtime_version = 3.11
    packages =('snowflake-snowpark-python')
    handler = 'main' comment = 'Automatically generate table or column descriptions using AI_GENERATE_TABLE_DESC'
    as '# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, current_timestamp
import json
from snowflake.snowpark.types import StringType, StructType, StructField, TimestampType

def main(session: snowpark.Session, databases, apply_descriptions_to_table_and_columns): 
    
    # Your code goes here, inside the "main" handler.
    #databases = [''test_auto_desc_1'',''test_auto_desc_2'']
    #schema_name =''N''
    #apply_descriptions_to_table_and_columns = ''Y''
    print(databases)


    for database_name in databases:
    # If the schema_name is N; that means we need descriptions from all schemas in the database
        print(database_name)
        
        tables_sql = f"""
            SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
            FROM {database_name}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG ilike ''{database_name}'' -- Add current database filter for precision 
            AND TABLE_TYPE = ''BASE TABLE''
            AND TABLE_SCHEMA not in (''INFORMATION_SCHEMA'',''PUBLIC'');
        """;
        
        table_rows = session.sql(tables_sql).collect()
        # else:
        #     table_rows = session.sql(schema_sql).collect()

        print(table_rows)

        if not table_rows:
            print(f"Success: No tables found in database ''{database_name}''");

        # --- Prepare for bulk column insertions (more efficient) ---
        all_columns_data_for_insert = []

        for table_row in table_rows:
            # Access column values by their uppercase names from INFORMATION_SCHEMA results
            table_name = table_row[''TABLE_NAME'']
            table_catalog = table_row[''TABLE_CATALOG'']
            table_schema = table_row[''TABLE_SCHEMA'']

            # Fully qualified table name for calling AI_GENERATE_TABLE_DESC if it expects it
            # The code passed just `tableName`, assuming `AI_GENERATE_TABLE_DESC` knows context.
            # Let''s stick to just the table name as per JS, or adjust if AI_GENERATE needs FQN.
            #ai_sproc_input_table_name = table_name 

             # ---- 2. Check if a comment already exists for a table. If yes; then skip apply else apply it
             
            # Comment value is returned if the table comment already exists
            is_table_comment = f""" SELECT case when COMMENT is null then ''Y'' else ''N'' end as table_comment_null
                                    FROM {database_name}.INFORMATION_SCHEMA.TABLES
                                    WHERE TABLE_CATALOG = ''{table_catalog}'' -- Add current database filter for precision 
                                    AND TABLE_TYPE = ''BASE TABLE''
                                    AND TABLE_SCHEMA =''{table_schema}'' AND TABLE_NAME=''{table_name}'';""";
            
            is_table_comment_null = session.sql(is_table_comment).collect()[0][0];


            # ----- 3. Check if all comments exists for a table in a schema within a database
            # Value of 1 is returned if all columns contains comments for that specific db.schema.table
            all_cols_comments = f""" select case when COUNT_COLS_COMMENT_NOT_NULL = COUNT_COLS then 1 else 0 end as ALL_COMMENTS
                                      from
                                        (
                                            select  
                                            sum(case when COMMENT IS NOT NULL then 1 else 0 end) as COUNT_COLS_COMMENT_NOT_NULL
                                            , count(COLUMN_NAME) as COUNT_COLS
                                            from {database_name}.INFORMATION_SCHEMA.COLUMNS 
                                            where table_catalog=''{table_catalog}'' and table_schema=''{table_schema}'' and table_name=''{table_name}''
                                            group by TABLE_NAME
                                        ); """;

            is_cols_comments = session.sql(all_cols_comments).collect()[0][0];

            
            # --- 4. Call AI_GENERATE_TABLE_DESC for each table ---
            # AI_GENERATE_TABLE_DESC expects a STRING for table_name, not a quoted identifier.
            # The JSON output will be in the first column of the result set.
            call_ai_sproc_sql = f"CALL AI_GENERATE_TABLE_DESC(''{table_catalog}.{table_schema}.{table_name}'', {{''describe_columns'': true, ''use_table_data'': true}})"
            
            
            # Only execute the AI_GENERATE_TABLE_DESC when the table doesn''t contain a comment or any of the columns within the table doesn''t contain any comment
            if is_table_comment_null ==''Y'' or is_cols_comments == 0:
                ai_sproc_result_rows = session.sql(call_ai_sproc_sql).collect()
            else:
                print(f"Warning: AI_GENERATE_TABLE_DESC is skipped since the table ''{table_catalog}''.''{table_schema}''.''{table_name}'' & columns already have comments ")
                continue

            if not ai_sproc_result_rows or ai_sproc_result_rows[0][0] is None:
                print(f"Warning: AI_GENERATE_TABLE_DESC returned no data or NULL for table ''{table_name}''. Skipping.")
                continue # Skip to next table

            # The result is typically in the first column of the returned row.
            # Snowpark automatically deserializes VARIANT/JSON into Python dicts/lists.
            json_data = json.loads(ai_sproc_result_rows[0][0]) 
            print(json_data)
            print(type(json_data))
        

            # Extract table details from JSON
            table_meta_list = json_data.get(''TABLE'', []) # Use .get with default []
        
            if not table_meta_list:
                print(f"Warning: No ''TABLE'' metadata found in AI_GENERATE_TABLE_DESC output for ''{table_name}''. Skipping.")
                continue
        
            table_meta = table_meta_list[0] # Get the first (and usually only) table object

            table_description = table_meta.get(''description'')
            table_name_from_ai_json = table_meta.get(''name'') # Should match original table name
            # Use current_database and schema_name parameter as the source of truth for DB/Schema in DML for consistency
            
            # Ensure essential details are available
            if not all([table_description, table_name_from_ai_json]):
                print(f"Warning: Missing essential table description or name from AI_GENERATE_TABLE_DESC for ''{table_name}''. Skipping.")
                continue

            # --- 3. Insert table metadata into catalog_table ---
            insert_table_sql = f"""
                    INSERT INTO ai_generated_description_catalog (domain, description, name, database_name, schema_name, table_name)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """
           
                                                
            # --- 5. Optionally set comment on the actual table ---
            # Apply comments only when the flag apply_descriptions_to_table_and_columns = Y and if any table_description exist for that table
            if apply_descriptions_to_table_and_columns==''Y'' and table_description and is_table_comment_null==''Y'':
                print(table_name_from_ai_json)
                print(is_table_comment_null)
                set_table_comment_sql = f"ALTER TABLE {table_catalog}.{table_schema}.{table_name_from_ai_json} SET COMMENT = ?"
                session.sql(set_table_comment_sql, params=[table_description]).collect()

                session.sql(insert_table_sql,
                            params=[''TABLE'', table_description, table_name_from_ai_json, table_catalog, table_schema, table_name] # Pass None for table_name column in catalog_table for table-level entry
                           ).collect() # .collect() to execute the DML

            # When apply_descriptions_to_table_and_columns is N then just insert the generated descriptions into the catalog table
            if apply_descriptions_to_table_and_columns==''N'' and table_description:
                session.sql(insert_table_sql,
                            params=[''TABLE'', table_description, table_name_from_ai_json, table_catalog, table_schema, table_name] # Pass None for table_name column in catalog_table for table-level entry
                           ).collect() # .collect() to execute the DML

        
            # --- 6. Loop through each column and prepare data for bulk insertion ---
            columns_meta_list = json_data.get(''COLUMNS'', []) # Get list of columns metadata
            
            # Populate data for bulk insertion
            for column_meta in columns_meta_list:
                column_description = column_meta.get(''description'')
                column_name = column_meta.get(''name'')
                ingested_timestamp = session.sql("SELECT CURRENT_TIMESTAMP();").collect()[0][0]

                # ---- Check if a comment already exists for a column. If yes; then skip apply else apply it
                is_column_comment = f""" SELECT case when COMMENT is null then ''Y'' else ''N'' end as col_comment_null
                                    FROM {database_name}.INFORMATION_SCHEMA.COLUMNS
                                    WHERE TABLE_CATALOG = ''{table_catalog}'' -- Add current database filter for precision 
                                    AND TABLE_SCHEMA =''{table_schema}'' AND TABLE_NAME=''{table_name_from_ai_json}'' 
                                    AND COLUMN_NAME=''{column_name}'';""";
            
                is_column_comment_null = session.sql(is_column_comment).collect()[0][0];
                print(''Column comment check-------'')
                print(is_column_comment_null)
                print(is_column_comment_null[0])
                print(type(is_column_comment_null))

                # Only apply the column descriptions to columns if the flag apply_descriptions_to_table_and_columns is Y else skip this part
                if apply_descriptions_to_table_and_columns==''Y'' and is_column_comment_null==''Y'':
                    set_column_comment_sql = f""" ALTER TABLE {table_catalog}.{table_schema}.{table_name_from_ai_json}
                                                  MODIFY COLUMN {column_name} COMMENT ''{column_description}'' """;

                    session.sql(set_column_comment_sql).collect()

                if apply_descriptions_to_table_and_columns==''N'' and column_name and column_description and is_column_comment_null==''Y'':
                    all_columns_data_for_insert.append([''COLUMN'', column_description, column_name, table_catalog, table_schema, table_name_from_ai_json,ingested_timestamp])
                elif column_name and column_description and is_column_comment_null==''Y'': # Only insert if we have name and description & if the comment doesn''t exist
                    all_columns_data_for_insert.append([''COLUMN'', column_description, column_name, table_catalog, table_schema, table_name_from_ai_json,ingested_timestamp])
                else:
                    print(f"Warning: Missing essential column description or name in AI_GENERATE_TABLE_DESC output for table ''{table_name_from_ai_json}'' or the column description already exists. Column: ''{column_name}''. Skipping.")

        
            # --- Final Bulk Column Insertion after processing all tables ---
            if all_columns_data_for_insert:
                # Define schema for the Snowpark DataFrame to be inserted
                column_insert_schema = StructType([
                                        StructField("domain", StringType()),
                                        StructField("description", StringType()),
                                        StructField("name", StringType()),
                                        StructField("database_name", StringType()),
                                        StructField("schema_name", StringType()),
                                        StructField("table_name", StringType()),
                                        StructField("ingested_timestamp", TimestampType())
                                        ])
            
                # Create a Snowpark DataFrame from the collected list of column data
                column_df_to_insert = session.create_dataframe(all_columns_data_for_insert, schema=column_insert_schema)
            
                # Write to the catalog_table in append mode (most efficient way for bulk inserts)
                column_df_to_insert.write.mode("append").save_as_table("ai_generated_description_catalog")
            else:
                print("No column data to insert into catalog_table.")
    
    return ''Success'';';