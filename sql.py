def sql_queries(query_name, schema, table = None):
    ''' Returns a SQL query string as 
    related to the parameter query_name'''
    
    sql_dict = {
        'get_tables':'''SELECT table_name, table_type FROM information_schema.tables 
                        WHERE table_schema = '{0}' '''.format(schema),
        
        'get_columns': '''SELECT  column_name, data_type, ordinal_position, is_nullable, column_default 
                        FROM information_schema.columns 
                        WHERE table_schema = '{0}'
                        AND table_name= '{1}' '''.format(schema, table),
                            
        'get_views': '''SELECT table_name 
                        FROM information_schema.views 
                        WHERE table_schema = '{0}' '''.format(schema),
                        
        'get_functions': '''SELECT routines.routine_name, parameters.data_type, parameters.ordinal_position
                        FROM information_schema.routines
                        JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name
                        WHERE routines.specific_schema = '{0}' '''.format(schema)
        
                }
    
    return sql_dict.get(query_name, None) # perhaps we will just return the dict to iterate over


#need to also check owners ...


