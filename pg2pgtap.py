import  sys, os, yaml, psycopg2, logging, sql, Db_object

def exec_sql(sql, cur, return_table):
    
    try:
        cur.execute(sql)
    except psycopg2.Error as e:
        return False

    if return_table:
        return cur.fetchall()

    return True

def main():
    '''Main entrypoint for the pg database > pgtap script'''
    
    # --------------------------------------------
    # -- Configuration
    # --------------------------------------------
    
    with open('config.yaml', 'r') as f:
        config = yaml.load(f)
        
    # Schema to evaluate
    if 'Schema' in config:
        schema_list = config['Schema']
    else:
        print 'CONFIG ERROR: No "Schema" section'
        sys.exit(-1)    

    if 'Connection' in config:
        host = config['Connection']['Host']
        db = config['Connection']['Database']
        user = config['Connection']['User']
        pwd = config['Connection']['Password']
    else:
        print 'CONFIG ERROR: No database "Connection" section'
        sys.exit(-1)
    
    #output
    if 'output' in config:
        output_dest = config['output']['Destination']
    else: 
        log_dest = ''
        
    # --------------------------------------------
    # -- DB Queries & Python objects rep of pg db
    # --------------------------------------------
    
    # Connect to database
    conn_string = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format(host,db, user, pwd)
    conn = psycopg2.connect(conn_string)
    
    # get the cursor
    cur = conn.cursor() # WITH!
    
    # lists of db objects
    schema_objs, table_objs, column_objs, = [], [], []
    
    #iterate over schema in config to be evaluated
    for schema in schema_list:
        schema_objs.append(Db_object.Schema(schema))
        
        #iterate over tables
        sql_query = sql.sql_queries('get_tables', schema)
        row = exec_sql(sql_query, cur, True)
        for table in row: 
            # table = [(table_name, table_type), ...]
            table_objs.append(Db_object.Table(schema, table[0], table[1]))
    
        for table in table_objs:
            sql_query = sql.sql_queries('get_columns', schema, table.table_name() )
            row = exec_sql(sql_query,cur, True)
            # create / store ref, column objects
            for column in row:
                # column[x] = [column_name, column_type, c_ordinal_pos, is_nullable, column_defualt] # may remove ordianil_pos
                column_objs.append(Db_object.Column(schema, table.table_name(),  table.table_type(), column[0], column[1], column[2], column[3], column[4]))
    
    # --------------------------------------------
    # -- Write out PGTAP tests
    # --------------------------------------------         
    s_tests, t_tests, c_tests = 0, 0, 0
    
    # open output file and add header
    with open(output_dest, 'w') as text_file:
        text_file.write('\set ECHO')
        text_file.write('\set QUIET 1')
        text_file.write('-- Turn off echo and keep things quiet.')
        text_file.write('\n-- Format the output for nice TAP.')
        text_file.write('\n\pset format unaligned')
        text_file.write('\pset tuples_only true')
        text_file.write('\pset pager')
        text_file.write('\n-- Revert all changes on failure.')
        text_file.write('\set ON_ERROR_ROLLBACK 1')
        text_file.write('--\set ON_ERROR_STOP true')
        text_file.write('\set QUIET 1')
        text_file.write('\n-- Load the TAP functions')
        text_file.write('BEGIN;')
        text_file.write('\n\i pgtap.sql')
                                    
            
        # Schema Tests
        for s in schema_objs:
            s.has_schema()
            
            s_tests += 1
            
        #Table Tests        
        for t in table_objs:
            text_file.write('{0} \n'.format( t.has_table()) )
            
            t_tests += 1
    
        # Column Tests
        col_tests = ['has_default', 'is_null', 'type_is', 'has_column'] 
        
        for c in column_objs:
            c_header = '\n-- Column >>> {0}.{1}.{2}'.format(
                            c.schema_name(), c.table_name(), c.column_name())
            
            text_file.write( '{0} \n'.format( c_header ) )
            
            for test in col_tests:    
                test_method = getattr(c, test)
                text_file.write( '{0}\n'.format(test_method()) )
                
                c_tests += 1

        text_file.write( '\n{0} Schema tests'.format( s_tests ))    
        text_file.write( '\n{0} Table tests'.format( t_tests ))
        text_file.write( '\n{0} Column tests'.format( c_tests ))

#SELECT * FROM finish();

#ROLLBACK;

if __name__ == '__main__':
    main()

