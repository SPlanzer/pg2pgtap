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
    
    with open('config.yaml', 'r') as f:
        config = yaml.load(f)
        
    #Schema to evaluate
    if "Schema" in config:
        schema_list = config["Schema"]
    else:
        print 'CONFIG ERROR: No "Schema" section'
        sys.exit(-1)    
        
    # Get config
    with open('config.yaml', 'r') as f:
        config = yaml.load(f)
    
    if "Connection" in config:
        host = config["Connection"]["Host"]
        db = config["Connection"]["Database"]
        user = config["Connection"]["User"]
        pwd = config["Connection"]["Password"]
    else:
        print 'CONFIG ERROR: No database "Connection" section'
        sys.exit(-1)
     
    # Connect to database
    conn_string = "host='{0}' dbname='{1}' user='{2}' password='{3}'".format(host,db, user, pwd)
    conn = psycopg2.connect(conn_string)
    # get the cursor
    cur = conn.cursor()
    
    # lists of objects
    schema_objs = []
    table_objs = []
    column_objs = []
    
    #iterate over schema in config to be evaluated
    for schema in schema_list:
        # create / store ref schema, objects
        schema_objs.append(Db_object.Schema(schema))
        
        sql_query = sql.sql_queries('get_tables', schema)
        row = exec_sql(sql_query, cur, True)
        # create / store ref table, objects
        for table in row: 
            # table = [table_name, table_type]
            table_objs.append(Db_object.Table(schema, table[0], table[1]))
    
        for table in table_objs:
            sql_query = sql.sql_queries('get_columns', schema, table.table_name() )
            row = exec_sql(sql_query,cur, True)
            # create / store ref, column objects
            for column in row:
                # column[x] = [column_name, column_type, c_ordinal_pos, is_nullable, column_defualt] # may remove ordianil_pos
                column_objs.append(Db_object.Column(schema, table.table_name(),  table.table_type(), column[0], column[1], column[2], column[3], column[4]))
                
            
    #for t in table_objs:
    #    print t.has_table()

    col_tests = ['has_default', 'is_null', 'type_is', 'has_column'] 
    
    for c in column_objs:
        print '\n-- Column >>> {0}.{1}'.format(c.table_name(), c.column_name())
        for test in col_tests:    
            func = getattr(c, test)
            print func()
    

            
    

if __name__ == "__main__":
    main()

