class Schema(object): #s_
    
    def __init__(self, s_name):
        self.s_name = s_name
    
    def schema_name(self):
        return self.s_name
   
class Table(Schema): #t_
    
    def __init__(self, s_name, t_name, t_type):
        super(Table, self).__init__(s_name)
        self.t_name = t_name 
        self.t_type = t_type
        #owner
        
    def table_name(self):
        return self.t_name
    
    def table_type(self):
        return self.t_type
    
    #PGTAP TEST STRINGS
    def has_table(self):
        #SELECT has_table( :schema, :table );
        if self.t_type == 'VIEW':
            return 'SELECT has_table( :{0}, :{1} );'.format(self.s_name, self.t_name)
        else:
            return 'SELECT has_view( :{0}, :{1}, {2} );'.format(self.s_name, self.t_name, None)

class Column(Table): #c_
    
    def __init__(self, s_name, t_name, t_type, c_name, c_datatype, c_ordinal_pos, c_nullable, c_default):
        super(Column, self).__init__(s_name, t_name, t_type)
        self.c_name = c_name 
        self.c_datatype = c_datatype
        self.c_ordinal_pos = c_ordinal_pos
        self.c_nullable = c_nullable  
        self.c_default = c_default

    def column_name(self):
        return self.c_name
        
    
    #PGTAP TEST STRINGS

    def has_column(self):
        # SELECT has_column( :schema, :table, :column, :description );
        return 'SELECT has_column( :{0}, :{1}, :{2}, :{3} );'.format(
                    self.s_name, self.t_name, self.c_name, None)
        
    def type_is(self):
        # SELECT col_type_is( :schema, :table, :column, :type, :description );
        return 'SELECT col_type_is( :{0}, :{1}, :{2}, :{3}, {4} );'.format(
                    self.s_name, self.t_name, self.c_name, self.c_datatype ,None)
    
    def is_null(self):
        if self.c_nullable == 'NO':
            #SELECT col_not_null( :schema, :table, :column, :description );
            return 'SELECT col_not_null( :{0}, :{1}, :{2}, :{3} );'.format(
                        self.s_name, self.t_name, self.c_name, None)
        else: 
            #SELECT col_is_null( :schema, :table, :column, :description );
            return 'SELECT col_is_null( :{0}, :{1}, :{2}, :{3} );'.format(
                        self.s_name, self.t_name, self.c_name, None)
    
    def has_default(self):
        if self.c_default:
            #SELECT col_default_is( :schema, :table, :column, :default, :description );
            return 'SELECT col_default_is( :{0}, :{1}, :{2}, :{3} );'.format(
                        self.s_name, self.t_name, self.c_name, self.c_default, None)
        else:
            # SELECT col_hasnt_default( :schema, :table, :column, :description );
            return 'SELECT col_hasnt_default( :{0}, :{1}, :{2}, :{3} );'.format(
                        self.s_name, self.t_name, self.c_name, None)
        
    

    


            
        
    

                                        
class Index(Table): #i
    def __init__(self):
        pass

class Function(Table): #f
    def __init__(self):
        pass

class Keys(Table):
    pass
    #or is this part of table?






#Tests        
s_name = 'schema1'
t_name = 'table1'
t_type = 'view'
c_name = 'col1'
c_datatype = 'char_var'
c_ordinal_pos = '1'

#t = Table(s_name, t_name, t_type)
#col = Column(s_name, t_name, t_type, c_name, c_datatype, c_ordinal_pos)