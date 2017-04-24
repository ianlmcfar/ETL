from modules import connectionclients
from modules import configparser as config

try:
    import pandas as pd
    import numpy as np
except:
    pass

class table(object):
    def __init__(self, name, connection, fields_query, content_query, limit):
        self.fields_query = fields_query
        self.content_query = content_query+name+limit
        self.table_name = name
        self.mysql = connection
        self.dataframe = self.get_dataframe()
    def get_dataframe(self):
        data = pd.read_sql(self.content_query, self.mysql.connection) #reads table to dataframe
        print self.table_name + str(data.shape)
        data = data.fillna('NULL')
        return data

def make_tables(connection, tables_query, fields_query, content_query):
    tables = [t[0] for t in connection.execute(tables_query)[0]]
    table_dict = dict()
    for tbl in tables:
        print "Starting Query for "+tbl
        if tbl == 'so' or tbl == 'soitem' or tbl == 'postsoitem' or tbl == 'postso':
            table_dict[tbl] = table(tbl, connection, fields_query, "select * from ", " where datelastmodified >= date_add(curdate(),interval -1 day)")
            print "limited"
        else:
            table_dict[tbl] = table(tbl, connection, fields_query, content_query, "")
            print 'all'
    return table_dict

def normalize_table(table_dict): #splits certification multiselect ids to create new row
        table_df = table_dict['catalog_product_entity_varchar'].dataframe
        df1 = table_df[(table_df['attribute_id'] == 180) & table_df['value'].str.contains(',') ]
        s = df1['value'].str.split(',').apply(pd.Series,1).stack()
        s.index = s.index.droplevel(-1)
        s.name = 'value'
        dfa=  table_df[(table_df['attribute_id'] == 180) & table_df['value'].str.contains(',') ]
        dfb=  table_df[~((table_df['attribute_id'] == 180) & table_df['value'].str.contains(','))]
        del dfa['value']
        dffinal = dfa.join(s).append(dfb, ignore_index=True)
        print dffinal.shape
        table_dict['catalog_product_entity_varchar'].dataframe = dffinal
        return table_dict
