import subprocess
import os
import csv
try:
    import pandas as pd
    import numpy as np
except ImportError:
    pass
def sync_to(host):
    subprocess.call(host, shell=True)
    print '***rsync executed successfully***'
        
    
class load_tables(object):
    def __init__(self, mysqlconnection, tables, fbconnection=None, structure=None):
        self.tables = tables
        self.mysql = mysqlconnection
        if fbconnection != None:
            self.structure = structure
            self.fishbowl = fbconnection 
            self.fbdata = self.read_fdb(fbconnection)
            self.magdata = self.read_csv()
        
    def read_fdb(self,fbconnection):
        data = dict()
        for table in self.tables:
            limit = " where datelastmodified >= date_add(current_date, interval -2 month)" if (table == 'so' or table == 'soitem' or table == 'postsoitem' or table == 'postso') else ''
            # content = pd.read_sql("SELECT * FROM "+table, fbconnection.connection).astype(object).replace(np.nan, 'NULL')
            query = "SELECT * FROM "+table + limit
            print "Starting Query:\n    "+query
            content = pd.read_sql(query, fbconnection.connection).astype(object).fillna('NULL')
            data[table] = content #list of row tuples
            data[table].columns = [x.lower() for x in data[table].columns]
            print 'Finished SQL for ' + table + ' table on fb'
        fbconnection.close()
        print 'FB connection closed'
        return data
        
    def read_csv(self):
        path = '/var/local/dev/pythondev/data/table_data/'
        directory = os.listdir(path)
        data = dict()
        for filename in directory:
            filestr = filename.split('.')[0]
            # data[filestr] = pd.read_csv(path+filename).astype(object).replace(np.nan, 'NULL')
            data[filestr] = pd.read_csv(path+filename).astype(object).fillna('NULL')
            # print 'Finished reading '+filestr+' to memory'
        return data
    
    def upsert(self, secondary=None):
        try: 
            self.fbdata
        except AttributeError:
            self.data = secondary
            database = 'warehouse'
        else:
            self.data = self.fbdata.copy()
            self.data.update(self.magdata)
            database = 'staging'
            
        error_log = dict()
        for table in self.data:
            error_log[table] = []
            fields = list(self.data[table].columns.values)
            insert = "INSERT INTO "+table.lower()+' ('+str(fields)[1:-1].lower().replace("'","")  + ") VALUES (" + ",".join(["{"+str(fields.index(field))+"}" for field in fields])+")"
            update = "ON DUPLICATE KEY UPDATE "+ ",".join([field.lower() +"={"+str(fields.index(field))+"}" for field in fields])
            query = insert+update
            print table
            if (table.lower() != 'so' and table.lower() != 'soitem' and table != 'postsoitem' and table != 'postso'):
                primary_key = self.mysql.execute("show index from "+table.lower()+" where Key_name = 'PRIMARY' ")[0][0][4]
                tuples, error = self.mysql.execute("select "+primary_key+" from "+table.lower())
                table_ids = [e for t1 in tuples for e in t1]
  
                to_delete = list(set(table_ids)-set(self.data[table][primary_key].values.tolist()))
                if len(to_delete)>0:
                    if type(to_delete[0]) is str:
                        where = " where "+primary_key+" in ("+', '.join(map(lambda x: "'"+str(x)+"'", to_delete)) +")"
                    else:
                        where = " where "+primary_key+" in ("+', '.join(map(str, to_delete)) +")"
                    print "delete from "+table.lower()+where
                    results, error = self.mysql.execute("delete from "+table.lower()+where)
                    print error
            for index, row in self.data[table].iterrows():
                rowlist = ["'"+str(item).replace("'",r"\'")+"'" if item != 'NULL' else item for item in row]
                results, error = self.mysql.execute(query.format(*rowlist))
                if len(error) > 3:
                    error_log[table].append({'error': error, 'statement': query.format(*rowlist)})
            print '\n\n***Updated '+table+' table to '+ database+' with '+str(len(error_log[table]))+' errors***\n\n'
            if len(error_log[table]) > 0:
                for litem in error_log[table]:
                    print '\n\nERROR: '+litem['error'] + '\n\n STATEMENT: ' + litem['statement']


class load_tables_warehouse(object):
    def __init__(self, mysqlconnection, tables):
        self.tables = tables
        self.mysql = mysqlconnection
        self.composed = load_tables(mysqlconnection,tables)
    def upsert(self):
        table_content = dict()
        for table in self.tables:
            table_content[table] = self.tables[table].dataframe
        # table_content['product'] = self.tables['product'].dataframe
        self.composed.upsert(table_content)

