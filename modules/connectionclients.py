import MySQLdb


import sys
import datetime

try:
    import paramiko
    import fdb
except ImportError:
    pass

class fdbclient(object): #for connecting to firebird DB
    def __init__(self,dsn,user,pw):
        self.connection = self.connect(dsn,user,pw)
    
    @staticmethod
    def connect(dsn,user,pw):
        try:
            fishbowldb = fdb.connect(dsn=dsn, user=user, password=pw)
            print '\nConnected to FDB\n'
        except fdb.Error as e:
            print '\nError connecting to the Firebird DB: '+ str(e)+'\n'
            sys.exit(1)
        return fishbowldb

    def execute(self, query):
  
        cursor = self.connection.cursor() #no rollback or commit needed, read only
        cursor.execute(query)
        results= cursor.fetchall()
        return results
    def close(self):
        self.connection.close()


class mysqlclient(object): #for connecting to MySQL DB
    def __init__(self,host,user,pw,db,port):
        self.connection = self.connect(host,user,pw,db,port)
    @staticmethod
    def connect(host,user,pw,db,port):
        try:
            conn = MySQLdb.connect(host=host, user=user, passwd=pw, db=db, port=int(port))
            print '\nConnected to '+db+'\n'
        except MySQLdb.Error as e:
            print '\nError connecting to'+db+': '+ str(e)+'\n'
            sys.exit(1)
        return conn
    def execute(self, query):
        results = []
        errors = []
        cursor = self.connection.cursor()
        start_time = str(datetime.datetime.now().time())
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            self.connection.commit()
        except MySQLdb.Error as e:
            self.connection.rollback() 
            errors = (str(e))
        return results, errors #list of row tuples, list of error strings
    def close(self):
        self.connection.close()
    
class sshclient(object): #for SSH connection, to be used in conjunction with mysqlclient for webserver
    def __init__(self,host,user, key):
        self.connection = self.connect(host,user,key)

    @staticmethod
    def connect(host,user,key):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=user, key_filename=key)
        print '\nConnected to '+host+'\n'
        return ssh
        
    def execute(self,command):
        return self.connection.exec_command(command)
    def close(self):
        self.connection.close()
        print '\nSSH connection closed.\n'
