import sqlite3
from helper.singleton import SingletonDecorator
import config;
import pandas


class __SQLLiteDB:
    
    
    
    def __init__(self, sqlFilePath = None):       
        if(sqlFilePath == None):
            self.__sqlFilePath = config.SQLLITE_CONFIG['FILE_PATH']
        else:
            self.__sqlFilePath = sqlFilePath

        print ('IMDB SQLLite Database: {}'.format(self.__sqlFilePath))    
        self.__connection =  sqlite3.connect(self.__sqlFilePath)
        self.__cursor =  self.__connection.cursor()
    
    def execute(self, cmd):
        print ('EXECUTE: {}'.format(cmd))
        try:
            if(self.__cursor):            
                return self.__cursor.execute(cmd)
        except Exception as ex:
            print('EXECUTE (error): "{}": [{}]'.format(cmd, ex))
            raise ex
            
    def commit(self):
        if(self.__connection):
            self.__connection.commit()

    def close(self):
        if(self.__connection):
            self.__connection.close()
    
    def get(self):
        return self.__connection, self.__cursor

    def rowCount(self, tableName):
        query = config.SQLLITE_CONFIG['QUERY']['table_count'].format(tableName= tableName)
        try:
            return self.query(query).iloc[0].to_dict()                  
        except Exception as ex:
            print('ROWCOUNT: "{}": [{}]'.format(query, ex))
            raise ex
        
    def query(self, query):
        #query = "select  count(1) count from {}".format(tableName)        
        try:
            df_query = pandas.read_sql_query(query, self.__connection)
            return df_query
        except Exception as ex:
            print('QUERY (error): "{}": [{}['.format(query, ex))
            raise ex
            
    def dropIndex(self, indexName):
        query = config.SQLLITE_CONFIG['QUERY']['drop_index'].format(indexName= indexName)
        try:
            self.execute(query)
        except Exception as ex:
            print('DROPINDEX (error): "{}": [{}['.format(query, ex))
            raise ex
        
    def createIndex(self, tableName, indexName, columns, isUnique = False, drop = True):
        if(isUnique):
            query = config.SQLLITE_CONFIG['QUERY']['create_unqiue_index'].format(indexName = indexName, 
                                                                                 tableName= tableName, columns = columns)
        else:
            query = config.SQLLITE_CONFIG['QUERY']['create_index'].format(indexName = indexName, 
                                                                          tableName= tableName, columns = columns)
        try:
            if(drop):
                self.dropIndex(indexName)
                
            self.execute(query)
            
        except Exception as ex:
            print('CREATEINDEX (error): "{}": [{}['.format(query, ex))
            raise ex

class IMDBSQLLite: pass
IMDBSQLLite = SingletonDecorator(__SQLLiteDB)      