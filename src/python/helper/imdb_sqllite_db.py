import sqlite3
from helper.singleton import SingletonDecorator


class __SQLLiteDB:
    
    def __init__(self, sqlFilePath = None):       
        if(sqlFilePath == None):
            self.__sqlFilePath = './data/imdb.sqllite'
        else:
            self.__sqlFilePath = sqlFilePath
            
        print ('IMDB SQLLite Database: {}'.format(self.__sqlFilePath))    
        self.__connection =  sqlite3.connect(self.__sqlFilePath)
        self.__cursor =  self.__connection.cursor()
    
    def execute(self, cmd):
        try:
            if(self.__cursor):            
                self.__cursor.execute(cmd)
        except Exception as ex:
            print('Error: {}'.format(ex))
            raise ex
            
    def commit(self):
        if(self.__connection):
            self.__connection.commit()

    def close(self):
        if(self.__connection):
            self.__connection.close()

class IMDBSQLLite: pass
IMDBSQLLite = SingletonDecorator(__SQLLiteDB)      