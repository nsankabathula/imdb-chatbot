from helper.singleton import SingletonDecorator
import ipyparallel
import pandas

#k = myModule.SingletonDecorator()

class _ParallelUtil:
            
    def __init__(self, configFilePath):        
        self.__CLUSTER_REQUESTS = 0
        self.__CLUSTER_FAILURES = []      
        self.__CLUSTERS = None
        self.getCluster()
        self.configFilePath = configFilePath
        
    @staticmethod
    def __PARALLEL_CLUSTERS(configFilePath = None, throwError = False):
        try:
            if(configFilePath == None):
                configFilePath = ''.join([LESSON_DATA_FOLDER, 'parallel.config.tsv'])
            PARALLEL_CONFIG = pandas.read_table(configFilePath)
            PARALLEL_CONFIG.replace(to_replace={'None':None},method='pad', inplace=True)
            default_config = PARALLEL_CONFIG[PARALLEL_CONFIG['default'] == True].to_dict('records')[0]
            default_config


            CLUSTERs = ipyparallel.Client(profile=default_config['profile'],sshserver=default_config['sshserver'],
                                          password=default_config['password'])
            print('PROFILE: {} || IDS: {} '.format(CLUSTERs.profile, CLUSTERs.ids))
            #print("IDs:", CLUSTER.ids) # Print process id numbers
    
            return CLUSTERs
        except Exception as ex:            
            print (ex)
            if(throwError) :
                raise
            else:
                pass
                return None
            
                
    def setImports(self):
        clusters = self.getCluster()

        dview = clusters[:]
        with dview.sync_imports():
            import pandas
            import numpy
            import datetime
            import time
                     
    def getCluster(self):
        if(self.__CLUSTERS == None):
            try:
                self.__CLUSTER_REQUESTS  = self.__CLUSTER_REQUESTS + 1
                self.__CLUSTERS = _ParallelUtil.__PARALLEL_CLUSTERS(configFilePath = self.configFilePath, throwError= True)
            except Exception as ex:   
                self.__CLUSTER_FAILURES.append(ex)
                pass
            
        return self.__CLUSTERS
    
    def info(self):
        print ('CLUSTER_REQUESTS: {}, CLUSTER_FAILURES: {}'.format(self.__CLUSTER_REQUESTS, self.__CLUSTER_FAILURES))

class ParallelUtil: pass
ParallelUtil = SingletonDecorator(_ParallelUtil)  
