from elasticsearch import Elasticsearch
from elasticsearch import helpers as esHelper
from helper.singleton import SingletonDecorator

class __IESHelper:
    def connection(self): pass
    def info(self): pass
    def bulk_stream(self, iterator):pass
    def create_index(self, index, doc_type, mapping, drop_create = False):pass
    
class __ESHelper(__IESHelper):
    def __init__(self, config):       
        self.__esconfig = config
        self.__esconnection = Elasticsearch( hosts= self.__esconfig['host'])
                
    def connection(self):
        return self.__esconnection;

    
    def create_index(self, index, doc_type, mapping, drop_create = False):
        if(drop_create == True):
            self.__esconnection.indices.delete(index=index, ignore=[400, 404])
           
        self.__esconnection.indices.create(index=index, ignore=400)
        
        if(mapping != None and doc_type != None):
            self.__esconnection.indices.put_mapping(index=index, doc_type=doc_type, body=mapping)
    
    def bulk_stream(self, iterator, failureThreshold = 100):
        success  = 0;
        failure = 0;
        for ok, result in esHelper.streaming_bulk(self.__esconnection, iterator, raise_on_error = False):
            if not ok:
                failure = failure + 1                                    
                __, result = result.popitem()
                if result['status'] == 409:
                    print('Duplicate event detected, skipping it: {}'.format (result))
                else:
                    print('Failed to record event: {}'.format(result))
                
                if(failure >= failureThreshold): 
                    print('Reached threshold.')
                    raise result;
            else:
                success = success + 1;
                
        print('Success count: {}, Failure count: {}'.format(success, failure))
           
    def info(self):
        return self.__esconnection.info()
    
                    

class ESHelper: pass
ESHelper = SingletonDecorator(__ESHelper)
                
                