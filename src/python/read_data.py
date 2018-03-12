import pandas as pandas
import numpy as numpy
#import matplotlib.pyplot as plt
#import seaborn as sns
#import scipy.stats as st
#import sys 
#import datetime as dt
import time as time
import gc as gc


LESSON_DATA_FOLDER = './data/'

#from helper.parallel_util import ParallelUtil
from helper.parsers import Utils
from helper.file_mapping import FILE_MAPPINGS


def callFunction(columnData, **funDict):
    
    converFuns = funDict[columnData.name] #[{'function':split, 'args':(',')}]
    
    if(converFuns != None):
        for funSpec in converFuns:
            params = funSpec['args']
            #print ('args: ', params, ' <> ', params == None)
            if(params == None):                
                columnData = columnData.apply(funSpec['function'] )
            else:            
                columnData = columnData.apply(funSpec['function'], args=params )
            
    return columnData

def readFile(file, nrows=None, cluster=None ):   
    print ('Start: ' + time.strftime("%Y-%m-%d %H:%M:%S"))
    mapping = FILE_MAPPINGS.getMapping(file)
    dtype = mapping['dtype']
        
    
    if(mapping['filePath'] == None):        
        filePath = ''.join([LESSON_DATA_FOLDER, file])
    else: 
        filePath = ''.join([LESSON_DATA_FOLDER, mapping['filePath']])
        
    usecols = list(dtype.keys())
    
    print('{}'.format(usecols))
    
    if(cluster != None):
        dview = cluster[:]
        dview.scatter(
            "df", 
            pandas.read_table(filePath, 
                           index_col=mapping['index_col'], 
                           dtype = dtype, 
                           #na_values = ['//N'],
                           true_values= mapping['true_values'],
                           false_values= mapping['false_values'],                       
                           usecols=usecols,
                           nrows =nrows,
                           #encoding = 'ascii'
                          )
        )
        df = pandas.concat([i for i in dview["df"]])
    else:
        df = pandas.read_table(filePath, 
                           index_col=mapping['index_col'], 
                           dtype = dtype, 
                           #na_values = ['//N'],
                           true_values= mapping['true_values'],
                           false_values= mapping['false_values'],                       
                           usecols=usecols,
                           nrows =nrows,
                           #encoding = 'ascii'
                          )
        
    
    df.fillna(method='pad', inplace=True)
    if(mapping['to_replace']!= None):
        df.replace(to_replace=mapping['to_replace'],method='pad', inplace=True)
    
    converters = mapping['converters']
    if(converters!= None):
        cols = list(converters.keys())
        df[cols] = df[cols].apply(callFunction, **converters)
        
    #mem_usage = df.memory_usage(index=True, deep=True)       
    print ('End: {}'.format(time.strftime("%Y-%m-%d %H:%M:%S"))  )
    return df

def merge_names_principals(mergeFilePath, conn, nrows = 10):    
    names = readFile('name.basics.tsv', nrows = nrows)

    names['isAlive'] = names['deathYear'] == 0
    print ('Alive Done: ' + time.strftime("%Y-%m-%d %H:%M:%S"))

    names['wiki'] = names['primaryName'].apply(Utils.wikiLink)
    print ('wikipedia Done: ' + time.strftime("%Y-%m-%d %H:%M:%S"))
    #print (names['wiki']);
    #names.info()


    '''

    names[['lastName', 'firstName']] = names['primaryName'].apply(lambda x: pd.Series(str(x).lower().split(' ', 1)))
    print ('Name split Done: ' + time.strftime("%Y-%m-%d %H:%M:%S"))


    '''

    title_principals = readFile('title.principals.tsv', nrows = nrows) 
    #title_principals.info()

    merged_names_principals = title_principals.merge(names, on=['nconst'], how='left')
    print ('Merge done: {}'.format(time.strftime("%Y-%m-%d %H:%M:%S"))  )
        
    if(mergeFilePath == None):
        if(conn == None): 
            mergeFilePath = ''.join([LESSON_DATA_FOLDER, 'merged.names.principals.tsv'])                    
            
                            
    if(conn == None):
        merged_names_principals.to_csv(mergeFilePath, '\t', index = False)
        print ('To FILE {} done: {}'.format(mergeFilePath, time.strftime("%Y-%m-%d %H:%M:%S"))  )
    else:
        merged_names_principals.to_sql("merged_names_principals", conn, if_exists="replace", index = False)
        print ('To TABLE {} done: {}'.format('merged_names_principals', time.strftime("%Y-%m-%d %H:%M:%S"))  )    

    #merged_names_principals.to_json(path_or_buf='./data/merged.names.principals.json', orient= 'records' )

    collection_dtype = merged_names_principals.dtypes
    preview = {key: collection_dtype[key] for key in list(collection_dtype.keys())}
    print (preview)

    del [title_principals, names, merged_names_principals]
    gc.collect()

    
def merge_title_ratings(mergeFilePath, conn, nrows = 10):
    
    ratings = readFile('title.ratings.tsv', nrows = nrows)
    #ratings.info()
    titles = readFile('title.basics.tsv', nrows = nrows)
    
    titles['wikiTitle'] = titles['primaryTitle'].apply(Utils.wikiLink)
    print ('wikipedia Done: ' + time.strftime("%Y-%m-%d %H:%M:%S"))

    #titles.info()
    #titles.tail()
    #titles[titles['primaryTitle'] != titles['originalTitle']][['wikipedia','originalTitle','primaryTitle']].to_dict('record')

    merged_titles_ratings = titles.merge(ratings, on=['tconst'], how='left')
    print ('Merge done: {}'.format(time.strftime("%Y-%m-%d %H:%M:%S"))  )
    
    if(mergeFilePath == None):
        if(conn == None): 
            mergeFilePath = ''.join([LESSON_DATA_FOLDER, 'merged.title.ratings.tsv'])                    
            
                            
    if(conn == None):
        merged_titles_ratings.to_csv(mergeFilePath, '\t', index = False)
        print ('To FILE {} done: {}'.format(mergeFilePath, time.strftime("%Y-%m-%d %H:%M:%S"))  )
    else:
        merged_titles_ratings.to_sql("merged_titles_ratings", conn, if_exists="replace", index = False)
        print ('To TABLE {} done: {}'.format('merged_titles_ratings', time.strftime("%Y-%m-%d %H:%M:%S"))  )
    
    #merged_titles_ratings.to_json('./data/merged.title.ratings.json')
    #titles[titles['primaryTitle'] != titles['originalTitle']].head(5)
    #titles.info()
    collection_dtype = merged_titles_ratings.dtypes
    preview = {key: collection_dtype[key] for key in list(collection_dtype.keys())}
    print (preview)
    del [titles, ratings, merged_titles_ratings]
    gc.collect()
    
def mergeAll(nrows = 10):
    merge_title_ratings(mergeFilePath = ''.join([LESSON_DATA_FOLDER, 'merged.title.ratings.tsv']), conn= None, nrows = nrows)
    merge_names_principals(mergeFilePath = ''.join([LESSON_DATA_FOLDER, 'merged.names.principals.tsv']), conn= None, nrows = nrows)
    
def mergeAllToTable(conn, nrows = 10):    
    merge_title_ratings(mergeFilePath = None, conn= conn, nrows = nrows)
    merge_names_principals(mergeFilePath = None, conn= conn, nrows = nrows)
    

    