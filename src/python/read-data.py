import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys as sys
from datetime import datetime as dt


def parse_date(date):
    if str(date) == '' or date == None:
        return None
    else:
        return dt.strptime(date,'%Y-%m-%d')    

def parse_int(i):
    if (str(i) == '' or i == None  or str(i) == 'NaN') :
        return None   
    else:
        return int(i)
    
def parse_float(f):
    if str(f) == '' or f == None:
        return None
    else:
        return float(f)
    
def parse_bool(boolean):
    if str(boolean) == '' or boolean == None:
        return None
    else:        
        return boolean =='True' 
    
def split(data, delimiter=','):
    if(data == '' or data == None):
        return None
    else:
        return str(data).lower().split(delimiter)    

def lower(data):
    if(data == '' or data == None):
        return None
    else:
        return str(data).lower() 
    
def upper(data):
    if(data == '' or data == None):
        return None
    else:
        return str(data).upper()     

def replaceNaN(data):
    if(np.isnan(data)): 
        return None
    else:
        return data

LESSON_DATA_FOLDER = './data/'

fileColumnMapping = {
'title.basics.tsv': {
      'index_col': None, 
      'dtype' : {'tconst':np.dtype('S'),
                 'titleType':np.dtype('S'), 
                 'primaryTitle':np.dtype('S'),
                 'originalTitle':np.dtype('S'),
                 'isAdult':np.dtype('S'),
                 'startYear':np.dtype('S'),
                 'endYear':np.dtype('S'),             
                 'runtimeMinutes':np.dtype('S'),
                 'genres':np.dtype('S')
                    },      
      'filePath':LESSON_DATA_FOLDER + 'title.basics.tsv',
      'to_replace':{
          'titleType':{'\\N':None},
          'primaryTitle':{'\\N':None},
          'originalTitle':{'\\N':None},
          'startYear':{'\\N':None},
          'endYear':{'\\N':None},
          'runtimeMinutes':{'\\N':None}
      },
     'true_values':['1'],
     'false_values':['0'],     
     'usecols':['tconst','titleType','primaryTitle','originalTitle','isAdult','startYear','endYear','runtimeMinutes','genres'],
     'converters' : {
                 'primaryTitle':[lower],
                 'titleType':[lower],
                 'originalTitle':[lower],
                 'isAdult':[parse_bool],
                 'startYear':[parse_int] ,
                 'endYear':[parse_int]  ,
                 'runtimeMinutes':[parse_int],
                 'genres': [split]
                }
    },
'title.crew.tsv': {
      'index_col': None, 
      'dtype' : {'tconst':np.dtype('S'),'directors':np.dtype('S') ,'writers':np.dtype('S')  },
      'split' : ['directors','writers'],
      'filePath':LESSON_DATA_FOLDER + 'title.crew.tsv',
      'to_replace':{
          'directors':{'\\N':None},
          'writers':{'\\N':None},          
      },
     'true_values':None,
     'false_values':None,     
     'usecols': None,
     'converters' : {                 
                 'writers':[split],
                 'directors': [split]
                }
    }, 
'title.episode.tsv': {
      'index_col': None, 
      'dtype' : {'tconst':np.dtype('S'),
                 'parentTconst':np.dtype('S'),
                 'seasonNumber':np.dtype('S'),  
                 'episodeNumber':np.dtype('S')  
                },
      'split' :None,
      'filePath':LESSON_DATA_FOLDER + 'title.episode.tsv',
      'to_replace':{
          'seasonNumber':{'\\N':None},
          'episodeNumber':{'\\N':None},                   
      },
      'true_values':None,
      'false_values':None,      
      'usecols': None,
      'converters' : {                 
                 'seasonNumber':[parse_int],
                 'episodeNumber': [parse_int]
                }
    },
'title.principals.tsv': {
      'index_col': None, 
      'dtype' : {'tconst':np.dtype('S'),
                 'ordering':np.dtype('S'),
                 'nconst':np.dtype('S'),
                 'category':np.dtype('S'),
                 'job':np.dtype('S'),                 
                 'characters':np.dtype('S'),                 
                },
      'split' : None,
      'filePath':LESSON_DATA_FOLDER + 'title.principals.tsv',
      'to_replace':{
          'job':{'\\N':None},
          'characters':{'\\N':None},                   
      },
      'true_values':None,
      'false_values':None,
      'converters' : {                 
                 'ordering':[parse_int]                 
                },
      'usecols': None      
    },    
'title.ratings.tsv': {
      'index_col': None, 
      'dtype' : {'tconst':np.dtype('S'),'averageRating':np.float64 ,'numVotes':np.int32  },
      'split' :None,
      'filePath':LESSON_DATA_FOLDER + 'title.ratings.tsv',
      'to_replace':None,
      'true_values':None,
      'false_values':None,      
      'usecols': None,
      'converters' : {                 
                 'numVotes':[parse_int]                 
                }    
    },  
'name.basics.tsv': {
      'index_col': None, 
      'dtype' : {'nconst':np.dtype('S'),
                 'primaryName':np.dtype('S') ,
                 'birthYear':np.dtype('S')  ,
                 'deathYear':np.dtype('S'),
                 'primaryProfession':np.dtype('S'),
                 'knownForTitles':np.dtype('S')
                },      
      'filePath':LESSON_DATA_FOLDER + 'name.basics.tsv',
      'to_replace':{
          'primaryProfession':{'\\N':None},
          'knownForTitles':{'\\N':None},    
          'birthYear':{'\\N':None},    
          'deathYear':{'\\N':None},       
      },
      'true_values':None,
      'false_values':None,
      'usecols': None,    
      'converters' : {                 
             'primaryName':[lower],                 
             'birthYear':[parse_int],                 
             #'deathYear':[parse_int],
             'primaryProfession' :[split],
             'knownForTitles':[split],          
            } 
    },    
}

def getMapping(file):
    return fileColumnMapping.get(file)

def callFunction(columnData, **funDict):
    
    converFuns = funDict[columnData.name]
    
    if(converFuns != None):
        for fun in converFuns:
            columnData = columnData.apply(fun)
            
    return columnData

def readFile(file, nrows=None ):   
    
    mapping = getMapping(file)
    dtype = mapping['dtype']
    usecols = list(dtype.keys())

    df = pd.read_table(mapping['filePath'], 
                       index_col=mapping['index_col'], 
                       dtype = dtype, 
                       #na_values = ['//N'],
                       true_values= mapping['true_values'],
                       false_values= mapping['false_values'],                       
                       usecols=usecols,
                       nrows =nrows 
                      )
    df.fillna(method='pad', inplace=True)
    if(mapping['to_replace']!= None):
        df.replace(to_replace=mapping['to_replace'],method='pad', inplace=True)
    
    converters = mapping['converters']
    if(converters!= None):
        cols = list(converters.keys())
        df[cols] = df[cols].apply(callFunction, **converters)
        
    
    return df
    
crew = readFile('title.crew.tsv', 10)
print (crew)
