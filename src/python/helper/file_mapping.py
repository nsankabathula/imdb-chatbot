from helper.parsers import Utils
import numpy as np

INTERNAL_FILE_MAPPINGS = {
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
          'filePath':None,
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
         'usecols':['tconst','titleType','primaryTitle','originalTitle','isAdult',
                    'startYear','endYear','runtimeMinutes','genres'],
         'converters' : {
                     #'primaryTitle':[{'function':Utils.lower, 'args':None}],
                     'titleType':[{'function':Utils.lower, 'args':None}],
                     #'originalTitle':[{'function':Utils.lower, 'args':None}],
                     'isAdult':[{'function':Utils.parse_bool, 'args':None}],
                     'startYear':[{'function':Utils.parse_int, 'args':None}] ,
                     'endYear':[{'function':Utils.parse_int, 'args':None}]  ,
                     'runtimeMinutes':[{'function':Utils.parse_int, 'args':None}],
                     #'genres': [{'function':Utils.split, 'args':(',',)}]

                    }
        },
    'title.crew.tsv': {
          'index_col': None, 
          'dtype' : {'tconst':np.dtype('S'),'directors':np.dtype('S') ,'writers':np.dtype('S')  },
          'filePath':None,
          'to_replace':{
              'directors':{'\\N':None},
              'writers':{'\\N':None},          
          },
         'true_values':None,
         'false_values':None,     
         'usecols': None,
         'converters' : {                 
                     #'writers':[{'function':Utils.split, 'args':(',',)}],
                     #'directors': [{'function':Utils.split, 'args':(',',)}]
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
          'filePath':None,
          'to_replace':{
              'seasonNumber':{'\\N':None},
              'episodeNumber':{'\\N':None},                   
          },
          'true_values':None,
          'false_values':None,      
          'usecols': None,
          'converters' : {                 
                     'seasonNumber':[{'function':Utils.parse_int, 'args':None}],
                     'episodeNumber': [{'function':Utils.parse_int, 'args':None}]
                    }
        },
    'title.principals.tsv': {
          'index_col': None, 
          'dtype' : {'tconst':str,
                     'ordering':int,
                     'nconst':str,
                     'category':str,
                     'job':str,                 
                     'characters':str,                 
                    },
          'split' : None,
          'filePath':None,
          'to_replace':{
              'job':{'\\N':None},
              'characters':{'\\N':None},                   
          },
          'true_values':None,
          'false_values':None,
          'converters' : {                 
                     'ordering':[{'function':Utils.parse_int, 'args':(0,)}]             
                    },
          'usecols': None      
        },    
    'title.ratings.tsv': {
          'index_col': None, 
          'dtype' : {'tconst':np.dtype('S'),'averageRating':np.float64 ,'numVotes':np.int32  },
          'split' :None,
          'filePath':None,
          'to_replace':None,
          'true_values':None,
          'false_values':None,      
          'usecols': None,
          'converters' : {                 
                     'numVotes':[{'function':Utils.parse_int, 'args':(0,)}]               
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
          'filePath':None,
          'to_replace':{
              'primaryProfession':{'\\N':None},
              'knownForTitles':{'\\N':None},    
              'birthYear':{'\\N':0},    
              'deathYear':{'\\N':0},       
          },
          'true_values':None,
          'false_values':None,
          'usecols': None,    
          'converters' : {                 
                 #'primaryName':[{'function':Utils.lower, 'args':None}],                 
                 'birthYear':[{'function':Utils.parse_int, 'args':(0,)}],                 
                 'deathYear':[{'function':Utils.parse_int, 'args':(0,)}],
                 #'primaryProfession' :[{'function':Utils.split, 'args':(',',)}],
                 #'knownForTitles':[{'function':Utils.split, 'args':(',',)}],          
                },      
        },        
        'merged.title.ratings.tsv': {
          'index_col': None, 
          'dtype' : {
                'numVotes': np.dtype('f'),
                'titleType': np.dtype('S'),
                'isAdult': np.dtype('?'),
                'primaryTitle': np.dtype('S'),
                'genres': np.dtype('S'),
                'endYear': np.dtype('S'),
                'startYear': np.dtype('S'),
                'originalTitle': np.dtype('S'),
                'runtimeMinutes': np.dtype('f'),
                'tconst': np.dtype('S'),
                'wikiTitle': np.dtype('S'),
                'averageRating': np.dtype('f')
                },      
          'filePath':None,
          'to_replace': None,
          'to_replace#':{        
              'startYear':{'\\N':0, 'NA':0},    
              'endYear':{'\\N':0, 'NA':0},       
          },
          'true_values':None,
          'false_values':None,
          'usecols': None,    
          'converters' : {                                
                 'startYear':[{'function':Utils.parse_int, 'args':(0,)}],                 
                 'endYear':[{'function':Utils.parse_int, 'args':(0,)}],                
                },      
        }, 
        'merged.names.principals.tsv': {
          'index_col': None, 
          'dtype' :{
                'ordering': np.dtype('i'),
                'job': np.dtype('S'),
                'characters': np.dtype('S'),
                'primaryName': np.dtype('S'),
                'deathYear': np.dtype('S'),
                'primaryProfession': np.dtype('S'),
                'wiki': np.dtype('S'),
                'isAlive': np.dtype('?'),
                'birthYear': np.dtype('S'),
                'category': np.dtype('S'),
                'tconst': np.dtype('S'),
                'knownForTitles': np.dtype('S'),
                'nconst': np.dtype('S')
          }
            ,      
          'filePath':None,
          'to_replace': None,
          'to_replace#':{        
              'deathYear':{'\\N':0, 'NA':0},    
              'birthYear':{'\\N':0, 'NA':0},       
          },
          'true_values':None,
          'false_values':None,
          'usecols': None,    
          'converters' : {                                
                 'birthYear':[{'function':Utils.parse_int, 'args':(0,)}],                 
                 'deathYear':[{'function':Utils.parse_int, 'args':(0,)}],                
                },      
        }, 
    }

class FILE_MAPPINGS:    
    
    @staticmethod
    def mapping():
        return INTERNAL_FILE_MAPPINGS
    
    @staticmethod
    def getMapping(file):
        return INTERNAL_FILE_MAPPINGS.get(file)
    
    
    
