import pandas as pd
import numpy as numpy
#import matplotlib.pyplot as plt
#import seaborn as sns
#import scipy.stats as st
#import sys 
#import datetime as dt
import time as time
import gc as gc

from helper.parallel_util import ParallelUtil
from helper.parsers import Utils
from helper.file_mapping import FILE_MAPPINGS
from helper.imdb_sqllite_db import IMDBSQLLite
import config


LESSON_DATA_FOLDER = config.IMDB_DATA_FOLDER

imdb_db = IMDBSQLLite();
imdbConn, imdbCurs = imdb_db.get()

def transform(df, key):
    mapping = FILE_MAPPINGS.getMapping(key)
    df.fillna(method='pad', inplace=True)
    if(mapping['to_replace']!= None):
        df.replace(to_replace=mapping['to_replace'],method='pad', inplace=True)
    
    converters = mapping['converters']
    if(converters!= None):
        cols = list(converters.keys())
        df[cols] = df[cols].apply(Utils.callFunction, **converters)
        
    return df

def prep_title(title):
    limit = title['limit']
    offset = title['offset']
    file = title['file']
    q_titles = "select  * from merged_title_ratings LIMIT {limit} OFFSET {offset}".format(limit = limit, offset = offset)    
    df_titles = imdb_db.query(q_titles)
    df_titles = transform(df_titles, 'merged.title.ratings.sql')
    df_titles['imdb'] = df_titles['tconst'].apply(Utils.wikiLink, args = ('www.imdb.com/title/', ))
    
    #names['wiki'] = names['primaryName'].apply(Utils.wikiLink)
    
    q_names = 'SELECT * FROM merged_name_principals WHERE tconst IN ({tconsts})'.format(
        tconsts =  ','.join("'"+ tconst +"'" for tconst in df_titles['tconst']))
                    
    df_names  = imdb_db.query(q_names)
    df_names['imdb'] = df_names['nconst'].apply(Utils.wikiLink, args = ('www.imdb.com/name/', ))
    df_names = transform(df_names, 'merged.names.principals.sql')
    df_names['isAlive'] = df_names['deathYear'] == 0    
    df_names['wiki'] = df_names['primaryName'].apply(Utils.wikiLink)
    df_names[['lastName', 'firstName']] = df_names['primaryName'].apply(lambda x: pd.Series(str(x).lower().split(' ', 1)))
    
    dfg_names = df_names.groupby('tconst')
    
    q_episodes = 'SELECT * FROM title_episodes WHERE parentTconst IN ({tconsts})'.format(
        tconsts =  ','.join("'"+ tconst +"'" for tconst in df_titles['tconst']))
    df_episodes  = imdb_db.query(q_episodes)
    df_episodes = transform(df_episodes, 'title.episodes.sql')
    dfg_episodes = df_episodes.groupby(['parentTconst'])
    
    
    def funNames(row, df):       
        try:
            dic = df.get_group(row['tconst']).to_dict('records')
            #dic = dfg_episodes.get_group(row['tconst']).to_dict('records')
            return dic
        except:        
            return []
    
    def funSeasons(data):
        #print(data['tconst'])
        dic = {
            'seasonNumber':str(int(data['seasonNumber'].unique()[0])), 
            'episodes': data['tconst'],                        
             }
        #print(dic)
        return dic
    def funEpisodes(row, df):       
        try:            
            if(row['parentTconst'] == None):
                dic = df.get_group(row['tconst']).dropna(axis=0, how='any').groupby('seasonNumber').apply(funSeasons)                
                return dic
            else:
                return []
        except KeyError as kerr:
            return []          
        except Exception as ex:
            raise ex;
            return []  

    df_titles['crew'] = df_titles.apply(funNames, axis = 1, args= (dfg_names, ))
    df_titles['series'] = df_titles.apply(funEpisodes, axis = 1, args= (dfg_episodes, ))
    
    df_titles.to_json(file, orient='records')
    del [df_titles,df_names, dfg_names, df_episodes, dfg_episodes]
    gc.collect()

    