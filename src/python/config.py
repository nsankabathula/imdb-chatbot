from helper.file_mapping import FILE_MAPPINGS as __FILE_MAPPINGS

IMDB_DATA_FOLDER = '../data/';

FILE_MAPPINGS = __FILE_MAPPINGS;


ES_CONFIG = {
    "host": [
        {
            "host": "localhost",                  
            "port":"9229"
        }
    ],
    "log": "error"
};

IMDB_DATASETS = ['https://datasets.imdbws.com/name.basics.tsv.gz', 
        'https://datasets.imdbws.com/title.akas.tsv.gz',
        'https://datasets.imdbws.com/title.basics.tsv.gz',
        'https://datasets.imdbws.com/title.crew.tsv.gz',
        'https://datasets.imdbws.com/title.episode.tsv.gz',
        'https://datasets.imdbws.com/title.principals.tsv.gz',
        'https://datasets.imdbws.com/title.ratings.tsv.gz'
        ];

IMDB_TABLE_DICT = { 
                'merged_name_principals':{'indexs':[{'indexName':'idx_mnp_tconst', 'columns':'tconst', 'isUnique':False}]}, 
                'merged_title_ratings':{'indexs':[{'indexName':'uidx_mtr_tconst', 'columns':'tconst', 'isUnique':True}]}, 
                'title_episodes':{'indexs':[{'indexName':'idx_te_ptconst', 'columns':'parentTconst', 'isUnique':False}]},        
              };

SQLLITE_CONFIG = {
    'FILE_PATH' : ''.join ([IMDB_DATA_FOLDER, 'imdb.sqllite']),
    'QUERY': {
        'create_index' : 'CREATE INDEX {indexName} on {tableName}({columns})',
        'create_unqiue_index' : 'CREATE UNIQUE INDEX {indexName} ON {tableName}({columns})',
        'drop_index' : 'DROP INDEX IF EXISTS {indexName}',
        'table_count': "SELECT COUNT(1) count, '{tableName}' tableName FROM {tableName}"
    }
};
