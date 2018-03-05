import datetime 
import numpy

class Utils:
    
    @staticmethod
    def index_array(length):
        result = [x for x in range(length)]   
        return result
    
    @staticmethod
    def parse_date(date):
        if str(date) == '' or date == None:
            return None
        else:
            return datetime.strptime(date,'%Y-%m-%d')    
        
    @staticmethod
    def parse_int(i, defaultValue=None):    
       # print (defaultValue)
        if ( i == None or str(i) == '' or str(i) == 'NaN' or i == numpy.NaN) :
            return defaultValue   
        else:
            try:
                #print ('convert', int(i))
                return int(float(i))
            except:
                return i
            
    @staticmethod
    def parse_float(f):
        if str(f) == '' or f == None:
            return None
        else:
            try:
                return float(f)
            except:
                return f
            
    @staticmethod
    def parse_bool(boolean):
        if str(boolean) == '' or boolean == None:
            return None
        else:        
            return boolean =='True' 

    @staticmethod
    def split(data, delimiter=',' ):
        #print ('data: ', data)    
        if(data == '' or data == None or str(data) == None):
            return np.array([None])
        else:
            #return np.array(str(data).lower().split(delimiter))    
            lst = np.array(str(data).split(delimiter))       
            return lst    

    @staticmethod
    def lower(data):
        if(data == '' or data == None):
            return None
        else:
            return str(data).lower() 
    
    @staticmethod
    def upper(data):
        if(data == '' or data == None):
            return None
        else:
            return str(data).upper()     
    
    @staticmethod
    def replaceNaN(data):
        if(np.isnan(data)): 
            return None
        else:
            return data
        
    @staticmethod
    def wikiLink(name):
        link = ''.join (['https://en.wikipedia.org/wiki/', ''.join ([n + '_' for n in name.split()])]).rstrip('_')    
        return link

    @staticmethod
    def explode(df, lst_cols, fill_value=''):
        # make sure `lst_cols` is a list
        if lst_cols and not isinstance(lst_cols, list):
            lst_cols = [lst_cols]
        # all columns except `lst_cols`
        idx_cols = df.columns.difference(lst_cols)

        # calculate lengths of lists
        lens = df[lst_cols[0]].str.len()

        if (lens > 0).all():
            # ALL lists in cells aren't empty
            return pd.DataFrame({
                col:np.repeat(df[col].values, df[lst_cols[0]].str.len())
                for col in idx_cols
            }).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
              .loc[:, df.columns]
        else:
            # at least one list in cells is empty
            return pd.DataFrame({
                col:np.repeat(df[col].values, df[lst_cols[0]].str.len())
                for col in idx_cols
            }).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
              .append(df.loc[lens==0, idx_cols]).fillna(fill_value) \
              .loc[:, df.columns]
          
    # Python does not have switch statment, rather use dict approach
    parser = {
            'int':parse_int,
            'date':parse_date,
            'bool':parse_bool
        }