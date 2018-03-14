import requests

class UrlUtils:
    
    @staticmethod
    def index_array(length):
        result = [x for x in range(length)]   
        return result
    
    @staticmethod
    def is_downloadable(url):
        """
        Does the url contain a downloadable resource
        """
        h = requests.head(url, allow_redirects=True)
        header = h.headers
        content_type = header.get('content-type')
        print (content_type)
        if 'text' in content_type.lower():
            return True 
        if 'html' in content_type.lower():
            return False
        return True
    
    @staticmethod
    def get_file_name(url):
        if url.find('/'):
            return url.rsplit('/', 1)[1]
    
    @staticmethod
    def download(url, localPath = None):       
        req = requests.get(url, allow_redirects=True)
        fileName = UrlUtils.get_file_name(url)
        
        if(fileName == None):
            fileName = UrlUtils.get_filename_from_cd(req.headers.get('content-disposition'))
            
        fileName = ''.join([localPath, fileName]);
        print ('Dowloading from {} to file {}'.format(url, fileName));
        open(fileName, 'wb').write(req.content)
        

    def get_filename_from_cd(cd):
        """
        Get filename from content-disposition
        """
        if not cd:
            return None
        fname = re.findall('filename=(.+)', cd)
        if len(fname) == 0:
            return None
        return fname[0]

        