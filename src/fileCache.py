import config
import os
import requests

class FileCache:
    def __init__(self):
        self.cacheFolder = f"/mnt/data/{config.SOLR_PREFIX}"
        if not os.path.exists(self.cacheFolder):
            os.makedirs(self.cacheFolder)
        
    def download(self, link, docID):
        try:
            r = requests.get(link, allow_redirects=True)
            r.raise_for_status()
            filePath = f"{self.cacheFolder}/{docID}.pdf"
            open(filePath, 'wb').write(r.content)
            return filePath
        except:
            return None

    def remove(self, docID):
        filePath = f"{self.cacheFolder}/{docID}.pdf"
        if os.path.exists(filePath):
            os.remove(filePath)