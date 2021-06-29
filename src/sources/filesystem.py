import os
import config
import logging as log
from pathlib import Path
import hashlib

def md5(string):
    return hashlib.md5(string.encode('utf-8')).hexdigest()

class Documents:
    def __init__(self):
        self._index = 0
        self._prefix = config.SOLR_PREFIX
        self._rootFolder = os.getenv("ROOT_FOLDER", "/")
        self._documents = list(Path(self._rootFolder).glob('**/*.pdf'))

    # define individual facets for this crawler
    fieldList = {
            "facets": {}
        }

    def __iter__(self):
        return self

    def __len__(self):
        return len(self._documents)

    def __next__(self):
        if self._index < len(self._documents):
            file = self._documents[self._index]
            doc = {
                "id": f"{self._prefix}_{md5(str(file))}",
                "file": str(file),
            }
            self._index += 1
            return doc
        raise StopIteration