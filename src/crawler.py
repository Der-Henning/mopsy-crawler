import os
import sys
from multiprocessing import Process, Value, Manager
from ctypes import c_wchar_p, c_bool, c_double
import hashlib
import config
import time
from solr import Solr
from bs4 import BeautifulSoup
from fileCache import FileCache

class Crawler:
    def __init__(self):
        manager = Manager()
        self.prozStatus = manager.Value(c_wchar_p, "untätig")
        self.prozProgress = manager.Value(c_double, 0.0)
        self.prozText = manager.Value(c_wchar_p, "")
        self.prozStop = manager.Value(c_bool, False)
        self.prozStartable = manager.Value(c_bool, True)
        self.prozAutorestart = manager.Value(c_bool, config.AUTORESTART)
        self.task = None
        self.fileCache = FileCache()
        self.indexedIDs = []

        sys.path.insert(0, "./sources")
        self.Documents = __import__(config.CRAWLER).Documents

        self.solr = Solr(config.SOLR_HOST, config.SOLR_PORT, config.SOLR_CORE)

    def start(self):
        if self.prozStartable.value:
            self.indexedIDs = []
            self.prozProgress.value = 0
            self.prozStatus.value = "wird gestartet ..."
            self.prozText.value = ""
            self.prozStop.value = False
            self.prozStartable.value = False
            self.task = Process(target=self.worker, args=(self.prozStop, self.prozText, self.prozProgress, self.prozStatus, self.prozStartable, self.prozAutorestart))
            self.task.start()
        return self.getStatus()

    def stop(self):
        if not self.prozStartable.value:
            self.prozStop.value = True
            self.prozStatus.value = "wird gestopped ..."
        return self.getStatus()

    def toggleAutorestart(self):
        self.prozAutorestart.value = False if self.prozAutorestart.value else True
        return self.getStatus()

    def getStatus(self):
        return {
            "name": config.CRAWLER_NAME,
            "status": self.prozStatus.value,
            "progress": self.prozProgress.value,
            "text": self.prozText.value,
            "startable": self.prozStartable.value,
            "autorestart": self.prozAutorestart.value
        }

    def worker(self, stopped, text, progress, status, startable, autorestart):
        try:
            # print(config.PRE_CLEANUP)
            # if config.PRE_CLEANUP:
            #     print("starting cleaning")
            #     status.value = "Nicht mehr vorhandene Documente löschen ..."
            #     self.cleanup(self.Documents())

            status.value = "Calibre Datenbank einlesen ..."
            documents = self.Documents()
            status.value = "Dokumente werden indiziert ..."
            for idx, doc in enumerate(documents):
                if stopped.value:
                    break
                try:
                    progress.value = round(idx / len(documents) * 100, 2)
                    if "title" in doc:
                        text.value = doc["title"]
                        print(doc["title"])
                    if config.DIRECT_COMMIT:
                        self.solr.commit(doc)
                    else:
                        self.indexer(doc)
                    self.indexedIDs.append(doc["id"])
                except:
                    print(sys.exc_info())
                finally:
                    time.sleep(config.SLEEP_TIME)
            
            if not stopped.value:
                progress.value = 100
                status.value = "Lösche nicht mehr vorhandene Einträge ..."
                self.cleanup()

            status.value = "Erzeuge Index für Suchvorschläge ..."
            text.value = ""
            self.solr.buildDict()

            status.value = "Optimiere SOLR Index ..."
            self.solr.optimize()

            if stopped.value:
                status.value = "abgebrochen"
            else:
                status.value = "fertig"
        except:
            status.value = "fehler: {}".format(sys.exc_info())
            print(sys.exc_info())
        finally:
            startable.value = True
            if autorestart.value and not stopped.value:
                self.start()

    def cleanup(self):
        numFound = 100
        offset = 0
        rows = 100
        while offset < numFound:
            try:
                res = self.solr.select({"q": "*:*", "fl": "id", "rows": rows, "start": offset})
                solrDocs = res["response"]["docs"]
                for solrDoc in solrDocs:
                    if not solrDoc["id"] in self.indexedIDs:
                        print(f"Deleting {solrDoc['id']} from index")
                        self.solr.remove(solrDoc["id"])
                        self.fileCache.remove(solrDoc["id"])
                numFound = res["response"]["numFound"]
            except:
                print(sys.exc_info())
            finally:
                offset += rows

    def indexer(self, doc):
        def tomd5(fname):
            hash_md5 = hashlib.md5()
            with open(fname, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()

        solrDoc = self.solr.select({"q":f"id:{doc['id']}", "fl": "md5"})

        md5 = ""
        if len(solrDoc["response"]["docs"]) > 0:
            md5 = solrDoc["response"]["docs"][0]["md5"]

        if "indexlink" in doc:
            filePath = self.fileCache.download(doc["indexlink"], doc["id"])
            doc.pop("indexlink", None)
            doc['file'] = filePath
        elif "file" in doc:
            filePath = doc["file"]
        elif "link" in doc:
            filePath = self.fileCache.download(doc["link"], doc["id"])
            doc['file'] = filePath
        else: return

        if doc['file'] == None or not os.path.exists(doc['file']):
            print("No file found")
            print(f"Deleting {doc['id']} from index")
            self.solr.remove(doc["id"])
            self.fileCache.remove(doc["id"])
            return

        doc["md5"] = tomd5(filePath)
        print(doc["md5"])
        if md5 != doc["md5"]:
            print("new Document")
            extract = self.extractData(filePath, doc)
            doc.update(extract['pages'])
            if not "language" in doc: doc['language'] = extract['language']
            if not "title" in doc: doc["title"] = extract["title"]
            if "title" in doc: doc[f"title_txt_{doc['language']}"] = doc["title"]
            if "tags" in doc: doc[f"tags_txt_{doc['language']}"] = doc["tags"]
            if "creationDate" in extract: doc["creationDate"] = extract["creationDate"]
            if "modificationDate" in extract: doc["modificationDate"] = extract["modificationDate"]
            doc.pop("tags", None)
            doc.pop("title", None)
            self.solr.commit(doc)
        else:
            print("no changes")

    def extractData(self, filePath, doc):
        def getPages(html):
            soup = BeautifulSoup(html, 'html.parser')
            pages = [s.get_text() for s in soup.find_all("div", "page")]
            return pages
        data = {}
        extract = self.solr.extract(filePath)
        filename = os.path.basename(filePath)
        content = extract["file"] if "file" in extract else extract[filename]
        meta = extract["file_metadata"] if "file_metadata" in extract else extract[f"{filename}_metadata"]
        meta = dict(zip(meta[::2], meta[1::2]))
        data['title'] = meta['dc:title'][0] if 'dc:title' in meta else filename
        if "language" in doc:
            data['language'] = doc['language']
        else:
            data['language'] = meta['language'][0].lower() if "language" in meta else "de"
            data['language'] = data['language'] if data['language'] == "de" or data['language'] == "en" else "other"
        data['pages'] = {f"p_{num}_page_txt_{data['language']}": page for num, page in enumerate(getPages(content), start=1)}
        if "created" in meta: data['creationDate'] = meta['created'][0] 
        if "Last-Modified" in meta: data['modificationDate'] = meta['Last-Modified'][0]
        return data