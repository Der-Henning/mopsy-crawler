import os
import sys
from multiprocessing import Process, Value, Manager
from ctypes import c_wchar_p, c_bool, c_double
from threading import Thread
import hashlib
import config
import time
import datetime
from solr import Solr
from bs4 import BeautifulSoup
from fileCache import FileCache
from buildSchema import langs
import logging as log
from langdetect import detect

fieldList = "id,md5,source,rating,publishers,document,authors,publicationDate,language,title_txt_*,subtitle_txt_*,tags_txt_*,summary_txt_*,creationDate,modificationDate"


class Crawler:
    def __init__(self):
        log.debug("Initializing Crawler ...")
        manager = Manager()
        self.prozStatus = manager.Value(c_wchar_p, "untätig")
        self.prozProgress = manager.Value(c_double, 0.0)
        self.prozText = manager.Value(c_wchar_p, "")
        self.prozStop = manager.Value(c_bool, False)
        self.prozStartable = manager.Value(c_bool, True)
        self.prozAutorestart = manager.Value(c_bool, config.AUTORESTART)
        self.autostart = config.AUTOSTART
        self.task = None
        self.fileCache = FileCache()
        self.indexedIDs = []

        sys.path.insert(0, "./sources")
        self.Documents = __import__(config.CRAWLER).Documents

        self.solr = Solr(config.SOLR_HOST, config.SOLR_PORT, config.SOLR_CORE)

        self.whatchThread = Thread(target=self.whatchman)
        self.whatchThread.start()

        if self.autostart:
            log.info("Autostarting Crawler ...")
            self.start()

    def whatchman(self):
        while True:
            time.sleep(10)
            if self.prozAutorestart.get() and self.prozStartable.get() and not self.prozStop.get():
                log.info("Restarting Crawler ...")
                self.start()

    def start(self):
        if self.prozStartable.get():
            self.indexedIDs = []
            self.prozProgress.set(0)
            self.prozStatus.set("wird gestartet ...")
            self.prozText.set("")
            self.prozStop.set(False)
            self.prozStartable.set(False)
            self.task = Process(target=self.worker, args=(
                self.prozStop, self.prozText, self.prozProgress, self.prozStatus, self.prozStartable))
            self.task.start()
        return self.getStatus()

    def stop(self):
        if not self.prozStartable.get():
            self.prozStop.set(True)
            self.prozStatus.set("wird gestopped ...")
        return self.getStatus()

    def toggleAutorestart(self):
        self.prozAutorestart.set(False) if self.prozAutorestart.get() else True
        return self.getStatus()

    def getStatus(self):
        return {
            "name": config.CRAWLER_NAME,
            "status": self.prozStatus.get(),
            "progress": self.prozProgress.get(),
            "text": self.prozText.get(),
            "startable": self.prozStartable.get(),
            "autorestart": self.prozAutorestart.get()
        }

    def worker(self, stopped, text, progress, status, startable):
        try:
            status.set("Calibre Datenbank einlesen ...")
            documents = self.Documents()
            status.set("Dokumente werden indiziert ...")
            for idx, doc in enumerate(documents):
                if stopped.get():
                    break
                try:
                    progress.set(round(idx / len(documents) * 100, 2))
                    if "title" in doc:
                        text.set(doc["title"])
                        log.info(doc["title"])
                    if config.DIRECT_COMMIT:
                        self.solr.commit(doc)
                    else:
                        self.indexer(doc)
                except:
                    log.error(sys.exc_info())
                finally:
                    self.indexedIDs.append(doc["id"])
                    time.sleep(config.SLEEP_TIME)

            if not stopped.get():
                progress.set(100)
                status.set("Lösche nicht mehr vorhandene Einträge ...")
                self.cleanup()

            status.set("Erzeuge Index für Suchvorschläge ...")
            text.set("")
            self.solr.buildDict()

            status.set("Optimiere SOLR Index ...")
            self.solr.optimize()

            if stopped.get():
                status.set("abgebrochen")
            else:
                status.set("fertig")
        except:
            status.set("fehler: {}".format(sys.exc_info()))
            log.error(sys.exc_info())
        finally:
            startable.set(True)

    def cleanup(self):
        numFound = 100
        offset = 0
        rows = 100
        while offset < numFound:
            try:
                res = self.solr.select({
                    "q": f'id:"{config.SOLR_PREFIX}_*"',
                    "fl": fieldList,
                    "fq": "deleted:false",
                    "rows": rows,
                    "start": offset
                })
                solrDocs = res["response"]["docs"]
                for solrDoc in solrDocs:
                    if not solrDoc["id"] in self.indexedIDs:
                        log.info(f"mark {solrDoc['id']} as deleted")
                        doc = solrDoc
                        doc["deleted"] = True
                        doc["md5"] = ""
                        doc["scanDate"] = datetime.datetime.now().isoformat() + "Z"
                        self.solr.commit(doc)
                        self.fileCache.remove(solrDoc["id"])
                numFound = res["response"]["numFound"]
            except:
                log.error(sys.exc_info())
            finally:
                offset += rows

    def indexer(self, doc):
        def tomd5(fname):
            hash_md5 = hashlib.md5()
            with open(fname, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()

        # get existing doc from solr
        solrDoc = self.solr.select({"q": f'id:"{doc["id"]}"', "fl": fieldList})

        # read md5 if exists
        md5 = ""
        if len(solrDoc["response"]["docs"]) > 0 and "md5" in solrDoc["response"]["docs"][0]:
            md5 = solrDoc["response"]["docs"][0]["md5"]

        # get file
        if "indexlink" in doc:
            filePath = self.fileCache.download(doc["indexlink"], doc["id"])
            doc.pop("indexlink", None)
            doc['file'] = filePath
        elif "file" in doc:
            filePath = doc["file"]
        elif "link" in doc:
            filePath = self.fileCache.download(doc["link"], doc["id"])
            doc['file'] = filePath
        # else: return

        # if no file provided delete cached file
        # if exists in solr -> mark as deleted else stop
        if doc['file'] == None or not os.path.exists(doc['file']):
            log.info("No file found")
            self.fileCache.remove(doc["id"])
            if len(solrDoc["response"]["docs"]) > 0:
                doc["deleted"] = True
                doc.update(solrDoc["response"]["docs"][0])
                doc["md5"] = ""
            else:
                return

        # read file and compare md5
        # if md5 matches saved one -> stop
        # else extract data from file
        else:
            doc["md5"] = tomd5(filePath)
            log.info(doc["md5"])
            if md5 != doc["md5"]:
                log.info("new Document")
                extract = self.extractData(filePath)
                if not extract:
                    log.info("Error extracting data from file")
                    return
                if not "title" in doc:
                    doc["title"] = extract["title"]
                if "language" in extract and not "language" in doc:
                    doc['language'] = extract['language']
                if "creationDate" in extract:
                    doc["creationDate"] = extract["creationDate"]
                if "modificationDate" in extract:
                    doc["modificationDate"] = extract["modificationDate"]
                doc["pages"] = extract['pages']
                doc["deleted"] = False
            else:
                log.info("no changes")
                return

        # set document language for fields
        if not "language" in doc:
            doc["language"] = "other"
        if not doc['language'] in langs:
            doc['language'] = "other"
        if doc['language'] == "other" and "pages" in doc:
            doc['language'] = self.detectLang(doc["pages"])
        if "title" in doc:
            doc[f"title_txt_{doc['language']}"] = doc["title"]
            doc.pop("title", None)
        if "tags" in doc:
            doc[f"tags_txt_{doc['language']}"] = doc["tags"]
            doc.pop("tags", None)
        if "subtitle" in doc:
            doc[f"subtitle_txt_{doc['language']}"] = doc["subtitle"]
            doc.pop("subtitle", None)
        if "summary" in doc:
            doc[f"summary_txt_{doc['language']}"] = doc["summary"]
            doc.pop("summary", None)
        if "pages" in doc:
            doc.update({f"p_{num}_page_txt_{doc['language']}": page for num, page in enumerate(
                doc['pages'], start=1)})
            doc.pop("pages", None)
        doc["scanDate"] = datetime.datetime.now().isoformat() + "Z"

        # commit document to solr
        self.solr.commit(doc)

    def extractData(self, filePath):
        def getPages(html):
            soup = BeautifulSoup(html, 'html.parser')
            pages = [s.get_text() for s in soup.find_all("div", "page")]
            return pages
        data = {}
        try:
            extract = self.solr.extract(filePath)
            filename = os.path.basename(filePath)
            content = extract["file"] if "file" in extract else extract[filename]
            meta = extract["file_metadata"] if "file_metadata" in extract else extract[
                f"{filename}_metadata"]
            meta = dict(zip(meta[::2], meta[1::2]))
            data['title'] = meta['dc:title'][0] if 'dc:title' in meta and meta['dc:title'][0] != '' else filename
            data['pages'] = getPages(content)
            if "created" in meta:
                data['creationDate'] = meta['created'][0]
            if "Last-Modified" in meta:
                data['modificationDate'] = meta['Last-Modified'][0]
            if "language" in meta:
                data['language'] = meta['language'][0].lower()
        except:
            log.error(sys.exc_info())
        return data

    def detectLang(self, pages):
        langs = [detect(page) for page in pages]
        counter = {}
        for lang in langs:
            if lang in counter:
                counter[lang] += 1
            else:
                counter[lang] = 1
        sorted_counter = sorted(langs, key=counter.get, reverse=True)
        return sorted_counter[0]
