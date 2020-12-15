import os
import sys
from multiprocessing import Process, Value, Manager
from ctypes import c_wchar_p, c_bool, c_double
import hashlib
from solr import Solr
from bs4 import BeautifulSoup

manager = Manager()
prozStatus = manager.Value(c_wchar_p, "")
prozProgress = manager.Value(c_double, 0.0)
prozText = manager.Value(c_wchar_p, "")
prozStop = manager.Value(c_bool, False)
prozStartable = manager.Value(c_bool, True)

CRAWLER_NAME = os.getenv("CRAWLER_NAME", "Calibre")
CRAWLER = os.getenv("CRAWLER_TYPE", "calibre")
PREFIX = os.getenv("MOPSY_SOLR_PREFIX", "calibre")
SOLR_HOST = os.getenv("MOPSY_SOLR_HOST", "solr")
SOLR_PORT = os.getenv("MOPSY_SOLR_PORT", 8983)
SOLR_CORE = os.getenv("MOPSY_SOLR_CORE", "mopsy")

sys.path.insert(0, f"./sources")
Documents = __import__(CRAWLER).Documents

solr = Solr(SOLR_HOST, SOLR_PORT, SOLR_CORE)

def start():
    if prozStartable.value:
        prozProgress.value = 0
        prozStatus.value = "wird gestartet ..."
        prozText.value = ""
        prozStop.value = False
        prozStartable.value = False
        task = Process(target=worker, args=(prozStop, prozText, prozProgress, prozStatus, prozStartable))
        task.start()
    return getStatus()

def stop():
    if not prozStartable.value:
        prozStop.value = True
        prozStatus.value = "wird gestopped ..."
    return getStatus()

def getStatus():
    return {"name": CRAWLER_NAME, "status": prozStatus.value, "progress": prozProgress.value, "text": prozText.value, "startable": prozStartable.value}

def worker(stopped, text, progress, status, startable):
    try:
        status.value = "Calibre Datenbank einlesen ..."
        documents = Documents(PREFIX)

        status.value = "Nicht mehr vorhandene Documente löschen ..."
        cleanup(documents)

        status.value = "Documente werden indiziert ..."
        for idx, doc in enumerate(documents):
            if stopped.value:
                break
            try:
                progress.value = round(idx / len(documents) * 100, 2)
                text.value = doc["title"]
                indexer(doc)
            except:
                print(sys.exc_info())
        
        status.value = "Erzeuge Index für Suchvorschläge ..."
        text.value = ""
        solr.buildDict()

        if stopped.value:
            status.value = "abgebrochen"
        else:
            status.value = "fertig"
    except:
        status.value = "fehler: {}".format(sys.exc_info())
        print(sys.exc_info())
    finally:
        startable.value = True

def cleanup(documents):
    numFound = 100
    offset = 0
    rows = 100
    while offset < numFound:
        try:
            res = solr.select({"fl": "id", "rows": rows, "start": offset})
            solrDocs = res["response"]["docs"]
            for solrDoc in solrDocs:
                found = False
                for doc in documents:
                    if doc["id"] == document["id"]:
                        found = True
                        break
            numFound = res["response"]["numFound"]
        except:
            print(sys.exc_info())
        finally:
            offset += rows

def indexer(doc):
    print(doc["title"])

    def tomd5(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    solrDoc = solr.select({"q":f"id:{doc['id']}", "fl": "md5"})

    md5 = ""
    if len(solrDoc["response"]["docs"]) > 0:
        md5 = solrDoc["response"]["docs"][0]["md5"]

    if "file" in doc:
        filePath = doc["file"]
    elif "link" in doc:
        # download
        pass
    else: return

    doc["md5"] = tomd5(filePath)
    print(doc["md5"])
    if md5 != doc["md5"]:
        print("new Document")
        doc.update(extractData(filePath))
        doc[f"title_txt_{doc['language']}"] = doc["title"]
        doc[f"tags_txt_{doc['language']}"] = doc["tags"]
        doc.pop("tags", None)
        doc.pop("title", None)
        solr.commit(doc)
    else:
        print("no changes")

def extractData(filePath):
    def getPages(html):
        soup = BeautifulSoup(html, 'html.parser')
        pages = [s.get_text() for s in soup.find_all("div", "page")]
        return pages
    data = {}
    extract = solr.extract(filePath)
    filename = os.path.basename(filePath)
    meta = extract[f"{filename}_metadata"]
    meta = dict(zip(meta[::2], meta[1::2]))
    data['language'] = meta['language'][0].lower() if "language" in meta else "de"
    data['language'] = data['language'] if data['language'] == "de" or data['language'] == "en" else "other"
    data.update({f"p_{num}_page_txt_{data['language']}": page for num, page in enumerate(getPages(extract[filename]), start=1)})
    return data