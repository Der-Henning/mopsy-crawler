import os
import sys
import sqlite3
from multiprocessing import Process, Value, Manager
from ctypes import c_wchar_p, c_bool, c_double
import hashlib
from solr import Solr
from bs4 import BeautifulSoup

calibre_path = "/books"

manager = Manager()
prozStatus = manager.Value(c_wchar_p, "")
prozProgress = manager.Value(c_double, 0.0)
prozText = manager.Value(c_wchar_p, "")
prozStop = manager.Value(c_bool, False)
prozStartable = manager.Value(c_bool, True)

CRAWLER_NAME = os.getenv("CRAWLER_NAME", "Calibre")
PREFIX = os.getenv("MOPSY_SOLR_PREFIX", "calibre")
SOLR_HOST = os.getenv("MOPSY_SOLR_HOST", "solr")
SOLR_PORT = os.getenv("MOPSY_SOLR_PORT", 8983)
SOLR_CORE = os.getenv("MOPSY_SOLR_CORE", "mopsy")

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
    return {"status": prozStatus.value, "progress": prozProgress.value, "text": prozText.value}

def worker(stopped, text, progress, status, startable):
    status.value = "läuft"
    try:
        status.value = "Calibre Datenbank einlesen ..."
        documents = getDocuments()

        status.value = "Nicht mehr vorhandene Documente löschen ..."
        cleanup(documents)

        status.value = "Documente werden indiziert ..."
        counter = 0
        for doc in documents:
            if stopped.value:
                break
            try:
                progress.value = round(counter / len(documents) * 100, 2)
                text.value = doc["title"]
                indexer(doc)
            finally:
                counter += 1
        
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

def getDocuments():
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def create_connection(db_file):
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except Error as e:
            print(e)
        return conn

    conn = create_connection(os.path.join(calibre_path, "metadata.db"))
    conn.row_factory = dict_factory
    with conn:
        cur = conn.cursor()
        cur.execute("SELECT id, title, pubdate, path, isbn FROM books")
        documents = cur.fetchall()
        for doc in documents:
            doc["authors"] = [a['name'] for a in cur.execute("SELECT name FROM books_authors_link LEFT JOIN authors ON author=authors.id WHERE book={}".format(doc["id"])).fetchall()]
            doc["publishers"] = [p['name'] for p in cur.execute("SELECT name FROM books_publishers_link LEFT JOIN publishers ON publisher=publishers.id WHERE book={}".format(doc["id"])).fetchall()]
            data = cur.execute("SELECT format, name FROM data WHERE book={}".format(doc["id"])).fetchall()
            doc["formats"] = [d["format"] for d in data]
            if len(data) > 0 : doc["file"] = data[0]["name"]
            doc["tags"] = [t["name"] for t in cur.execute("SELECT name FROM books_tags_link LEFT JOIN tags ON tag=tags.id WHERE book={}".format(doc["id"])).fetchall()]
            ratings = [r["rating"] for r in cur.execute("SELECT ratings.rating FROM books_ratings_link LEFT JOIN ratings ON books_ratings_link.rating=ratings.id WHERE book={}".format(doc["id"])).fetchall()]
            if len(ratings) > 0 : doc["rating"] = ratings[0]
            # doc["identifiers"] = cur.execute("SELECT type, val FROM identifiers WHERE book={}".format(doc["id"])).fetchall()
            doc["path"] = os.path.join(calibre_path, doc["path"])
            doc["id"] = f"{PREFIX}_{str(doc['id'])}"
        return documents

def cleanup(documents):
    numFound = 100
    offset = 0
    rows = 100
    while offset < numFound:
        res = solr.select({"fl": "id", "rows": rows, "start": offset})
        solrDocs = res["response"]["docs"]
        for solrDoc in solrDocs:
            found = False
            for doc in documents:
                if doc["id"] == document["id"]:
                    found = True
                    break
        numFound = res["response"]["numFound"]
        offset += rows

def indexer(doc):
    print(doc["title"])

    def tomd5(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def getPages(html):
        soup = BeautifulSoup(html, 'html.parser')
        pages = [s.get_text() for s in soup.find_all("div", "page")]
        return pages

    solrDoc = solr.select({"q":f"id:{doc['id']}", "fl": "md5"})

    md5 = ""
    if len(solrDoc["response"]["docs"]) > 0:
        md5 = solrDoc["response"]["docs"][0]["md5"]
    fileExtension = doc["formats"][0].lower()
    doc["filename"] = os.path.join(doc["path"], doc["file"] + "." + fileExtension)

    doc["md5"] = tomd5(doc["filename"])
    if md5 != doc["md5"]:
        print("new Document")

        extract = solr.extract(doc["filename"])
        meta = extract[f"{doc['file']}.{fileExtension}_metadata"]
        meta = dict(zip(meta[::2], meta[1::2]))

        doc['language'] = meta['language'][0].lower() if "language" in meta else "de"
        doc['language'] = doc['language'] if doc['language'] == "de" or doc['language'] == "en" else "other"
        doc.update({f"{num}_page_txt_{doc['language']}": page for num, page in enumerate(getPages(extract[f"{doc['file']}.{fileExtension}"]), start=1)})
        doc[f"title_txt_{doc['language']}"] = doc["title"]
        doc[f"tags_txt_{doc['language']}"] = doc["tags"]
        doc.pop("tags", None)
        doc.pop("title", None)

        solr.commit(doc)

    else:
        print("no changes")
