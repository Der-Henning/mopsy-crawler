import sqlite3
import os

class Documents:
    def __init__(self, prefix):
        self._documents = []
        self._index = 0
        self._prefix = prefix
        calibrePath = os.getenv("CALIBRE_PATH", "/mnt/books")
        self._documents = self._readCalibreDB(calibrePath)

    def __iter__(self):
        return self

    def __len__(self):
        return len(self._documents)

    def __next__(self):
        if self._index < len(self._documents):
            doc = self._documents[self._index]
            self._index += 1
            return doc
        raise StopIteration

    def _readCalibreDB(self, calibrePath):
        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
        
        with sqlite3.connect(os.path.join(calibrePath, "metadata.db")) as conn:
            conn.row_factory = dict_factory
            cur = conn.cursor()
            cur.execute("SELECT id, title, pubdate, path, isbn FROM books")
            rows = cur.fetchall()
            documents = []
            for row in rows:
                doc = {}
                data = cur.execute("SELECT format, name FROM data WHERE book={}".format(row["id"])).fetchall()
                for d in data:
                    if d["format"] == "PDF":
                        doc["id"] = f"{self._prefix}_{str(row['id'])}"
                        doc["title"] = row["title"]
                        doc["file"] = os.path.join(calibrePath, row["path"], d["name"] + ".pdf")
                        doc["authors"] = [a['name'] for a in cur.execute("SELECT name FROM books_authors_link LEFT JOIN authors ON author=authors.id WHERE book={}".format(row["id"])).fetchall()]
                        doc["publishers"] = [p['name'] for p in cur.execute("SELECT name FROM books_publishers_link LEFT JOIN publishers ON publisher=publishers.id WHERE book={}".format(row["id"])).fetchall()]
                        doc["tags"] = [t["name"] for t in cur.execute("SELECT name FROM books_tags_link LEFT JOIN tags ON tag=tags.id WHERE book={}".format(row["id"])).fetchall()]
                        ratings = [r["rating"] for r in cur.execute("SELECT ratings.rating FROM books_ratings_link LEFT JOIN ratings ON books_ratings_link.rating=ratings.id WHERE book={}".format(row["id"])).fetchall()]
                        doc["rating"] = ratings[0] if len(ratings) > 0 else 0
                        documents.append(doc)
            return documents