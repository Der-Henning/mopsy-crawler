import requests

class Solr:
    def __init__(self, host, port, core):
        self._host = host
        self._port = port
        self._core = core

    def _buildURL(self, handler):
        return f"http://{self._host}:{self._port}/solr/{self._core}{handler}"

    def select(self, params):
        url = self._buildURL("/select")
        res = requests.get(url, params=params)
        return res.json()

    def commit(self, doc, params={"commit": "true"}):
        url = self._buildURL("/update/json/docs")
        res = requests.post(url, json=doc, params=params)
        return res.json()

    def extract(self, file, params={"extractOnly": "true"}):
        url = self._buildURL("/update/extract")
        files = {'file': open(file, 'rb')}
        res = requests.post(url, files=files, params=params)
        return res.json()

    def buildDict(self, params={"suggest.build": "true"}):
        url = self._buildURL("/suggest")
        res = requests.get(url, params=params)
        return res.json()

    def remove(self, docID):
        return self.commit({"delete": {"query": f"id:{docID}"}})

    def optimize(self):
        url = self._buildURL("/update")
        params = {"optimize": "true"}
        res = requests.get(url, params=params)
        return res.json()

    def isLangSupported(self, lang):
        url = self._buildURL("/schema/fields")
        res = requests.get(url).json()
        langIsSupported = any(x["name"] == f"content_txt_{lang}" for x in res["fields"])
        return langIsSupported

    def buildSchema(self, langs):
        url = self._buildURL("/schema")
        for lang in langs:
            type = "text_general" if lang=="other" else f"text_{lang}"
            data = {
                "add-field": {"name": "spellShingle", "type": "textSpellShingle", "multiValued": True, "indexed": True, "stored": True },
                "add-field": {"name": f"content_txt_{lang}", "type": type, "multiValued": True, "indexed": True, "stored": True },
                "add-field": {"name": f"tags_txt_{lang}", "type": type, "multiValued": True, "indexed": True, "stored": True },
                "add-dynamic-field": {"name": f"*_page_txt_{lang}", "type": type, "multiValued": False, "indexed": True, "stored": True },
                "add-copy-field": {"source": f"*_page_txt_{lang}", "dest": f"content_txt_{lang}" },
                "add-copy-field": {"source": f"*_txt_{lang}", "dest": "spellShingle" }
            }
            requests.post(url, json=data)
        