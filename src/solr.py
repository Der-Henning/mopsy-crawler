import requests
import logging as log
import time
import sys

class Solr:
    def __init__(self, host, port, core):
        self._host = host
        self._port = port
        self._core = core

    def _buildURL(self, handler):
        return f"http://{self._host}:{self._port}/solr/{self._core}{handler}"

    def wait_for_connection(self):
        while True:
            try:
                solr_status = self.ping()
                if solr_status["status"] != "OK":
                    raise Exception("Solr not ready")
            except: 
                log.error(sys.exc_info())
                log.warning("Waiting for SOLR to start up")
                time.sleep(10)
            else:
                break

    def ping(self):
        url = self._buildURL("/admin/ping")
        res = requests.get(url, params={"wt": "json"})
        return res.json()

    def select(self, params):
        url = self._buildURL("/select")
        self.wait_for_connection()
        res = requests.get(url, params=params)
        return res.json()

    def commit(self, doc, params={"commit": "true"}):
        url = self._buildURL("/update/json/docs")
        self.wait_for_connection()
        res = requests.post(url, json=doc, params=params)
        return res.json()

    def extract(self, file, params={"extractOnly": "true"}):
        url = self._buildURL("/update/extract")
        files = {'file': open(file, 'rb')}
        self.wait_for_connection()
        res = requests.post(url, files=files, params=params)
        return res.json()

    def buildDict(self, params={"suggest.build": "true"}):
        url = self._buildURL("/suggest")
        self.wait_for_connection()
        res = requests.get(url, params=params)
        return res.json()

    def remove(self, docID):
        url = self._buildURL("/update")
        self.wait_for_connection()
        res = requests.post(url, json={"commit": {}, "delete": {"id": docID}})
        return res.json()

    def optimize(self):
        url = self._buildURL("/update")
        params = {"optimize": "true"}
        self.wait_for_connection()
        res = requests.get(url, params=params)
        return res.json()

    def isLangSupported(self, lang):
        url = self._buildURL("/schema/fields")
        self.wait_for_connection()
        res = requests.get(url).json()
        langIsSupported = any(x["name"] == f"content_txt_{lang}" for x in res["fields"])
        return langIsSupported
        