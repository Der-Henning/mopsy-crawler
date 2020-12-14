import requests

class Solr:
    def __init__(self, host, port, core):
        self.host = host
        self.port = port
        self.core = core

    def buildURL(self, handler):
        return f"http://{self.host}:{self.port}/solr/{self.core}{handler}"

    def select(self, params):
        url = self.buildURL("/select")
        res = requests.get(url, params=params)
        return res.json()

    def commit(self, doc, params={"commit": "true"}):
        url = self.buildURL("/update/json/docs")
        res = requests.post(url, json=doc, params=params)
        return res.json()

    def extract(self, file, params={"extractOnly": "true"}):
        url = self.buildURL("/update/extract")
        files = {'file': open(file, 'rb')}
        res = requests.post(url, files=files, params=params)
        return res.json()

    def buildDict(self, params={"suggest.build": "true"}):
        url = self.buildURL("/suggest")
        res = requests.get(url, params=params)
        return res.json()