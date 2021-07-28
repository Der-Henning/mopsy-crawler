import flask
from flask import request, jsonify
from crawler import Crawler
import os
import logging as log

LOGLEVEL = log.DEBUG if os.getenv("DEBUG") == "true" else log.INFO
PORT = os.getenv("PORT", 80)

log.basicConfig(level=LOGLEVEL, format='%(levelname)s - %(name)s - %(message)s',)

server = flask.Flask("api")
server.config["DEBUG"] = False

crawler = Crawler()

@server.route("/", methods=['GET'])
def status():
   return jsonify(crawler.getStatus())

@server.route("/start", methods=['POST'])
def start():
   return jsonify(crawler.start())

@server.route("/stop", methods=['POST'])
def stop():
   return jsonify(crawler.stop())

@server.route("/toggleAutorestart", methods=["POST"])
def toggleAutorestart():
   return jsonify(crawler.toggleAutorestart())

@server.route("/fieldList", methods=["GET"])
def fieldList():
   return jsonify(crawler.Documents.fieldList)

def startServer():
   server.run(host='0.0.0.0', port=PORT)

if __name__ == "__main__":
   startServer()