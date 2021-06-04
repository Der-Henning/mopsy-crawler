import flask
from flask import request, jsonify
import crawler
import os

server = flask.Flask(__name__)
server.config["DEBUG"] = True if os.getenv("DEBUG") == "true" else False
PORT = os.getenv("PORT", 80)

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