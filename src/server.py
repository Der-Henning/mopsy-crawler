import flask
from flask import request, jsonify
import crawler

server = flask.Flask(__name__)
server.config["DEBUG"] = True

@server.route("/", methods=['GET'])
def status():
   return crawler.status()

@server.route("/start", methods=['POST'])
def start():
   return crawler.start()

@server.route("/stop", methods=['POST'])
def stop():
   return crawler.stop()

@server.route("/config", methods=['GET'])
def getConfig():
   return jsonify(crawler.getConfig())

@server.route("/config", methods=['POST'])
def setConfig():
   return crawler.setConfig()

if __name__ == "__main__":
   server.run(host='0.0.0.0')