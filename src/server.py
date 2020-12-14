import flask
from flask import request, jsonify
import crawler
import os

server = flask.Flask(__name__)
server.config["DEBUG"] = True
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

def startServer():
   server.run(host='0.0.0.0', port=PORT)

if __name__ == "__main__":
   startServer()