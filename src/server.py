import flask
from flask import request, jsonify

server = flask.Flask(__name__)
server.config["DEBUG"] = True

@server.route("/", methods=['GET'])
 def hello():
    return "Hello World!"

if __name__ == "__main__":
   server.run()