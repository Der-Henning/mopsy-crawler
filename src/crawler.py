import os
import sys
import sqlite3

def start():
    return "Crawler gestartet"


def stop():
    return "Crawler gestoppt"


def status():
    return "status"


def setConfig():
    return "config set"


def getConfig():
    print(os.environ, file=sys.stderr)
    return "config"
