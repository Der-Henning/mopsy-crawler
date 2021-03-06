import os

CRAWLER_NAME = os.getenv("CRAWLER_NAME", "Calibre")
CRAWLER = os.getenv("CRAWLER_TYPE", "calibre")
SOLR_HOST = os.getenv("MOPSY_SOLR_HOST", "solr")
SOLR_PORT = os.getenv("MOPSY_SOLR_PORT", 8983)
SOLR_CORE = os.getenv("MOPSY_SOLR_CORE", "mopsy")
SOLR_PREFIX = os.getenv("MOPSY_SOLR_PREFIX", "calibre")
DIRECT_COMMIT = True if os.getenv("CRAWLER_DIRECT_COMMIT") == "true" else False
AUTORESTART = False if os.getenv("CRAWLER_AUTORESTART") == "false" else True
AUTOSTART = False if os.getenv("CRAWLER_AUTOSTART") == "false" else True
SLEEP_TIME = int(os.getenv("CRAWLER_SLEEP_TIME", 0))