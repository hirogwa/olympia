from olympia import app
from logging.handlers import RotatingFileHandler
import logging
import os

handler = RotatingFileHandler('olympia.log', maxBytes=10000000, backupCount=1)
handler.setLevel(logging.DEBUG)
handler.setFormatter(logging.Formatter(
    "%(asctime)s %(pathname)s:%(lineno)d %(levelname)s - %(message)s"))
app.logger.addHandler(handler)

app.debug = True
app.run(
    host='0.0.0.0',
    port=int(os.getenv('PORT', 5001))
)
