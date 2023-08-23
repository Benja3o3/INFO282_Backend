from flask import Flask
from .controllers.initial import getHome

app = Flask(__name__)

# Routes

app.add_url_rule("/", "getHome", getHome, methods=["GET"])
