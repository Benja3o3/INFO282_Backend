from flask import Flask
from .database.db import db
from .routes.comuna_routes import comunasRoutes


app = Flask(__name__)
app.register_blueprint(comunasRoutes)
