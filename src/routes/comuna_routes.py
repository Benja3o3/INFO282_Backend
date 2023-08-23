from flask import Blueprint, jsonify
from ..controllers.comuna_controller import getComunas

comunasRoutes = Blueprint("comunas", __name__)


@comunasRoutes.route("/comunas", methods=["GET"])
def getComunasRoute():
    comunas = getComunas()
    return jsonify(comunas)
