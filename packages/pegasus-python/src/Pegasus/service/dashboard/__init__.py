from flask import Blueprint
import os

blueprint = Blueprint("dashboard", __name__)

from Pegasus.service.dashboard import views  # noqa: E402,F401 isort:skip
