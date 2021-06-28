from flask import Blueprint
import os

blueprint = Blueprint("dashboard", __name__,
                      static_folder="../static",
                      static_url_path="/static")


from Pegasus.service.dashboard import views  # noqa: E402,F401 isort:skip
