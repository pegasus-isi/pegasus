from flask import Blueprint

from Pegasus.service import app

dashboard_routes = Blueprint("dashboard_routes", __name__)

from Pegasus.service.dashboard import views  # noqa: E402,F401 isort:skip

app.register_blueprint(dashboard_routes)
