from flask import Blueprint

dashboard_routes = Blueprint('dashboard_routes', __name__)

from Pegasus.service.dashboard import views

from Pegasus.service import app

app.register_blueprint(dashboard_routes)
