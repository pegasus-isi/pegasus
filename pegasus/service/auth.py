from flask import request, Response, g

from pegasus.service import app, users

def authenticate(username, password):
    "Check username/password"
    try:
        user = users.getuser(username)
        if not user.password_matches(password):
            return False

        # This makes the user object available to the entire app
        g.user = user

        return True
    except users.NoSuchUser:
        return False

@app.before_request
def perform_basic_auth():
    auth = request.authorization
    if not auth or not authenticate(auth.username, auth.password):
        return Response('Basic Auth Required', 401,
                        {'WWW-Authenticate': 'Basic realm="Pegasus Service"'})

