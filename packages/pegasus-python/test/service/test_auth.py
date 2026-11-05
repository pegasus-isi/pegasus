import os

import flask
import pytest

from Pegasus.service.auth import LocalTokenAuthentication


@pytest.fixture
def app():
    return flask.Flask(__name__)


class TestLocalTokenAuthentication:
    @pytest.mark.parametrize(
        "configured,password,expected",
        [
            ("secret", "secret", True),
            ("secret", "wrong", False),
            ("secret", "", False),
            ("secret", None, False),
            (None, "secret", False),
            ("", "", False),
        ],
    )
    def test_authenticate(self, app, configured, password, expected):
        app.config["EM_TOKEN"] = configured
        with app.app_context():
            assert LocalTokenAuthentication("anyone", password).authenticate() is (
                expected
            )

    def test_token_unset(self, app):
        app.config.pop("EM_TOKEN", None)
        with app.app_context():
            assert LocalTokenAuthentication("u", "secret").authenticate() is False

    def test_get_user_is_service_owner(self, app):
        user = LocalTokenAuthentication("someone-else", "secret").get_user()
        assert user.uid == os.getuid()


def test_resolved_by_lifecycle():
    # lifecycle.authenticate() looks the class up by name in its globals()
    from Pegasus.service import lifecycle

    assert lifecycle.__dict__["LocalTokenAuthentication"] is LocalTokenAuthentication
