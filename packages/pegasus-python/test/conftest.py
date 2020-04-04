import logging
from pathlib import Path

from click.testing import CliRunner

import pytest
from flask import _request_ctx_stack

log = logging.getLogger(__name__)


class FlaskTestClient:
    log = logging.getLogger(__name__)

    def __init__(self, app):
        self.app = app
        self.client = app.test_client()
        self.get_context = self.get_with_context
        self.post_context = self.post_with_context
        self.put_context = self.put_with_context
        self.patch_context = self.patch_with_context
        self.delete_context = self.delete_with_context

    def get(self, *a, headers={"Accept": "application/json"}, **kw):
        return self.client.get(*a, headers=headers, **kw)

    def post(self, *a, headers={"Accept": "application/json"}, **kw):
        return self.client.post(*a, headers=headers, **kw)

    def put(self, *a, headers={"Accept": "application/json"}, **kw):
        return self.client.put(*a, headers=headers, **kw)

    def patch(self, *a, headers={"Accept": "application/json"}, **kw):
        return self.client.patch(*a, headers=headers, **kw)

    def delete(self, *a, headers={"Accept": "application/json"}, **kw):
        return self.client.delete(*a, headers=headers, **kw)

    def get_with_context(
        self, uri, data=None, headers=None, pre_callable=None, **kwargs
    ):
        return self.request_with_context(
            uri, "GET", data, headers=headers, pre_callable=pre_callable, **kwargs
        )

    def post_with_context(
        self, uri, data=None, headers=None, pre_callable=None, **kwargs
    ):
        return self.request_with_context(
            uri, "POST", data, headers=headers, pre_callable=pre_callable, **kwargs
        )

    def put_with_context(
        self, uri, data=None, headers=None, pre_callable=None, **kwargs
    ):
        return self.request_with_context(
            uri, "PUT", data, headers=headers, pre_callable=pre_callable, **kwargs
        )

    def patch_with_context(
        self, uri, data=None, headers=None, pre_callable=None, **kwargs
    ):
        return self.request_with_context(
            uri, "PATCH", data, headers=headers, pre_callable=pre_callable, **kwargs
        )

    def delete_with_context(
        self, uri, data=None, headers=None, pre_callable=None, **kwargs
    ):
        return self.request_with_context(
            uri, "DELETE", data, headers=headers, pre_callable=pre_callable, **kwargs
        )

    def request_with_context(
        self, uri, method="GET", data=None, headers=None, pre_callable=None, **kwargs
    ):
        """
        Take a request through Flask"s request lifecycle in a test context.

        :param uri: URI to call
        :param method: HTTP verb to call request with
        :param headers: headers to be passed along with the request.
        :param pre_callable: A callable method, invoked after pre_processing functions of Flask are called.
        :param kwargs: data to be passed along with request
        :return: Flask Response object
        """
        _headers = {"Accept": "application/json"}
        if headers:
            _headers.update(headers)

        with self.app.test_request_context(
            uri, method=method, data=data, headers=_headers, **kwargs
        ):
            try:
                self.app.try_trigger_before_first_request_functions()

                # Pre process Request
                rv = self.app.preprocess_request()

                if pre_callable is not None:
                    pre_callable()

                if rv is None:
                    # Main Dispatch
                    rv = self.app.dispatch_request()

            except Exception as e:
                rv = self.app.handle_user_exception(e)

            response = self.app.make_response(rv)

            # Post process Request
            response = self.app.process_response(response)

        return response


@pytest.fixture(scope="session")
def app():
    from Pegasus.service.server import create_app

    app = create_app(env="testing")

    with app.app_context():
        yield app


@pytest.fixture()
def request_ctx(app):
    if _request_ctx_stack.top is None:
        with app.test_request_context("/"):
            app.preprocess_request()
            yield _request_ctx_stack.top
    else:
        return _request_ctx_stack.top


@pytest.fixture(scope="session")
def cli(app):
    return FlaskTestClient(app)


@pytest.fixture()
def runner():
    """Return click test runner."""
    runner = CliRunner(mix_stderr=False)
    return runner


@pytest.fixture()
def tmp_path(runner):
    with runner.isolated_filesystem():
        yield Path.cwd()
