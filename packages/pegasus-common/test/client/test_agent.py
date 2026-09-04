import requests

from Pegasus.client import agent


class _FakeResponse:
    """A minimal stand-in for requests.Response."""

    def __init__(self, status_code=200, json_data=None, error_body=""):
        self.status_code = status_code
        self._json_data = json_data or {}
        self._error_body = error_body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(
                f"{self.status_code} Client Error: {self._error_body}"
            )

    def json(self):
        return self._json_data


def _make_client(mocker, client_version="5.2.0dev"):
    """An AgentClient that doesn't touch real properties files or subprocesses."""
    mocker.patch("Pegasus.client.agent.properties.Properties.new", return_value=None)
    mocker.patch(
        "Pegasus.client.agent.properties.Properties.property", return_value=None
    )
    mocker.patch(
        "Pegasus.client.agent.utils.pegasus_version", return_value=client_version
    )
    return agent.AgentClient()


def test_analyze_posts_expected_contract(mocker):
    """analyze() must POST to /wf/analyze/ai/<id> with the API key header and a
    JSON body matching the service's WFAnalyzeRequest schema (client_version
    and analyze_stdout, both strings)."""
    client = _make_client(mocker)
    mock_post = mocker.patch(
        "Pegasus.client.agent.requests.post",
        return_value=_FakeResponse(200, {"message": "looks fine"}),
    )

    result = client.analyze("wf-123", "some analyzer output")

    assert result == "looks fine"
    args, kwargs = mock_post.call_args
    assert args[0] == "https://api.pegasus-ai.org/wf/analyze/ai/wf-123"
    assert kwargs["headers"] == {
        "X-API-Key": "default",
        "Content-Type": "application/json",
    }
    payload = kwargs["json"]
    assert payload == {
        "client_version": "5.2.0dev",
        "analyze_stdout": "some analyzer output",
    }


def test_analyze_truncates_stdout_to_api_max_length(mocker):
    client = _make_client(mocker)
    mock_post = mocker.patch(
        "Pegasus.client.agent.requests.post",
        return_value=_FakeResponse(200, {"message": "ok"}),
    )
    huge = "x" * (agent.API_MAX_LENGTH + 500)

    client.analyze("wf-123", huge)

    payload = mock_post.call_args.kwargs["json"]
    assert len(payload["analyze_stdout"]) == agent.API_MAX_LENGTH


def test_analyze_raises_runtimeerror_on_http_error(mocker):
    """Mirrors the real-world failure: the service returns 422 Unprocessable
    Content when the request body fails WFAnalyzeRequest validation, and
    analyze() must surface that as a RuntimeError rather than propagating the
    raw requests exception."""
    client = _make_client(mocker)
    mocker.patch(
        "Pegasus.client.agent.requests.post",
        return_value=_FakeResponse(
            422,
            error_body='{"detail":[{"type":"string_type","loc":["body",'
            '"client_version"],"msg":"Input should be a valid string",'
            '"input":null}]}',
        ),
    )

    try:
        client.analyze("wf-123", "some output")
        assert False, "expected RuntimeError"
    except RuntimeError as e:
        assert "422" in str(e)


def test_client_version_is_never_none(mocker):
    """Regression test: pegasus_version() returns None when it can't locate
    the pegasus-version binary (e.g. sys.argv[0] has no directory component,
    which happens when invoked via the unified `pegasus` CLI -- see
    test_utils.TestPegasusVersion). The service's WFAnalyzeRequest/
    WFStatisticsRequest schemas require client_version to be a string, so a
    JSON null there is exactly what produces the 422 seen in the wild.
    AgentClient must coerce it to a non-null string before posting."""
    client = _make_client(mocker, client_version=None)
    mock_post = mocker.patch(
        "Pegasus.client.agent.requests.post",
        return_value=_FakeResponse(200, {"message": "ok"}),
    )

    client.analyze("wf-123", "some output")

    payload = mock_post.call_args.kwargs["json"]
    assert payload["client_version"] is not None
    assert isinstance(payload["client_version"], str)
    assert payload["client_version"] != ""


def test_statistics_posts_expected_contract(mocker):
    client = _make_client(mocker)
    mock_post = mocker.patch(
        "Pegasus.client.agent.requests.post",
        return_value=_FakeResponse(200, {"message": "stats summary"}),
    )

    result = client.statistics("wf-123", "some stats output")

    assert result == "stats summary"
    args, kwargs = mock_post.call_args
    assert args[0] == "https://api.pegasus-ai.org/wf/statistics/ai/wf-123"
    payload = kwargs["json"]
    assert payload == {
        "client_version": "5.2.0dev",
        "statistics_stdout": "some stats output",
    }


def test_url_and_token_come_from_properties(mocker):
    "pegasus.agent.url / pegasus.agent.token properties override the defaults"
    mocker.patch("Pegasus.client.agent.properties.Properties.new", return_value=None)
    mocker.patch(
        "Pegasus.client.agent.properties.Properties.property",
        side_effect=lambda key, val=None: {
            "pegasus.agent.url": "https://custom.example.org",
            "pegasus.agent.token": "mytoken",
        }.get(key),
    )
    mocker.patch("Pegasus.client.agent.utils.pegasus_version", return_value="5.2.0dev")
    mock_post = mocker.patch(
        "Pegasus.client.agent.requests.post",
        return_value=_FakeResponse(200, {"message": "ok"}),
    )

    client = agent.AgentClient()
    client.analyze("wf-123", "output")

    args, kwargs = mock_post.call_args
    assert args[0] == "https://custom.example.org/wf/analyze/ai/wf-123"
    assert kwargs["headers"]["X-API-Key"] == "mytoken"
