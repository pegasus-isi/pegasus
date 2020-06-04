import logging
from json import JSONEncoder, dumps

from flask import current_app, make_response, request

__all__ = (
    "serialize",
    "jsonify",
)

log = logging.getLogger(__name__)


def serialize(rv):
    log.debug("Serializing output")

    if rv is None or (isinstance(rv, str) and not len(rv)):
        log.info("No content")
        rv = make_response("", 204)
    elif (
        isinstance(rv, current_app.response_class)
        or callable(rv)
        or isinstance(rv, str)
    ):
        ...
    else:
        log.info("Serializing")
        rv = jsonify(rv)
        if request.method == "POST":
            make_response(rv, 201)

    return rv


def jsonify(*args, **kwargs):
    if args and kwargs:
        raise TypeError("jsonify() behavior undefined when passed both args and kwargs")
    elif len(args) == 1:  # single args are passed directly to dumps()
        data = args[0]
    else:
        data = args or kwargs

    pretty_print = bool(
        request.args.get(
            "pretty-print", current_app.config["JSONIFY_PRETTYPRINT_REGULAR"]
        )
    )

    indent = None
    separators = (",", ":")
    cls = current_app.json_encoder or JSONEncoder

    if pretty_print is True and request.is_xhr is False:
        indent = 2
        separators = (", ", ": ")

    if hasattr(request, "operation") and request.operation.produces:
        mime_type = request.operation.produces[0]
    elif "JSONIFY_MIMETYPE" in current_app.config:
        mime_type = current_app.config["JSONIFY_MIMETYPE"]
    else:
        mime_type = "application/json; charset=utf-8"

    json_str = dumps(data, indent=indent, separators=separators, cls=cls) + "\n"
    json_str.encode("utf-8")

    return current_app.response_class(json_str, mimetype=mime_type)
