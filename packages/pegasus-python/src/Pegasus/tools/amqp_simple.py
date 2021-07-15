import ssl

import pika

try:
    # Python 3.0 and later
    from urllib import parse as urllib
except ImportError:
    # Fall back to Python 2's urllib
    import urllib

try:
    # Python 3.0 and later
    from urllib import parse as urlparse
except ImportError:
    # Fall back to Python 2's urllib
    import urlparse


def connect(amqp_url):
    # return pika.BlockingConnection(pika.connection.URLParameters(amqp_url))
    url = urlparse.urlparse(amqp_url)
    creds = pika.PlainCredentials(url.username, url.password)
    virtual_host = urllib.unquote(url.path.lstrip("/"))  # Replace %2F with /

    SSLOptions = None
    if url.scheme == "amqps":
        context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        SSLOptions = pika.SSLOptions(context)

    parameters = pika.ConnectionParameters(
        host=url.hostname,
        port=url.port,
        ssl_options=SSLOptions,
        virtual_host=virtual_host,
        credentials=creds,
    )
    return pika.BlockingConnection(parameters)
