import pika
import ssl

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
    #return pika.BlockingConnection(pika.connection.URLParameters(amqp_url))
    url = urlparse.urlparse(amqp_url)
    creds = pika.PlainCredentials(url.username, url.password)
    virtual_host = urllib.unquote(url.path.lstrip("/")) # Replace %2F with /
    parameters = pika.ConnectionParameters(host=url.hostname,
                                           port=url.port,
                                           ssl=(url.scheme == "amqps"),
                                           ssl_options={"cert_reqs": ssl.CERT_NONE},
                                           virtual_host=virtual_host,
                                           credentials=creds)
    return pika.BlockingConnection(parameters)

