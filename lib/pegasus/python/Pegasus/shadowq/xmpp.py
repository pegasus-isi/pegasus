import sys
import logging
import getpass
from optparse import OptionParser

import sleekxmpp
from sleekxmpp.xmlstream import ET, tostring
from sleekxmpp.xmlstream.matcher import StanzaPath
from sleekxmpp.xmlstream.handler import Callback
from sleekxmpp.exceptions import IqError, IqTimeout

# Python versions before 3.0 do not use UTF-8 encoding
# by default. To ensure that Unicode is handled properly
# throughout SleekXMPP, we will set the default encoding
# ourselves to UTF-8.
if sys.version_info < (3, 0):
    from sleekxmpp.util.misc_ops import setdefaultencoding
    setdefaultencoding('utf8')

class PubsubClient(sleekxmpp.ClientXMPP):

    def __init__(self, jid, password, server):
        super(PubsubClient, self).__init__(jid, password)

        self.register_plugin('xep_0004') # Data forms
        self.register_plugin('xep_0030') # Service Discovery
        self.register_plugin('xep_0059')
        self.register_plugin('xep_0060')
        self.register_plugin('xep_0066') # Out-of-band Data
        self.register_plugin('xep_0077') # In-band Registration

        # Some servers don't advertise support for inband registration, even
        # though they allow it. If this applies to your server, use:
        #self['xep_0077'].force_registration = True

        self.pubsub_server = server

        self.add_event_handler('session_start', self.start)

        # The register event provides an Iq result stanza with
        # a registration form from the server. This may include
        # the basic registration fields, a data form, an
        # out-of-band URL, or any combination. For more advanced
        # cases, you will need to examine the fields provided
        # and respond accordingly. SleekXMPP provides plugins
        # for data forms and OOB links that will make that easier.
        self.add_event_handler("register", self.register, threaded=True)

        self.register_handler(Callback('Pubsub event', StanzaPath('message/pubsub_event'), self.handle_event))

    def register(self, iq):
        """
        Fill out and submit a registration form.

        The form may be composed of basic registration fields, a data form,
        an out-of-band link, or any combination thereof. Data forms and OOB
        links can be checked for as so:

        if iq.match('iq/register/form'):
            # do stuff with data form
            # iq['register']['form']['fields']
        if iq.match('iq/register/oob'):
            # do stuff with OOB URL
            # iq['register']['oob']['url']

        To get the list of basic registration fields, you can use:
            iq['register']['fields']
        """
        resp = self.Iq()
        resp['type'] = 'set'
        resp['register']['username'] = self.boundjid.user
        resp['register']['password'] = self.password

        try:
            resp.send(now=True)
            logging.info("Account created for %s!" % self.boundjid)
        except IqError as e:
            logging.error("Could not register account: %s" % e.iq['error']['text'])
            self.disconnect()
        except IqTimeout:
            logging.error("No response from server.")
            self.disconnect()

    def handle_event(self, msg):
        print('Received pubsub event: %s' % msg['pubsub_event'])

    def start(self, event):
        self.get_roster()
        self.send_presence()

    def items(self, node):
        try:
            result = self['xep_0060'].get_nodes(self.pubsub_server, node)
            return result['disco_items']['items']
        except:
            logging.error('Could not retrieve node list.')

    def create(self, node):
        try:
            self['xep_0060'].create_node(self.pubsub_server, node)
            logging.info('Created node: %s' % node)
        except:
            logging.error('Could not create node: %s' % node)

    def delete(self, node):
        try:
            self['xep_0060'].delete_node(self.pubsub_server, node)
            logging.info('Deleted node: %s' % node)
        except:
            logging.error('Could not delete node: %s' % node)

    def publish(self, node, data):
        payload = ET.fromstring("<test xmlns='test'>%s</test>" % data)
        try:
            result = self['xep_0060'].publish(self.pubsub_server, node, payload=payload)
            id = result['pubsub']['publish']['item']['id']
            logging.info('Published at item id: %s' % id)
            return id
        except:
            logging.error('Could not publish to: %s' % node)

    def get(self, node, item):
        try:
            result = self['xep_0060'].get_item(self.pubsub_server, node, item)
            return result['pubsub']['items']['substanzas']
        except:
            logging.error('Could not retrieve item %s from node %s' % (item, node))

    def retract(self, node, item):
        try:
            result = self['xep_0060'].retract(self.pubsub_server, node, item)
            logging.info('Retracted item %s from node %s' % (item, node))
        except:
            logging.error('Could not retract item %s from node %s' % (item, node))

    def purge(self, node):
        try:
            result = self['xep_0060'].purge(self.pubsub_server, node)
            logging.info('Purged all items from node %s' % self.node)
        except:
            logging.error('Could not purge items from node %s' % self.node)

    def subscribe(self, node):
        try:
            result = self['xep_0060'].subscribe(self.pubsub_server, node)
            logging.info('Subscribed %s to node %s' % (self.boundjid.bare, node))
        except:
            logging.error('Could not subscribe %s to node %s' % (self.boundjid.bare, node))

    def unsubscribe(self):
        try:
            result = self['xep_0060'].subscribe(self.pubsub_server, node)
            logging.info('Unsubscribed %s from node %s' % (self.boundjid.bare, node))
        except:
            logging.error('Could not unsubscribe %s from node %s' % (self.boundjid.bare, node))


if __name__ == '__main__':
    usage = "Usage: %%prog [options] <jid> items|create|delete|purge|subscribe|unsubscribe|publish|retract|get [<node> <data>]"

    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s %(message)s')

    jid = raw_input("Username: ")
    password = getpass.getpass("Password: ")

    xmpp = PubsubClient(jid, password, server="pubsub.localhost")

    # If you are working with an OpenFire server, you may need
    # to adjust the SSL version used:
    # xmpp.ssl_version = ssl.PROTOCOL_SSLv3

    # If you want to verify the SSL certificates offered by a server:
    # xmpp.ca_certs = "path/to/ca/cert"

    # Connect to the XMPP server and start processing XMPP stanzas.
    if xmpp.connect():
        # If you do not have the dnspython library installed, you will need
        # to manually specify the name of the server if it does not match
        # the one in the JID. For example, to use Google Talk you would
        # need to use:
        #
        # if xmpp.connect(('talk.google.com', 5222)):
        #     ...
        xmpp.process(block=True)
        xmpp.disconnect()
    else:
        logging.error("Unable to connect.")

#jid = 734269f6-49f8-4d5f-931f-8bcba8c9a3a3@geni-imf-xmpp.renci.org:5222
#password = ????
#server = pubsub.geni-imf-xmpp.renci.org
#node = orca/sq

#ORCA.pubsub.server=geni-imf-xmpp.renci.org:5222
#ORCA.pubsub.usecertificate=true
#ORCA.pubsub.preferednodes=orca/sq
#ORCA.pubsub.login=734269f6-49f8-4d5f-931f-8bcba8c9a3a3
#ORCA.pubsub.password=<password_protecting_keystore>
#ORCA.pubsub.keystorepath=<full_path_to_orca-gcf-encrypted.jks>
#ORCA.pubsub.keystoretype=jks
#ORCA.pubsub.truststorepath=<full_path_to_orca-gcf-encrypted.jks>
#ORCA.pubsub.root=orca/sq

