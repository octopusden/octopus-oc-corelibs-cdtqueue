#!/usr/bin/env python

import pika
import os
import logging
import time
import re
from .queue_loopback import LoopbackConnection

logging.getLogger('pika').propagate = False

class QueueBase(object):
    """
    This is a base class for RabbitMQ interactions. You probably do not want to use it directly.
    Connection and channel setup is handled here (non-specific for client or server role).
    It also meant to be integrated to argparse, if needed - see child classes for details
    """

    default_queue_name = 'rpc'    # Default queue name
    default_reconnect_tries = 0    # Default connection tries, -1 for infinity
    default_reconnect_delay = 0    # Default max reconnect delay
    default_prefetch_count = 1      # Default prefetch count

    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue = None
        self.queue_declare = False

        self.parser = None
        self.args = None
        self.connection_parameters = None

        self.reconnect_tries = self.default_reconnect_tries
        self.reconnect_delay = self.default_reconnect_delay
        self.prefetch_count = self.default_prefetch_count

    # One can re-define it for debugging purposes or to implement another protocol driver
    # See queue_loopback.py as an somewhat dirty example
    _URLParameters = pika.URLParameters
    _Connection = LoopbackConnection

    def _get_connection_channel(self):
        """
        This is an alias for connection.channel()
        One can redefine this for debugging purposes or to implement another protocol driver

        :returns: pika.channel.Channel or compatible
        :raises: Anything pika.channel.Channel can raise
        """
        return self.connection.channel()

    def basic_args(self, parser = None):
        """
        This is intended as an aid for argparse-driven commandline utilities
        You may want to extend this method to add your own command-line arguments

        :param parser:    Supply your own parser. If not specified - self.parser will be used
        :returns:    Parser extended with RabbitMQ connection parameters
        """
        if parser == None: parser = self.parser
        parser.add_argument('--amqp-url', '-u', help = 'Queue server url in form: amqp://user:pass@host:port/<vhost>[?params]', 
                                default = os.getenv('AMQP_URL', 'amqp://guest:guest@localhost/'))
        parser.add_argument('--queue', '-q', help = 'Queue name', default = os.getenv('AMQP_QUEUE', self.default_queue_name))
        parser.add_argument('--declare', help = 'Ensure queue exists: yes, no, only. Use "only" just to set queue without handling any requests', 
                                choices = ['yes','no','only'], default = 'no', const = 'yes', nargs = '?')
        parser.add_argument('--deads-disabled', help = 'Disable additional dead messages queue and exchange creation', default = False, action = 'store_true')
        parser.add_argument('--amqp-username', '-l', help = 'Queue server connection username', default = os.getenv('AMQP_USER'))
        parser.add_argument('--amqp-password', help = 'Queue server connection password', default = os.getenv('AMQP_PASSWORD'))

        parser.add_argument('--reconnect-tries', help = 'Number of connection tries', default = self.default_reconnect_tries, type = int)
        parser.add_argument('--reconnect-delay', help = 'Max delay between reconnections', default = self.default_reconnect_delay, type = int)
        parser.add_argument('--prefetch-count', help = 'Pre-fetch count', default = self.default_prefetch_count, type = int)

        return parser

    def setup_from_args(self, args = None):
        """
        This is an aid for your argparse-driven commandline application.
        Converts argparse resulting namespace to ConnectionParameters you got
        by parsing arguments using basic_args()-extended parser to connection
        parameters.

        :param args:    Argparse namespace, self.args used if not specified
        :returns:    args used for setup
        :raises:    See setup() method
        """
        if args is None: args = self.args
        declare = (args.declare != 'no')
        self.setup(args.amqp_url, args.amqp_username, args.amqp_password, args.queue, declare, args.deads_disabled, args.reconnect_tries, args.reconnect_delay, args.prefetch_count)
        return args

    def setup(self, url, username = None, password = None, queue = None, queue_declare = False, deads_disabled = False, reconnect_tries = None, reconnect_delay = None, prefetch_count = None):
        """
        Sets up basic AMQP parameters. Call this (or setup_from_args()) before connecting 

        :param url:        AMQP url with or without username
        :param username:    If supplied - overrides one specified in URL
        :param password:    If supplied - overrides one specified in URL
        :param queue:        Queue name to use
        :param declare:        If set to True - will declare this queue on server
        :raises:        Raises anything URLParameters() constructor can rise
        """
        urlparams = self._URLParameters(url)
        if username is not None: urlparams.credentials.username = username
        if password is not None: urlparams.credentials.password = password
        self.connection_parameters = urlparams
        if queue is None: queue = self.default_queue_name
        self.queue = queue
        self.queue_declare = queue_declare
        self.deads_disabled = deads_disabled
        if reconnect_tries is not None: self.reconnect_tries = reconnect_tries
        if reconnect_delay is not None: self.reconnect_delay = reconnect_delay
        if prefetch_count is not None: self.prefetch_count = prefetch_count

    def __connect(self, params):
        """
        This is a helper for handling reconnections
        """
        self.disconnect()
        self.connection = self._Connection(params)
        self.channel = self._get_connection_channel()
        self.channel.basic_qos(prefetch_count = self.prefetch_count)
        if self.queue_declare: self.__queue_declare(deads_disabled = self.deads_disabled)

    def __queue_declare(self, deads_disabled = False):
        """
        Queue creation with or without dead messages queue and exchange
        """
        if deads_disabled:
            self.channel.queue_declare(queue = self.queue, durable = True, arguments = {'x-max-priority': 3})
        else:
            queue_regex = re.compile('.*(?=\.)')
            try:
                deads_queue_name = queue_regex.findall(self.queue)[0] + '.deads'
            except IndexError:
                deads_queue_name = self.queue + '.deads'
            self.channel.exchange_declare(exchange = deads_queue_name, exchange_type = 'direct', durable = True) # Creating dead-messages exchange
            self.channel.queue_declare(queue = deads_queue_name, durable = True, arguments = {'x-max-priority': 3}) # Creating dead-messages queue
            self.channel.queue_bind(queue = deads_queue_name, exchange = deads_queue_name, routing_key = deads_queue_name) # Binding the deads queue to the exchange
            # Creating or updating the existing exchange with the appropriate x-dead-letter arguments
            self.channel.queue_declare(queue = self.queue, durable = True, arguments = {'x-max-priority': 3, 
                                                                                        'x-dead-letter-exchange': deads_queue_name, 
                                                                                        'x-dead-letter-routing-key': deads_queue_name}) 
            
    def connect(self):
        """
        Connects to server and allocates channel. If allready connected - replaces the old connection
        with new one. Sets self.connection and seld.channel properties and declares queue if asked by setup()

        :raises:    Anything BlockingConnection() can raise
        """
        params = self.connection_parameters
        tries = self.reconnect_tries
        delay = 0

        while True:
            try:
                self.__connect(params)
            except (pika.exceptions.AMQPConnectionError,
                    pika.exceptions.ChannelClosed,
                    pika.exceptions.ConnectionClosed) as e:
                if tries == 0: raise
                if delay >= self.reconnect_delay: delay = self.reconnect_delay
                logging.debug("Connection to %s:%d as user %s failed: %s - trying again in %d seconds...", 
                        params.host, params.port, params.credentials.username, repr(e), delay)
                if tries > 0: 
                    logging.debug("%d tries left", tries)
                    tries -= 1
                time.sleep(delay)
                if delay == 0: delay = 1
                else: delay *= 2
                continue
            break



    def disconnect(self):
        """
        Disconnect ignoring any error
        """
        try:
            if self.connection != None and self.connection.is_open == True: self.connection.close()
        except AttributeError:
            pass

    def __del__(self):
        self.disconnect()


