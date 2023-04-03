#!/usr/bin/env python

import pika
import os
import logging
import time
from abc import abstractmethod

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

    # One can re-define it for debugging purposes or to implement another protocol driver
    # See queue_loopback.py as an somewhat dirty example
    _URLParameters = pika.URLParameters

    def __init__(self):
        self.queue = None
        self.queue_declare = 'no'

        self.parser = None
        self.args = None
        self.connection_parameters = None

        self.reconnect_tries = self.default_reconnect_tries
        self.reconnect_delay = self.default_reconnect_delay

        self.deads_disabled = False

    def basic_args(self, parser=None):
        """
        This is intended as an aid for argparse-driven commandline utilities
        You may want to extend this method to add your own command-line arguments

        :param parser:    Supply your own parser. If not specified - self.parser will be used
        :returns:    Parser extended with RabbitMQ connection parameters
        """
        if parser == None:
            parser = self.parser
        parser.add_argument('--amqp-url', '-u', help='Queue server url in form: amqp://user:pass@host:port/<vhost>[?params]',
                            default=os.getenv('AMQP_URL', 'amqp://guest:guest@localhost/'))
        parser.add_argument('--queue', '-q', help='Queue name',
                            default=os.getenv('AMQP_QUEUE', self.default_queue_name))
        parser.add_argument('--declare', help='Ensure queue exists: yes, no, only. Use "only" just to set queue without handling any requests',
                            choices=['yes', 'no', 'only'], default='no', const='yes', nargs='?')
        parser.add_argument(
            '--deads-disabled', help='Disable additional dead messages queue and exchange creation', default=False, action='store_true')
        parser.add_argument('--amqp-username', '-l',
                            help='Queue server connection username', default=os.getenv('AMQP_USER'))
        parser.add_argument(
            '--amqp-password', help='Queue server connection password', default=os.getenv('AMQP_PASSWORD'))

        parser.add_argument('--reconnect-tries', help='Number of connection tries',
                            default=self.default_reconnect_tries, type=int)
        parser.add_argument('--reconnect-delay', help='Max delay between reconnections',
                            default=self.default_reconnect_delay, type=int)

        return parser

    def setup_from_args(self, args=None):
        """
        This is an aid for your argparse-driven commandline application.
        Converts argparse resulting namespace to ConnectionParameters you got
        by parsing arguments using basic_args()-extended parser to connection
        parameters.

        :param args:    Argparse namespace, self.args used if not specified
        :returns:    args used for setup
        :raises:    See setup() method
        """
        if args is None:
            args = self.args
        self.setup(args.amqp_url, args.amqp_username, args.amqp_password, args.queue,
                   args.declare, args.deads_disabled, args.reconnect_tries, args.reconnect_delay)
        return args

    def setup(self, url, username=None, password=None, queue=None, queue_declare='no', deads_disabled=False, reconnect_tries=None, reconnect_delay=None):
        """
        Sets up basic AMQP parameters. Call this (or setup_from_args()) before connecting 

        :param url:        AMQP url with or without username
        :param username:    If supplied - overrides one specified in URL
        :param password:    If supplied - overrides one specified in URL
        :param queue:        Queue name to use
        :param queue_declare: ['yes', 'no', 'only'], 'yes' - declare and continue, 'only' - declare and stop processing (for server', 'no' - do not declare
        :raises:        Raises anything URLParameters() constructor can rise
        """
        urlparams = self._URLParameters(url)
        if username is not None:
            urlparams.credentials.username = username
        if password is not None:
            urlparams.credentials.password = password
        self.connection_parameters = urlparams
        if queue is None:
            queue = self.default_queue_name
        self.queue = queue
        self.queue_declare = queue_declare
        self.deads_disabled = deads_disabled
        if reconnect_tries is not None:
            self.reconnect_tries = reconnect_tries
        if reconnect_delay is not None:
            self.reconnect_delay = reconnect_delay

    @abstractmethod
    def connect(self):
        """
        Connects to server and allocates channel. If allready connected - replaces the old connection
        with new one. Sets self.connection and seld.channel properties and declares queue if asked by setup()

        To be implemented in derived classes
        """
        pass

    @abstractmethod
    def disconnect(self):
        """
        Disconnect ignoring any error

        To be implemented in derived classes
        """
        pass

    def __del__(self):
        self.disconnect()
