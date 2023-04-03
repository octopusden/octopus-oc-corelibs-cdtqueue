#!/usr/bin/env python

from oc_cdt_queue2.queue_base import QueueBase
import pika
import json
import os
import re
import logging
import time


class QueueClient(QueueBase):
    """
    This is AMQP client class. It can be used stand-alone, but better use QueueRPC from queue_rpc.py
    """
    resend_on_fail = True

    # One can re-define it for debugging purposes or to implement another protocol driver
    # See queue_loopback.py as an somewhat dirty example
    _Connection = pika.BlockingConnection

    def __init__(self, *args, **kvargs):
        super(QueueClient, self).__init__(*args, **kvargs)
        self.routing_key = None
        self.exchange = None
        self.priority = None
        self.connection = None
        self.channel = None

    def basic_args(self, parser=None):
        """
        Call this to add specific AMQP arguments to your argparse parser.
        Uses internal argparse object by default. Should be used with setup_from_args()
        See QueueBase for details.

        :param parser: argparse object
        :returns: Modified parser
        """
        parser = super(QueueClient, self).basic_args(parser)
        parser.add_argument('--routing-key', help='Set routing key. Default is to use queue name', default=None)
        parser.add_argument('--exchange', help='Set exchange. Use default if not specified', default='')
        parser.add_argument('--priority', help='Messages priority', default=1, type=int)
        return parser

    def setup_from_args(self, args=None):
        """
        Converts argparse resulting namespace to ConnectionParameters.
        See BaseQueue for details

        :param args: args from your argparse parser extended by basic_args()
        :return: Used args
        """
        args = super(QueueClient, self).setup_from_args(args)
        self.setup(routing_key=args.routing_key, exchange=args.exchange, priority=args.priority)

        return args

    def setup(self, *args, **argv):
        """
        Sets things up. Takes all argument QueueBase.setup() accepts and few additional

        :param routing_key:    Routing key. Uses queue name as routing key if not specified
        :param exchange:    RabbitMQ exchange name. Uses default exchange if not specified
        :param priority:    Messages priority for sending

        All the rest params are the same as QueueBase.setup(). If used without url - no
        basic parameters touched, only client-specific are set
        """
        self.routing_key = argv.pop('routing_key', None)
        self.exchange = argv.pop('exchange', '')
        self.priority = argv.pop('priority', 1)

        if len(args) > 0 or 'url' in argv:
            super(QueueClient, self).setup(*args, **argv)
        elif len(argv) > 0:
            raise(TypeError('Unexpected arguments to setup() call'))
        if self.routing_key is None:
            self.routing_key = self.queue

    def _basic_publish(self, body, content_type, headers, content_encoding):
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                priority=self.priority,
                content_type=content_type,
                headers=headers,
                content_encoding=content_encoding
            ))

    def __connect(self, params):
        """
        This is a helper for handling reconnections
        """
        self.disconnect()
        self.connection = self._Connection(params)
        self.channel = self.connection.channel()

        if self.queue_declare != 'no':
            self.__queue_declare(deads_disabled=self.deads_disabled)

    def __queue_declare(self, deads_disabled=False):
        """
        Queue creation with or without dead messages queue and exchange
        """
        if deads_disabled:
            self.channel.queue_declare(queue=self.queue, durable=True, arguments={'x-max-priority': 3})
        else:
            queue_regex = re.compile('.*(?=\.)')
            try:
                deads_queue_name = queue_regex.findall(self.queue)[0] + '.deads'
            except IndexError:
                deads_queue_name = self.queue + '.deads'

            self.channel.exchange_declare(exchange=deads_queue_name, exchange_type='direct', durable=True)  # Creating dead-messages exchange
            self.channel.queue_declare(queue=deads_queue_name, durable=True, arguments={'x-max-priority': 3})  # Creating dead-messages queue
            self.channel.queue_bind(queue=deads_queue_name, exchange=deads_queue_name, routing_key=deads_queue_name)  # Binding the deads queue to the exchange
            # Creating or updating the existing exchange with the appropriate x-dead-letter arguments
            self.channel.queue_declare(queue=self.queue, durable=True, arguments={'x-max-priority': 3,
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
                if tries == 0:
                    raise
                if delay >= self.reconnect_delay:
                    delay = self.reconnect_delay
                logging.debug("Connection to %s:%d as user %s failed: %s - trying again in %d seconds...",
                              params.host, params.port, params.credentials.username, repr(e), delay)
                if tries > 0:
                    logging.debug("%d tries left", tries)
                    tries -= 1
                time.sleep(delay)

                if delay == 0:
                    delay = 1
                else:
                    delay *= 2
                continue
            break

    def disconnect(self):
        """
        Disconnect ignoring any error
        """
        try:
            if self.connection != None and self.connection.is_open == True:
                self.connection.close()
        except AttributeError:
            pass

    def send(self, body, content_type=None, headers={}, content_encoding=None):
        """
        Sends an message

        :param body:        Message body - string/dict/list. dicts and lists gets packed json
        :param content_type:    Message content_type. This will be set automatically if list or dict supplied
        :param headers:        dict of message headers
        :param content_encoding:Message content encoding

        :raises: anything pika.channel.basic_publish() can raise + anything json.dumps() in case of list/dict body
        """
        if isinstance(body, dict) or isinstance(body, list):
            content_type = 'application/json'
            body = json.dumps(body)

        try:
            self._basic_publish(body, content_type, headers, content_encoding)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosed,
                pika.exceptions.ConnectionClosed) as e:
            if self.resend_on_fail:
                logging.debug('Got an error while trying to send message, will try to reconnect and re-send', exc_info=True)
                self.connect()
                self._basic_publish(body, content_type, headers, content_encoding)
            else: 
                raise


