#!/usr/bin/env python

# this thing should have minimum dependencies
import pika
import json
import time
from .queue_base import QueueBase
import logging
from time import sleep as time_sleep

class QueueServer(QueueBase):

    def __init__(self, *argv, **argp):
        super(QueueServer,self).__init__(*argv, **argp)
        self.max_sleep = 16
        self._nacks = 0
        self.counter_messages = 0
        self.counter_good = 0
        self.counter_bad = 0

    def on_ack(self, body, properties):
        pass

    def on_nack(self, body, properties, result):
        pass

    def on_message(self, data, properties):
        """
        Redefine this to add your own processing
        """
        pass

    def on_message_raw(self, body, properties):
        """
        Redefine this to do your own raw message processing
        """

        if properties.content_type != 'application/json':
            raise ValueError("invalid message: content-type should be application/json")
        data = json.loads(body)
        return self.on_message(data, properties)

    def _sleep_on_nack(self):
        # we may catch TypeError exceptions if we sleep on active connection
        sleep = 2 ** self._nacks
        if sleep > self.max_sleep: sleep = self.max_sleep
        if sleep > 0:
            if not self.connection.is_closed:
                self.connection.sleep(sleep)
            else:
                time_sleep(sleep)


    def _basic_nack(self, channel, delivery_tag, requeue = True):
        channel.basic_nack(delivery_tag = delivery_tag, requeue = requeue)

    def _basic_ack(self, channel, delivery_tag):
        channel.basic_ack(delivery_tag = delivery_tag)

    def _on_nack(self, body, properties, result):
        logging.warning("Failure: %s", str(result))
        self.counter_bad += 1
        self._nacks += 1
        self.on_nack(body, properties, result)
        if self.max_sleep > 0: self._sleep_on_nack()

    def _on_ack(self, body, properties):
        self.counter_good += 1
        self._nacks = 0
        self.on_ack(body, properties)

    def __debug_message(self, properties, body):
        logging.debug("stats: total %d good %d bad %d", self.counter_messages, self.counter_good, self.counter_bad)
        logging.debug("Received message: %s", body)
        logging.debug("Message properties - content-type: %s (encoding: %s) type: %s priority: %s",
            properties.content_type, properties.content_encoding, properties.type, properties.priority)
        if len(properties.headers) > 0:
            logging.debug("Message headers: %s", repr(properties.headers))

    def __process_message(self, ch, method, properties, body):
        self.counter_messages += 1
        self.__debug_message(properties, body)

        try:
            self.on_message_raw(body, properties)
            self._basic_ack(ch, method.delivery_tag)
            self._on_ack(body, properties)
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosed):
            # These two exceptions are raised on "_basic_ack"
            # No need to process them below, so raise them to the caller 'run()' method
            # where re-connect is done
            raise
        except Exception as e:
            r = 'Exception on processing message: ' + str(e)

            try:
                self._basic_nack(ch, method.delivery_tag, requeue = self.deads_disabled)
            finally:
                self._on_nack(body, properties, result = r)

    def prepare(self):
        try:
            self.channel.basic_consume(
                    queue = self.queue, 
                    on_message_callback = self.__process_message, 
                    auto_ack = False)
        except:
            self.disconnect()
            raise

    def run(self):
        try:
            self.channel.start_consuming()
        finally:
            self.disconnect()

