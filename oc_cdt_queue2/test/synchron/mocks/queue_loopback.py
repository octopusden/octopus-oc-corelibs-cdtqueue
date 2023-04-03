
import pika
import logging

# messages sent gets stored here by routing key
global_message_queue = {}
# last message id by routing key
global_message_id = {}
# disable/enable variables above
global_messaging = True


class _LoopbackMethod(object):

    def __init__(self, i):
        self.delivery_tag = i


class LoopbackChannel(object):
    raise_on_publish = None   # set this to exception you want to raise on basic_publish()

    def __init__(self, parent):
        self.parent = parent
        self.messages = []

        self.connect_to = None
        self.connect_to_run = False

        # mostly for server-side testing
        self.srv_messages = {}
        self.srv_counter = 0
        self.srv_queue = None

    def basic_qos(self, prefetch_count=None):
        self.prefetch_count = prefetch_count

    def queue_declare(self, queue, durable=None, arguments={}):
        self.queue = queue
        self.durable = durable
        self.arguments = arguments
        if global_messaging and queue not in global_message_queue:
            global_message_queue[queue] = {}
            global_message_id[queue] = 0

    def exchange_declare(self, exchange, exchange_type, durable):
        self.exchange = exchange

    def queue_bind(self, queue, exchange, routing_key):
        if (exchange == self.exchange) and (queue == exchange == routing_key):
            pass
        else:
            raise

    def basic_publish(self, exchange, routing_key, body, properties):
        if self.raise_on_publish is not None:
            raise(self.raise_on_publish)
        self.messages += [{'exchange': exchange, 'routing_key': routing_key,
                           'body': body, 'properties': properties}]
        if global_messaging and exchange == '' and routing_key in global_message_queue:
            global_message_id[routing_key] += 1
            global_message_queue[routing_key][global_message_id[routing_key]] = {
                'properties': properties, 'body': body}

        if not self.connect_to or len(self.connect_to.channels) == 0 or exchange != '':
            return

        for dst in self.connect_to.channels:
            if dst.srv_queue == routing_key and dst.parent.params.host == self.parent.params.host:
                dst.srv_counter += 1
                dst.srv_messages[dst.srv_counter] = {
                    'properties': properties, 'body': body}
                if self.connect_to_run:
                    dst.start_consuming()
                break

    def basic_consume(self, queue, on_message_callback, auto_ack=None):
        self.srv_auto_ack = auto_ack
        self.srv_queue = queue
        self.srv_callback = on_message_callback

    def basic_ack(self, delivery_tag):
        del self.srv_messages[delivery_tag]

    def basic_nack(self, delivery_tag, requeue=None):
        if not requeue:
            del self.srv_messages[delivery_tag]

    def start_consuming(self):
        if global_messaging and self.srv_queue in global_message_queue:
            self.srv_messages = global_message_queue[self.srv_queue]
        for msg_key in list(self.srv_messages.keys()):
            msg = self.srv_messages[msg_key]
            self.srv_callback(self, _LoopbackMethod(msg_key),
                              msg['properties'], msg['body'])


class LoopbackConnection(object):
    """
    Mock connection, to be set to _BlockingConnection in QueueBase instead of pika.BlockingConnection
    """

    def __init__(self, params):
        if params.host not in ['127.0.0.1', 'localhost']:
            raise pika.exceptions.ConnectionClosed(402, 'Not A Loopback Address')
        self.params = params
        self.is_open = True
        self.connect_to = None
        self.channels = []

    def channel(self):
        """
        Mock for a call that returns communicaton channel.

        :returns: LoopbackCnannel instance which emulates pike.CommunicationChannel
        """
        channel = LoopbackChannel(self)
        channel.connect_to = self.connect_to
        self.channels += [channel]
        return channel

    def close(self):
        self.is_open = False
