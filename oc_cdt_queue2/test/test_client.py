import unittest
from oc_cdt_queue2.queue_client import QueueClient
import argparse
import pika
import time
import logging
import json

try:
    import queue
except ImportError:
    import Queue as queue

class _ChannelMock(object):
    def __init__(self, *args, **kvargs):
        self.msg_buffer = queue.Queue()
        self.exceptions = list()
        self.exchanges = dict()
        self.queues = dict()

    def basic_publish(self, exchange, routing_key, body, properties):
        if len(self.exceptions):
            raise self.exceptions.pop()

        self.msg_buffer.put(
            {'exchange': exchange,
             'routing_key': routing_key,
             'body': body,
             'properties': properties})

    def exchange_declare(self, exchange, exchange_type, durable):
        if exchange not in self.exchanges.keys():
            self.exchanges.update({exchange: {'type': exchange_type, 'durable': durable}})
            return

        if self.exchanges.get(exchange).get('type') != exchange_type:
            raise pika.exceptions.ChannelClosedByBroker(406, "Exchange type differ for %s" % exchange)

        if self.exchanges.get(exchange).get('durable') != durable:
            raise pika.exceptions.ChannelClosedByBroker(406, "Durable differ for %s" % exchange)

    def queue_declare(self, queue, durable, arguments):
        if queue not in self.queues.keys():
            self.queues.update({queue: {'durable': durable, 'args': arguments}})
            return

        if self.queues.get(queue).get('durable') != durable:
            raise pika.exceptions.ChannelClosedByBroker(406, "Durable differ for queue %s" % queue)

        if self.queues.get(queue).get('args') != arguments:
            raise pika.exceptions.ChannelClosedByBroker(406, "Queue arguments differ for queue %s" % queue)

    def queue_bind(self, queue, exchange, routing_key):
        if routing_key != exchange:
            raise ValueError("Exchange and routing key should be the same")

        if exchange not in self.exchanges.keys():
            raise pika.exceptions.ChannelClosedByBroker(406, "No such exchange: %s" % exchange)

        if queue not in self.queues.keys():
            raise pika.exceptions.ChannelClosedByBroker(406, "No such queue: %s" % queue)

        self.exchanges[exchange].update({'queue': queue})


class _ConnectionMock(object):
    def __init__(self, params):
        self.channels = list()
        self.exceptions = list()
        self.is_open = True
        self.params = params

    def __check_raise_exception(self):
        if len(self.exceptions):
            raise self.exceptions.pop()

    def channel(self):
        self.__check_raise_exception()
        _chan = _ChannelMock()
        self.channels.append(_chan)
        return _chan

    def close(self):
        self.__check_raise_exception()
        self.is_open = False


class QueueClientTest(unittest.TestCase):
    def setUp(self):
        self.client = QueueClient()
        self.client._Connection = _ConnectionMock

    def test_setup_from_args(self):
        parser = argparse.ArgumentParser(description='test parser')
        self.client.basic_args(parser)
        args = parser.parse_args('--amqp-url amqp://203.0.113.1 --reconnect-tries 10 --reconnect-delay 0 --routing-key cdt.example.queue --exchange cdt.example.exchange --priority 3'.split(' ')) # 203.0.113.0/24 is example network: RFC5737
        self.client.setup_from_args(args)
        self.assertEqual(self.client.reconnect_tries, 10)
        self.assertEqual(self.client.reconnect_delay, 0)
        self.assertEqual(self.client.connection_parameters.host, '203.0.113.1')
        self.assertEqual(self.client.routing_key, "cdt.example.queue")
        self.assertEqual(self.client.exchange, "cdt.example.exchange")
        self.assertEqual(self.client.priority, 3)

    def test_setup(self):
        self.client.setup(routing_key="cdt.routing.default",
                          exchange="cdt.exchange.default", priority=2)
        self.assertEqual(self.client.routing_key, "cdt.routing.default")
        self.assertEqual(self.client.exchange, "cdt.exchange.default")
        self.assertEqual(self.client.priority, 2)

    def test_basic_publish(self):
        ### assume connection and channel has been done
        ### so check channel basic publish
        _chan = _ChannelMock()
        self.client.channel = _chan
        self.client.exchange = 'test_exchange'
        self.client.routing_key = 'test_routing_key'
        self.client.priority = 3
        _msg_body = json.dumps({'test_method_1': ['arg_1', 'arg_2']})
        _msg_content_type = 'application/json'
        _msg_headers = {'header_1': "value_1", 'header_2': 2}
        _msg_content_encoding = 'ascii'
        self.assertTrue(self.client.channel.msg_buffer.empty())
        self.client._basic_publish(_msg_body, _msg_content_type, _msg_headers, _msg_content_encoding)
        self.assertFalse(self.client.channel.msg_buffer.empty())
        _msg = self.client.channel.msg_buffer.get()
        self.assertTrue(self.client.channel.msg_buffer.empty())
        self.assertEqual(_msg.get('exchange'), 'test_exchange')
        self.assertEqual(_msg.get('routing_key'), 'test_routing_key')
        self.assertEqual(_msg.get('body'), _msg_body)
        _props = _msg.get('properties')
        self.assertIsInstance(_props, pika.BasicProperties)
        self.assertEqual(_props.delivery_mode, 2) #persistant
        self.assertEqual(_props.priority, 3)
        self.assertEqual(_props.content_type, _msg_content_type)
        self.assertEqual(_props.headers, _msg_headers)
        self.assertEqual(_props.content_encoding, _msg_content_encoding)

    def test_send_all_ok(self):
        _chan = _ChannelMock()
        self.client.channel = _chan
        self.client.exchange = 'test_exchange'
        self.client.routing_key = 'test_routing_key'
        self.client.priority = 1
        _msg_body = {'test_method_1': ['arg_1', 'arg_2']}
        _msg_body_str = json.dumps(_msg_body)
        self.assertTrue(self.client.channel.msg_buffer.empty())
        self.client.send(_msg_body)
        self.assertFalse(self.client.channel.msg_buffer.empty())
        _msg = self.client.channel.msg_buffer.get()
        self.assertTrue(self.client.channel.msg_buffer.empty())
        self.assertEqual(_msg.get('exchange'), 'test_exchange')
        self.assertEqual(_msg.get('routing_key'), 'test_routing_key')
        self.assertEqual(_msg.get('body'), _msg_body_str) # NOTE: body should be encoded to JSON string
        _props = _msg.get('properties')
        self.assertIsInstance(_props, pika.BasicProperties)
        self.assertEqual(_props.delivery_mode, 2) #persistant
        self.assertEqual(_props.priority, 1)
        self.assertEqual(_props.content_type, 'application/json')
        self.assertEqual(_props.headers, {})
        self.assertIsNone(_props.content_encoding)

    def test_send_content_type_differ(self):
        # the same as above but we specify content-type exactly
        _chan = _ChannelMock()
        self.client.channel = _chan
        self.client.exchange = 'test_exchange'
        self.client.routing_key = 'test_routing_key'
        self.client.priority = 1
        _msg_body = json.dumps({'test_method_1': ['arg_1', 'arg_2']})
        _msg_content_type = 'text/plain'
        _msg_headers = {'header_1': "value_1", 'header_2': 2}
        _msg_content_encoding = 'ascii'
        self.assertTrue(self.client.channel.msg_buffer.empty())
        self.client.send(_msg_body, content_type=_msg_content_type,
                         headers=_msg_headers, content_encoding=_msg_content_encoding)
        self.assertFalse(self.client.channel.msg_buffer.empty())
        _msg = self.client.channel.msg_buffer.get()
        self.assertTrue(self.client.channel.msg_buffer.empty())
        self.assertEqual(_msg.get('exchange'), 'test_exchange')
        self.assertEqual(_msg.get('routing_key'), 'test_routing_key')
        self.assertEqual(_msg.get('body'), _msg_body)
        _props = _msg.get('properties')
        self.assertIsInstance(_props, pika.BasicProperties)
        self.assertEqual(_props.delivery_mode, 2) #persistant
        self.assertEqual(_props.priority, 1)
        self.assertEqual(_props.content_type, _msg_content_type)
        self.assertEqual(_props.headers, _msg_headers)
        self.assertEqual(_props.content_encoding, _msg_content_encoding)

    def test_send_publish_fail_no_resend(self):
        # normal message but exception should appear
        _chan = _ChannelMock()
        self.client.channel = _chan
        self.client.exchange = 'test_exchange'
        self.client.routing_key = 'test_routing_key'
        self.client.priority = 3
        _msg_body = {'test_method_1': ['arg_1', 'arg_2']}
        self.assertTrue(self.client.channel.msg_buffer.empty())
        # emulate an error
        _chan.exceptions = [pika.exceptions.ChannelClosed(406, 'Test exception')]
        self.client.resend_on_fail = False
        with self.assertRaises(pika.exceptions.ChannelClosed):
            self.client.send(_msg_body)

        self.assertTrue(self.client.channel.msg_buffer.empty())

    def test_send_publish_fail_resend(self):
        # the same as above, but reconnect should be done and message have to be send
        # we have to mock 'connect' method and catch it is called
        class _QueueClientMock(QueueClient):
            def __init__(self, *args, **kwargs):
                self.connect_called = False
                super(_QueueClientMock, self).__init__(*args, **kwargs)

            def connect(self):
                self.connect_called = True

        self.client = _QueueClientMock()
        _chan = _ChannelMock()
        self.client.channel = _chan
        self.client.exchange = 'test_exchange'
        self.client.routing_key = 'test_routing_key'
        self.client.priority = 3
        _msg_body = {'test_method_1': ['arg_1', 'arg_2']}
        _msg_content_type = 'application/json'
        self.assertTrue(self.client.channel.msg_buffer.empty())
        # emulate an error
        _chan.exceptions = [pika.exceptions.ChannelClosed(406, 'Test exception')]
        self.client.resend_on_fail = True
        self.client.send(_msg_body)
        self.assertTrue(self.client.connect_called)
        self.assertFalse(self.client.channel.msg_buffer.empty())
        _msg = self.client.channel.msg_buffer.get()
        self.assertTrue(self.client.channel.msg_buffer.empty())
        self.assertEqual(_msg.get('exchange'), 'test_exchange')
        self.assertEqual(_msg.get('routing_key'), 'test_routing_key')
        self.assertEqual(_msg.get('body'), json.dumps(_msg_body))
        _props = _msg.get('properties')
        self.assertIsInstance(_props, pika.BasicProperties)
        self.assertEqual(_props.delivery_mode, 2)
        self.assertEqual(_props.priority, 3)
        self.assertEqual(_props.content_type, _msg_content_type)
        self.assertEqual(_props.headers, {})
        self.assertIsNone(_props.content_encoding)

    def test_disconnect_connection_ok(self):
        self.client.connection = _ConnectionMock(None)
        self.assertIsNotNone(self.client.connection)
        self.assertTrue(self.client.connection.is_open)
        self.client.disconnect()
        self.assertFalse(self.client.connection.is_open)

    def test_disconnect_no_connection(self):
        # this does nothing
        self.assertIsNone(self.client.connection)
        self.client.disconnect()
        self.assertIsNone(self.client.connection)

    def test_disconnect_connection_closed(self):
        self.client.connection = _ConnectionMock(None)
        self.assertIsNotNone(self.client.connection)
        self.client.connection.is_open = False
        self.assertFalse(self.client.connection.is_open)
        self.client.disconnect()
        self.assertFalse(self.client.connection.is_open)

    def test_disconnect_attribute_error(self):
        # this does nothing too
        self.client.connection = _ConnectionMock(None)
        self.client.connection.exceptions = [AttributeError("Test error")]
        self.assertIsNotNone(self.client.connection)
        self.assertTrue(self.client.connection.is_open)
        self.client.disconnect()
        self.assertTrue(self.client.connection.is_open)

    def test_disconnect_exception(self):
        # should raise
        self.client.connection = _ConnectionMock(None)
        self.client.connection.exceptions = [pika.exceptions.ConnectionClosed(403, "Test error")]
        self.assertIsNotNone(self.client.connection)
        self.assertTrue(self.client.connection.is_open)
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            self.client.disconnect()
        self.assertTrue(self.client.connection.is_open)

    def test_connect_success(self):
        _queue = 'my_queue'
        _url = 'amqp://127.0.0.1/'
        self.client.setup(_url, queue=_queue, queue_declare='yes')
        self.client.connect()
        self.assertEqual(self.client.connection.params.host, '127.0.0.1')
        _all_queues = self.client.channel.queues
        self.assertIn(_queue, _all_queues)
        self.assertEqual(_all_queues.get(_queue).get('args').get('x-max-priority'), 3)
        self.assertTrue(_all_queues.get(_queue).get('durable'))

    def test_connect_queue_exist(self):
        # should be OK
        _queue = 'my_queue'
        _url = 'amqp://127.0.0.1/'

        # emualte queue exists
        class _ConnPreQMock(_ConnectionMock):
            def channel(self):
                _chan = _ChannelMock()
                _chan.queue_declare('my_queue', durable=True, arguments={'x-max-priority': 3})
                self.channels.append(_chan)
                return _chan

        self.client._Connection = _ConnPreQMock
        _prs = argparse.ArgumentParser(description="test parser")
        _prs = self.client.basic_args(_prs)
        _args = _prs.parse_args(('--amqp-url %s --declare yes --queue %s' % (_url, _queue)).split(' '))
        self.client.setup_from_args(_args)
        self.client.deads_disabled = True
        self.client.connect()
        self.assertIn(_queue, self.client.channel.queues.keys())
        _q = self.client.channel.queues.get(_queue)
        self.assertTrue(_q.get('durable'))
        self.assertEqual(_q.get('args').get('x-max-priority'), 3)

    def test_connect_queue_durable_differ(self):
        # should raise an exception
        _queue = 'my_queue'
        _url = 'amqp://127.0.0.1/'

        # emualte queue exists
        class _ConnPreQMock(_ConnectionMock):
            def channel(self):
                _chan = _ChannelMock()
                # NOTE: we replace 'durable' here to 'False'
                _chan.queue_declare('my_queue', durable=False, arguments={'x-max-priority': 3})
                self.channels.append(_chan)
                return _chan

        self.client._Connection = _ConnPreQMock
        _prs = argparse.ArgumentParser(description="test parser")
        _prs = self.client.basic_args(_prs)
        _args = _prs.parse_args(('--amqp-url %s --declare yes --queue %s' % (_url, _queue)).split(' '))
        self.client.setup_from_args(_args)
        self.client.deads_disabled = True
        with self.assertRaises(pika.exceptions.ChannelClosedByBroker):
            self.client.connect()

    def test_connect_queue_arguments_differ(self):
        _queue = 'my_queue'
        _url = 'amqp://127.0.0.1/'

        # emualte queue exists
        class _ConnPreQMock(_ConnectionMock):
            def channel(self):
                _chan = _ChannelMock()
                # NOTE: we replace priority here to 1
                _chan.queue_declare('my_queue', durable=True, arguments={'x-max-priority': 1})
                self.channels.append(_chan)
                return _chan

        self.client._Connection = _ConnPreQMock
        _prs = argparse.ArgumentParser(description="test parser")
        _prs = self.client.basic_args(_prs)
        _args = _prs.parse_args(('--amqp-url %s --declare yes --queue %s' % (_url, _queue)).split(' '))
        self.client.setup_from_args(_args)
        self.client.deads_disabled = True
        with self.assertRaises(pika.exceptions.ChannelClosedByBroker):
            self.client.connect()

    def test_connect_queue_wrong(self):
        # should declare second queue end return without exceptions should be OK
        _queue = 'my_queue'
        _url = 'amqp://127.0.0.1/'

        # emualte queue exists
        class _ConnPreQMock(_ConnectionMock):
            def channel(self):
                _chan = _ChannelMock()
                _chan.queue_declare('another_queue', durable=True, arguments={'x-max-priority': 3})
                self.channels.append(_chan)
                return _chan

        self.client._Connection = _ConnPreQMock
        _prs = argparse.ArgumentParser(description="test parser")
        _prs = self.client.basic_args(_prs)
        _args = _prs.parse_args(('--amqp-url %s --declare yes --queue %s' % (_url, _queue)).split(' '))
        self.client.setup_from_args(_args)
        self.client.deads_disabled = True
        self.client.connect()
        self.assertIn(_queue, self.client.channel.queues.keys())
        self.assertIn('another_queue', self.client.channel.queues.keys())
        _q = self.client.channel.queues.get(_queue)
        self.assertTrue(_q.get('durable'))
        self.assertEqual(_q.get('args').get('x-max-priority'), 3)
        _q = self.client.channel.queues.get('another_queue')
        self.assertTrue(_q.get('durable'))
        self.assertEqual(_q.get('args').get('x-max-priority'), 3)

    def test_reconnect_fail(self):
        # emulate an exception while connecting
        # we are not interesting in successful connection here since it is preciesly tested above
        class _ConnectionMockExc(_ConnectionMock):
            def __init__(self, params):
                super(_ConnectionMockExc, self).__init__(params)
                self.exceptions = [pika.exceptions.ConnectionClosed(406, "URL not found")]

        self.client._Connection = _ConnectionMockExc
        parser = argparse.ArgumentParser(description='test parser')
        self.client.basic_args(parser)
        args = parser.parse_args('--amqp-url amqp://203.0.113.1 --reconnect-tries 2 --reconnect-delay 1'.split(' ')) # 203.0.113.0/24 is example network: RFC5737
        self.client.setup_from_args(args)

        t1 = time.time()

        with self.assertRaises(pika.exceptions.ConnectionClosed):
            self.client.connect()

        t2 = time.time()
        self.assertTrue(t2-t1 > 0.99)
        self.assertTrue(t2-t1 < 1.5)

    def test_declare_deads(self):
        _queue = 'my_queue.input'
        _deads = 'my_queue.deads'
        _url = 'amqp://127.0.0.1/'
        self.client.setup(_url, queue=_queue, queue_declare='yes')
        self.client.connect()
        self.assertEqual(self.client.connection.params.host, '127.0.0.1')
        _all_queues = self.client.channel.queues
        # check deads queue and its exchange have been created
        self.assertIn(_queue, _all_queues)
        self.assertIn(_deads, _all_queues)
        self.assertEqual(_all_queues.get(_queue).get('args').get('x-max-priority'), 3)
        self.assertEqual(_all_queues.get(_queue).get('args').get('x-dead-letter-exchange'), _deads)
        self.assertEqual(_all_queues.get(_queue).get('args').get('x-dead-letter-routing-key'), _deads)
        self.assertTrue(_all_queues.get(_queue).get('durable'))
        self.assertTrue(_all_queues.get(_deads).get('durable'))
        _all_exchanges = self.client.channel.exchanges
        self.assertIn(_deads, _all_exchanges)
        self.assertEqual(_all_exchanges.get(_deads).get('queue'), _deads)
