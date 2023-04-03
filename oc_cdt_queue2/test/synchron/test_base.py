import unittest
from .mocks.queue_base import QueueBase
from .mocks.queue_loopback import LoopbackConnection
import argparse
import pika
import time 
import logging

#logging.basicConfig(level=logging.DEBUG)
logging.getLogger().propagate = False
logging.getLogger().disable = True

class ConnectOrMaybe(LoopbackConnection):
    exceptions_to_raise = [] 
    calls = 0

    def __init__(self, *args, **kvargs):
        ConnectOrMaybe.calls += 1
        if len(ConnectOrMaybe.exceptions_to_raise) > 0: raise(ConnectOrMaybe.exceptions_to_raise.pop())
        super(ConnectOrMaybe, self).__init__(*args, **kvargs)

class QueueBaseTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_init(self):
        queue = QueueBase()

        self.assertIsNone(queue.connection)
        self.assertIsNone(queue.connection_parameters)
        self.assertIsNone(queue.args)
        self.assertIsNone(queue.parser)
        self.assertEqual(queue.queue_declare, False )
        self.assertIsNone(queue.queue )
#       self.assertIsNone(queue.routing_key )
        self.assertIsNone(queue.channel )

    def test_setup(self):
        queue = QueueBase()

        queue.setup('amqp://127.0.0.1/')
        self.assertEqual(queue.connection_parameters.host, '127.0.0.1')

        queue.setup('amqp://a:b@127.0.0.1/')
        self.assertEqual(queue.connection_parameters.host, '127.0.0.1')
        self.assertEqual(queue.connection_parameters.credentials.username, 'a')
        self.assertEqual(queue.connection_parameters.credentials.password, 'b')

        queue.setup('amqp://a:b@127.0.0.1/', username = 'c', password = 'd')
        self.assertEqual(queue.connection_parameters.host, '127.0.0.1')
        self.assertEqual(queue.connection_parameters.credentials.username, 'c')
        self.assertEqual(queue.connection_parameters.credentials.password, 'd')

        queue.setup('amqp://a:b@127.0.0.1/', username = 'c', password = 'd', queue = 'myq', queue_declare = True)
        self.assertEqual(queue.connection_parameters.host, '127.0.0.1')
        self.assertEqual(queue.connection_parameters.credentials.username, 'c')
        self.assertEqual(queue.connection_parameters.credentials.password, 'd')
        self.assertEqual(queue.queue_declare, True)
        self.assertEqual(queue.queue, 'myq')

    def test_connect(self):
        queue = QueueBase()
        queue.setup('amqp://127.0.0.1/', queue = 'my_queue', queue_declare = True)
        queue.connect()
        self.assertEqual(queue.connection.params.host, '127.0.0.1')
        self.assertEqual(queue.channel.prefetch_count, 1)
        self.assertEqual(queue.channel.queue, 'my_queue')
        self.assertEqual(queue.channel.arguments['x-max-priority'], 3)
        self.assertEqual(queue.channel.durable, True)

    def test_setup_from_args(self):
        queue = QueueBase()
        parser = argparse.ArgumentParser(description='test parser')
        queue.basic_args(parser)
        args = parser.parse_args('--amqp-url amqp://203.0.113.1 --reconnect-tries 10 --reconnect-delay 0 --prefetch-count 11'.split(' ')) # 203.0.113.0/24 is example network: RFC5737
        queue.setup_from_args(args)
        self.assertEqual(queue.reconnect_tries,10)
        self.assertEqual(queue.reconnect_delay,0)
        self.assertEqual(queue.connection_parameters.host,'203.0.113.1')
        self.assertEqual(queue.prefetch_count,11)

    def test_default_prefetch_count(self):
        queue = QueueBase()
        self.assertEqual(queue.prefetch_count, 1)

    def test_reconnect_fail(self):
        queue = QueueBase()
        queue._Connection = ConnectOrMaybe
        parser = argparse.ArgumentParser(description='test parser')
        queue.basic_args(parser)
        args = parser.parse_args('--amqp-url amqp://203.0.113.1 --reconnect-tries 2 --reconnect-delay 1'.split(' ')) # 203.0.113.0/24 is example network: RFC5737
        queue.setup_from_args(args)
        t1 = time.time()

        ConnectOrMaybe.exceptions_to_raise = []
        ConnectOrMaybe.calls = 0
        with self.assertRaises(pika.exceptions.ConnectionClosed): 
            queue.connect()
        t2 = time.time()
        self.assertTrue(t2-t1 > 0.99)
        self.assertTrue(t2-t1 < 1.5)
        self.assertEqual(ConnectOrMaybe.calls, 3)

        args = parser.parse_args('--amqp-url amqp://203.0.113.1 --reconnect-tries 1 --reconnect-delay 1'.split(' ')) # 203.0.113.0/24 is example network: RFC5737
        queue.setup_from_args(args)
        t1 = time.time()
        ConnectOrMaybe.calls = 0
        with self.assertRaises(pika.exceptions.ConnectionClosed): 
            queue.connect()
        t2 = time.time()
        self.assertTrue(t2-t1 < 0.5)
        self.assertEqual(ConnectOrMaybe.calls, 2)

    def test_reconnect_success(self):
        queue = QueueBase()
        queue._Connection = ConnectOrMaybe
        parser = argparse.ArgumentParser(description='test parser')
        queue.basic_args(parser)

        ConnectOrMaybe.calls = 0
        ConnectOrMaybe.exceptions_to_raise = [pika.exceptions.ConnectionClosed(402, 'oops'), pika.exceptions.AMQPConnectionError('whops'), pika.exceptions.ChannelClosed(403, 'boom')]
        args = parser.parse_args('--amqp-url amqp://127.0.0.1 --reconnect-tries 5 --reconnect-delay 0'.split(' ')) 
        queue.setup_from_args(args)

        t1 = time.time()
        queue.connect()
        t2 = time.time()
        self.assertTrue(t2-t1 < 0.5)



