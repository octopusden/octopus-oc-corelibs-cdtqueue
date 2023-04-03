import unittest
from oc_cdt_queue2.queue_base import QueueBase
import argparse
import logging


class QueueBaseTest(unittest.TestCase):

    def setUp(self):
        self.queue = QueueBase()

    def test_init(self):
        self.assertIsNone(self.queue.connection_parameters)
        self.assertIsNone(self.queue.args)
        self.assertIsNone(self.queue.parser)
        self.assertEqual(self.queue.queue_declare, 'no')
        self.assertIsNone(self.queue.queue)

    def test_setup(self):
        self.queue.setup('amqp://127.0.0.1/')
        self.assertEqual(self.queue.connection_parameters.host, '127.0.0.1')

        self.queue.setup('amqp://a:b@127.0.0.1/')
        self.assertEqual(self.queue.connection_parameters.host, '127.0.0.1')
        self.assertEqual(self.queue.connection_parameters.credentials.username, 'a')
        self.assertEqual(self.queue.connection_parameters.credentials.password, 'b')

        self.queue.setup('amqp://a:b@127.0.0.1/', username='c', password='d')
        self.assertEqual(self.queue.connection_parameters.host, '127.0.0.1')
        self.assertEqual(self.queue.connection_parameters.credentials.username, 'c')
        self.assertEqual(self.queue.connection_parameters.credentials.password, 'd')

        self.queue.setup('amqp://a:b@127.0.0.1/', username = 'c', password = 'd', queue = 'myq', queue_declare = 'yes')
        self.assertEqual(self.queue.connection_parameters.host, '127.0.0.1')
        self.assertEqual(self.queue.connection_parameters.credentials.username, 'c')
        self.assertEqual(self.queue.connection_parameters.credentials.password, 'd')
        self.assertEqual(self.queue.queue_declare, 'yes')
        self.assertEqual(self.queue.queue, 'myq')

    def test_setup_from_args(self):
        self.queue = QueueBase()
        parser = argparse.ArgumentParser(description='test parser')
        self.queue.basic_args(parser)
        args = parser.parse_args('--amqp-url amqp://203.0.113.1 --reconnect-tries 10 --reconnect-delay 0'.split(' ')) # 203.0.113.0/24 is example network: RFC5737
        self.queue.setup_from_args(args)
        self.assertEqual(self.queue.reconnect_tries, 10)
        self.assertEqual(self.queue.reconnect_delay, 0)
        self.assertEqual(self.queue.connection_parameters.host, '203.0.113.1')
