import unittest
from .mocks.queue_client import QueueClient
import json
import argparse
from pika.exceptions import ChannelClosed

class QueueClientTest(unittest.TestCase):

    def setUp(self):
        pass

    def test_send(self):
        queue = QueueClient()
        queue.setup('amqp://127.0.0.1/', queue = 'my_queue', queue_declare = 'yes')
        queue.connect()
        self.assertEqual(queue.connection.params.host, '127.0.0.1')
        self.assertEqual(queue.channel.queue, 'my_queue')
        self.assertEqual(queue.channel.arguments['x-max-priority'], 3)
        self.assertEqual(queue.channel.durable, True)
        queue.send('hello, world!')
        self.assertEqual(len(queue.channel.messages),1)
        msg = queue.channel.messages.pop()
        self.assertEqual(msg['exchange'],'')
        self.assertEqual(msg['routing_key'],'my_queue')
        self.assertEqual(msg['body'],'hello, world!')
        self.assertEqual(msg['properties'].delivery_mode,2)
        queue.send(['one','two','three'])
        msg = queue.channel.messages.pop()
        self.assertEqual(msg['properties'].delivery_mode,2)
        self.assertEqual(msg['properties'].content_type,'application/json')
        data = json.loads(msg['body'])
        self.assertEqual(data, ['one', 'two', 'three'])

    def test_setup(self):
        queue = QueueClient()

        queue.setup('amqp://127.0.0.1/')
        self.assertEqual(queue.connection_parameters.host, '127.0.0.1')
        self.assertEqual(queue.queue_declare, 'no')

        queue.setup('amqp://a:b@127.0.0.1/')
        self.assertEqual(queue.connection_parameters.host, '127.0.0.1')
        self.assertEqual(queue.connection_parameters.credentials.username, 'a')
        self.assertEqual(queue.connection_parameters.credentials.password, 'b')
        self.assertEqual(queue.queue_declare, 'no')

        queue.setup('amqp://a:b@127.0.0.1/', username = 'c', password = 'd')
        self.assertEqual(queue.connection_parameters.host, '127.0.0.1')
        self.assertEqual(queue.connection_parameters.credentials.username, 'c')
        self.assertEqual(queue.connection_parameters.credentials.password, 'd')
        self.assertEqual(queue.queue_declare, 'no')

        queue.setup('amqp://a:b@127.0.0.1/', username = 'c', password = 'd', queue = 'myq', queue_declare = 'yes')
        self.assertEqual(queue.connection_parameters.host, '127.0.0.1')
        self.assertEqual(queue.connection_parameters.credentials.username, 'c')
        self.assertEqual(queue.connection_parameters.credentials.password, 'd')
        self.assertEqual(queue.queue_declare, 'yes')
        self.assertEqual(queue.queue, 'myq')

        queue.setup('amqp://a:b@127.0.0.1/', username = 'c', password = 'd', queue = 'myq', queue_declare = 'yes', 
                exchange = 'abc', routing_key = '123', priority=2)
        self.assertEqual(queue.connection_parameters.host, '127.0.0.1')
        self.assertEqual(queue.connection_parameters.credentials.username, 'c')
        self.assertEqual(queue.connection_parameters.credentials.password, 'd')
        self.assertEqual(queue.queue_declare, 'yes')
        self.assertEqual(queue.queue, 'myq')
        self.assertEqual(queue.exchange, 'abc')
        self.assertEqual(queue.routing_key, '123')
        self.assertEqual(queue.priority, 2)

    def test_with_argparse(self):
        queue = QueueClient()

        parser = argparse.ArgumentParser(description = 'This is a test')
        parser = queue.basic_args(parser)
        args = parser.parse_args('--amqp-url amqp://localhost:333/test'.split(' '))
        queue.setup_from_args(args)
        self.assertEqual(queue.connection_parameters.host, 'localhost')
        self.assertEqual(queue.connection_parameters.port, 333)

        args = parser.parse_args('--amqp-url amqp://127.0.0.1 --declare yes --amqp-username c --amqp-password d --queue myq --exchange abc --routing-key 123 --priority 2'.split(' '))
        queue.setup_from_args(args)
        self.assertEqual(queue.connection_parameters.host, '127.0.0.1')
        self.assertEqual(queue.connection_parameters.credentials.username, 'c')
        self.assertEqual(queue.connection_parameters.credentials.password, 'd')
        self.assertEqual(queue.queue_declare, 'yes')
        self.assertEqual(queue.queue, 'myq')
        self.assertEqual(queue.exchange, 'abc')
        self.assertEqual(queue.routing_key, '123')
        self.assertEqual(queue.priority, 2)

    def test_resend(self):
        queue = QueueClient()
        queue.setup('amqp://127.0.0.1/', queue = 'my_queue', queue_declare = 'yes')
        queue.connect()
        self.assertEqual(queue.connection.params.host, '127.0.0.1')
        self.assertEqual(queue.channel.queue, 'my_queue')
        self.assertEqual(queue.channel.arguments['x-max-priority'], 3)
        self.assertEqual(queue.channel.durable, True)
        queue.channel.raise_on_publish = ChannelClosed(504, 'whoops connection lost hahaha')
        queue.resend_on_fail = False
        with self.assertRaises(ChannelClosed):
            queue.send('hello, world!')
        self.assertEqual(len(queue.channel.messages),0)
        queue.channel.raise_on_publish = ChannelClosed(504, 'whoops connection lost hahaha')
        queue.resend_on_fail = True
        queue.send(['one','two','three'])
        msg = queue.channel.messages.pop()
        self.assertEqual(msg['properties'].delivery_mode,2)
        self.assertEqual(msg['properties'].content_type,'application/json')
        data = json.loads(msg['body'])
        self.assertEqual(data, ['one', 'two', 'three'])




