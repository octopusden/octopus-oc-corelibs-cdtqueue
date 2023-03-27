import unittest
from .mocks.queue_server import QueueServer
from .mocks.queue_client import QueueClient
import json

# to get rid of logging output from imported classes
import logging
logging.getLogger().propagate = False
logging.getLogger().disabled = True

class TestClass(QueueServer):
    def __init__(self):
        super(TestClass,self).__init__()
        self.messages = []
        self.mode = 1

    def on_message(self, json, properties):
        self.messages += [json]
        if self.mode != 1: raise(ValueError(self.mode))


class QueueServerTest(unittest.TestCase):

    def setUp(self):
        self.server = TestClass()
        self.client = QueueClient()

        self.client.setup('amqp://127.0.0.1')
        self.server.setup('amqp://127.0.0.1')

        self.client.connect()
        self.server.connect()

        self.client.channel.connect_to = self.server.connection

    def test_settings(self):

        self.assertEqual(self.server.queue, 'rpc')
        self.assertEqual(self.client.queue, 'rpc')
        self.assertEqual(self.client.routing_key, 'rpc')
        self.assertEqual(self.client.exchange, '')

        self.assertEqual(self.server.channel.srv_queue, None)

    def test_receive(self):
        server = self.server
        client = self.client

        self.assertEqual(server.channel.srv_queue, None)
        server.prepare()
        self.assertEqual(server.channel.srv_queue, 'rpc')

        client.send([1,2,3])
        self.assertEqual(len(client.channel.messages),1)
        msg = client.channel.messages[0]
        self.assertEqual(json.loads(msg['body']),[1, 2, 3])
        self.assertEqual(len(server.channel.srv_messages),1)

        server.run()

        self.assertEqual(len(server.messages),1)
        self.assertEqual(len(server.channel.srv_messages),0)
        msg = server.messages[0]
        self.assertEqual(msg,[1, 2, 3])

    def test_nack(self):
        self.server.deads_disabled = True
        self.server.mode = 'nack testing'
        self.server.max_sleep = 0
        self.assertEqual(self.server.channel.srv_queue, None)
        self.server.prepare()
        self.assertEqual(self.server.channel.srv_queue, 'rpc')

        self.client.send([111,0,0])
 
        self.assertEqual(len(self.server.messages),0)
        self.assertEqual(len(self.server.channel.srv_messages),1)
        self.assertEqual(self.server.counter_bad,0)
        self.assertEqual(self.server.counter_good,0)

        self.server.run()

        self.assertEqual(self.server.counter_bad,1)
        self.assertEqual(self.server.counter_good,0)
        self.assertEqual(len(self.server.messages),1)
        self.assertEqual(self.server.messages[0], [111,0,0])
        self.assertEqual(len(self.server.channel.srv_messages),1)

        self.server.run()
        self.assertEqual(len(self.server.messages),2)
        self.assertEqual(len(self.server.channel.srv_messages),1)
        self.server.mode = 1
        self.server.run()
        self.assertEqual(len(self.server.messages),3)
        self.assertEqual(len(self.server.channel.srv_messages),0)


    def test_nack_deads_enabled(self):
        self.server.deads_disabled = False 
        self.server.mode = 'nack testing'
        self.server.max_sleep = 0
        self.assertEqual(self.server.channel.srv_queue, None)
        self.server.prepare()
        self.assertEqual(self.server.channel.srv_queue, 'rpc')
        self.client.send([90,0,0])
        self.assertEqual(len(self.server.messages),0)
        self.assertEqual(len(self.server.channel.srv_messages),1)
        
        self.server.run()

        self.assertEqual(self.server.counter_bad,1)
        self.assertEqual(self.server.counter_good,0)
        self.assertEqual(len(self.server.messages),1)
        self.assertEqual(len(self.server.channel.srv_messages),0)

        self.server.run()
        
        self.assertEqual(len(self.server.messages),1)
        self.assertEqual(len(self.server.channel.srv_messages),0)
        self.assertEqual(self.server.counter_bad,1)
        self.assertEqual(self.server.counter_good,0)
        
        self.server.mode = 1
        self.client.send([990,0,0])
        self.server.run()
        self.assertEqual(len(self.server.messages),2)
        self.assertEqual(len(self.server.channel.srv_messages),0)
        self.assertEqual(self.server.counter_bad,1)
        self.assertEqual(self.server.counter_good,1)


