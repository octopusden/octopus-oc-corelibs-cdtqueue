import unittest
from .mocks.queue_handler import QueueHandler
from .mocks.queue_rpc import QueueRPC
import json

# to get rid of logging messages from imported classes
import logging
logging.getLogger().propagate = False
logging.getLogger().disabled = True

class MyClient(QueueRPC):
    published = ['methodA', 'methodB', 'ping', 'violate']

class TestClass(QueueHandler):
    
    published = ['methodA', 'methodB', 'ping']

    def __init__(self):
        super(TestClass,self).__init__()
        self.args = None
        self.kvargs = None   # MethodB stores all of its args here
        self.method = ''     # MethodA and MethodB sets this to its name so we can check it works correctly
        self.messages = []   # for parameter parsing tests. ping() stores reveiced parameters here
        self.mode = 1        # set self.mode to string value to raise an exception on method call
        self.itworks = None  # for testing access violation

    def methodA(self):
        self.method = 'MethodA'
        if self.mode != 1: raise(ValueError(self.mode))

    def methodB(self, *args, **kvargs):
        self.args = args
        self.kvargs = kvargs
        self.method = 'MethodB'
        if self.mode != 1: raise(ValueError(self.mode))

    def ping(self, msg1 = None, msg2 = None, test = None):
        self.messages += [ ['ping', msg1, msg2, test] ]
        if self.mode != 1: raise(ValueError(self.mode))

    def violate(self):
        """
        This method can't be called from rpc because it is not defined in "published" property
        """
        self.itworks = True


class QueueHandlerTest(unittest.TestCase):

    def setUp(self):
#       logging.basicConfig(level = logging.DEBUG)

        self.server = TestClass()
        self.client = MyClient()

        self.client.setup('amqp://127.0.0.1')
        self.server.setup('amqp://127.0.0.1')

        self.client.connect()
        self.server.connect()

        self.client.channel.connect_to = self.server.connection
        self.server.max_sleep = 0

    def test_settings(self):

        self.assertEqual(self.server.queue, 'rpc')
        self.assertEqual(self.client.queue, 'rpc')
        self.assertEqual(self.client.routing_key, 'rpc')
        self.assertEqual(self.client.exchange, '')

        self.assertEqual(self.server.channel.srv_queue, None)

        self.assertEqual(self.server.counter_bad,0)
        self.assertEqual(self.server.counter_good,0)

    def test_receive(self):
        server = self.server
        client = self.client

        self.assertEqual(server.channel.srv_queue, None)
        server.prepare()
        self.assertEqual(server.channel.srv_queue, 'rpc')

        client.ping('test', 'more test', test='test')
        self.assertEqual(len(client.channel.messages),1)
        msg = client.channel.messages[0]
        self.assertEqual(json.loads(msg['body']),[u'ping', [u'test', u'more test'], {u'test':u'test'}])
        self.assertEqual(len(server.channel.srv_messages),1)

        server.run()

        self.assertEqual(len(server.messages),1)
        self.assertEqual(len(server.channel.srv_messages),0)
        msg = server.messages[0]
        self.assertEqual(msg,['ping', u'test', u'more test', u'test'])
        self.assertEqual(self.server.counter_bad,0)
        self.assertEqual(self.server.counter_good,1)

    def test_call_parameter(self):

        self.server.prepare()
        self.assertEqual(self.server.counter_bad,0)

        self.client.ping(msg2 = 'hello')
        self.server.run()
        msg = self.server.messages.pop()
        self.assertEqual(msg,['ping', None, u'hello', None])

        self.client.ping(msg1 = 'hello')
        self.server.run()
        msg = self.server.messages.pop()
        self.assertEqual(msg,['ping', u'hello', None, None])

        self.client.ping(test = 'hello')
        self.server.run()
        msg = self.server.messages.pop()
        self.assertEqual(msg,['ping', None, None, u'hello'])

        self.client.ping('te',msg1 = 'test') # this should fail
        self.server.run()
        self.assertEqual(len(self.server.messages),0)
        self.assertEqual(self.server.counter_bad,1)
        self.assertEqual(self.server.counter_good,3)

    def test_access_violation(self):
        self.server.prepare()
        self.server.deads_disabled = True
        self.client.violate()
        self.server.run()
        self.assertEqual(self.server.itworks, None)
        self.assertEqual(self.server.counter_bad,1)
        self.assertEqual(len(self.server.channel.srv_messages),1)

    def test_access_violation_deads_enabled(self):
        self.server.prepare()
        self.client.violate()
        self.server.run()
        self.assertEqual(self.server.itworks, None)
        self.assertEqual(self.server.counter_bad,1)
        self.assertEqual(len(self.server.channel.srv_messages),0)  
        # when dead queues enabled, message gets deleted from original queue and moved to deads queue


    def test_different_methods(self):
        self.server.prepare()
        self.client.methodA()
        self.server.run()
        self.assertEqual(self.server.method, 'MethodA')
        self.client.methodB()
        self.server.run()
        self.assertEqual(self.server.method, 'MethodB')
        self.assertEqual(self.server.counter_bad,0)

    def test_nack(self):
        self.server.deads_disabled = True # leave message in original queue when something goes wrong
        self.server.mode = 'nack testing' # now every ping message should cause an exception
        self.server.max_sleep = 0
        self.assertEqual(self.server.channel.srv_queue, None)
        self.server.prepare()
        self.assertEqual(self.server.channel.srv_queue, 'rpc')

        self.client.ping() # send message
        self.assertEqual(len(self.server.messages),0)             # no messages processed yet
        self.assertEqual(len(self.server.channel.srv_messages),1) # message got stored in inbound queue
        
        self.server.run() # now we process messages. first exception happens here

        self.assertEqual(len(self.server.messages),1)             # 1 processed messages by server
        self.assertEqual(len(self.server.channel.srv_messages),1) # message is still in queue
        self.assertEqual(self.server.counter_bad,1)
        self.assertEqual(self.server.counter_good,0)

        self.server.run() # try to process it second time. second exception happens

        self.assertEqual(len(self.server.messages),2)             # 2 processed messages by server (we processed first message twice)
        self.assertEqual(len(self.server.channel.srv_messages),1) # message is still in queue
        self.assertEqual(self.server.counter_bad,2)
        self.assertEqual(self.server.counter_good,0)

        self.server.mode = 1    # now ping message should be processed without exception
        self.server.run()
        self.assertEqual(len(self.server.messages),3)
        self.assertEqual(len(self.server.channel.srv_messages),0)
        self.assertEqual(self.server.counter_bad,2)
        self.assertEqual(self.server.counter_good,1)

    def test_nack_deads_enabled(self):
        self.server.deads_disabled = False # nack'ed message should go to deads queue
        self.server.mode = 'nack testing'  # now every ping message should cause an exception
        self.server.max_sleep = 0
        self.assertEqual(self.server.channel.srv_queue, None)
        self.server.prepare()
        self.assertEqual(self.server.channel.srv_queue, 'rpc')

        self.client.ping() # send message
        self.assertEqual(len(self.server.messages),0)             # no messages processed yet
        self.assertEqual(len(self.server.channel.srv_messages),1) # message got stored in inbound queue
        
        self.server.run() # now we process messages. first exception happens here

        self.assertEqual(len(self.server.messages),1)             # 1 processed messages by server
        self.assertEqual(len(self.server.channel.srv_messages),0) # queue is empty
        self.assertEqual(self.server.counter_bad,1)               # 1 message with exception
        self.assertEqual(self.server.counter_good,0)

        self.server.run() # run queue processing again. no messages should be there

        self.assertEqual(len(self.server.messages),1)             # 1 processed messages by server (not changed)
        self.assertEqual(len(self.server.channel.srv_messages),0) # queue is empty
        self.assertEqual(self.server.counter_bad,1)
        self.assertEqual(self.server.counter_good,0)

        self.server.mode = 1    # now ping message should be processed without exception
        self.client.ping()             # send new message
        self.server.run()
        self.assertEqual(len(self.server.messages),2)
        self.assertEqual(len(self.server.channel.srv_messages),0)
        self.assertEqual(self.server.counter_bad,1)
        self.assertEqual(self.server.counter_good,1)


