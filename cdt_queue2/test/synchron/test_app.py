import unittest
from .mocks.queue_rpc import QueueRPC
from .mocks.queue_application import QueueApplication
from .mocks.queue_loopback import LoopbackChannel, LoopbackConnection, global_messaging, global_message_queue
import pika

# to get rid of logging output from imported classes
import logging
logging.getLogger().propagate = False
logging.getLogger().disabled = True

class MediocreChannel(LoopbackChannel):
    exceptions_to_raise = [] 
    calls = 0

    def start_consuming(self, *args, **kvargs):
        MediocreChannel.calls += 1
        if len(MediocreChannel.exceptions_to_raise) > 0: raise(MediocreChannel.exceptions_to_raise.pop())
        super(MediocreChannel, self).start_consuming(*args, **kvargs)


class MediocreConnection(LoopbackConnection):
    def channel(self):
        return MediocreChannel(self)



class MyRPC(QueueRPC):
    published = ['ping', 'method1', 'method2']

class MyApplication(QueueApplication):
    published = ['ping', 'method1', 'method2']
    def ping(self):
        pass

    def method1(self):
        self.called_method1 = True
        pass

    def method2(self, arg1):
        self.called_method2 = True
        self.called_argument = arg1
        pass

class QueueApplicationTest(unittest.TestCase):

    def setUp(self):
        self.app = MyApplication()
        self.rpc = MyRPC()

        self.rpc.setup('amqp://127.0.0.1/', queue='my')
        self.rpc.connect()

        self.app.max_sleep = 0

    def test_app(self):
        self.assertTrue(global_messaging)
        self.app.main('--amqp-url amqp://127.0.0.1/ --declare only --queue my'.split(' '))
        self.assertTrue('my' in global_message_queue)
        self.rpc.ping()
        self.rpc.method1()
        self.app.main('--amqp-url amqp://127.0.0.1/ --queue my'.split(' '))
        self.assertEqual(self.app.called_method1, True)
        self.assertEqual(self.app.counter_bad,0)
        self.assertEqual(self.app.counter_good,2)

    def test_reconnect(self):
        self.app._Connection = MediocreConnection 

        MediocreChannel.calls = 0
        MediocreChannel.exceptions_to_raise = [pika.exceptions.ConnectionClosed(504, 'oops'), pika.exceptions.AMQPConnectionError('whops'), pika.exceptions.ChannelClosed(406, 'boom')]

        self.app.main('--amqp-url amqp://127.0.0.1/ --declare yes --queue my --reconnect --reconnect-tries 0 --reconnect-delay 0'.split(' '))
        self.assertEqual(MediocreChannel.calls, 4)

