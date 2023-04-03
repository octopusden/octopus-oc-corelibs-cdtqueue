import unittest
from oc_cdt_queue2.queue_handler import QueueHandler
import json
import logging

# to get rid of logging output from imported classes
logging.getLogger().propagate = False
logging.getLogger().disabled = True


class TestClass(QueueHandler):

    published = ['methodA', 'methodB', 'ping']

    def __init__(self):
        super(TestClass, self).__init__()
        self.args = None
        self.kvargs = None   # MethodB stores all of its args here
        self.method = list() # MethodA and MethodB sets this to its name so we can check it works correctly
        self.messages = []   # for parameter parsing tests. ping() stores reveiced parameters here
        self.mode = 1        # set self.mode to string value to raise an exception on method call
        self.itworks = None  # for testing access violation
        self.stop_after = 1

    def methodA(self):
        self.method.append('MethodA')
        if self.mode != 1:
            raise(ValueError(self.mode))

    def methodB(self, *args, **kvargs):
        self.args = args
        self.kvargs = kvargs
        self.method.append('MethodB')
        if self.mode != 1:
            raise(ValueError(self.mode))

    def ping(self, msg1=None, msg2=None, test=None):
        self.messages += [['ping', msg1, msg2, test]]
        if self.mode != 1:
            raise(ValueError(self.mode))

    def violate(self):
        """
        This method can't be called from rpc because it is not defined in "published" property
        """
        self.itworks = True


class QueueHandlerTest(unittest.TestCase):

    def setUp(self):
        self.server = TestClass()

    # testing settings is not interesting since
    # there is no separate settings in handler
    # other has been tested in 'test_server' which is parent class
    # also it is out of interest to verify counters - it is done in 'test_server' also

    def __msg(self, method, *args, **kwargs):
        if not isinstance(args, list):
            args = list(args)

        if not isinstance(kwargs, dict):
            kwargs = {}

        return [method, args, kwargs]

    # everywhere we are not interested in 'properties' since they are not (yet?) used
    def test_incorrect_message(self):
        with self.assertRaises(ValueError):
            self.server.on_message("blablabla", None)

    def test_call_parameter(self):

        self.server.on_message(self.__msg('ping', msg2='hello'), None)
        self.server.on_message(self.__msg('ping', msg1='hello'), None)
        self.server.on_message(self.__msg('ping', test='hello'), None)

        with self.assertRaises(TypeError):
            self.server.on_message(self.__msg(
                'ping', 'blablabla', msg1='test'), None)  # this should fail

        msg = self.server.messages.pop()
        self.assertEqual(msg, ['ping', None, None, 'hello'])
        msg = self.server.messages.pop()
        self.assertEqual(msg, ['ping', 'hello', None, None])
        msg = self.server.messages.pop()
        self.assertEqual(msg, ['ping', None, 'hello', None])
        self.assertEqual(len(self.server.messages), 0)

    def test_access_violation(self):
        with self.assertRaises(ValueError):
            self.server.on_message(self.__msg('violate'), None)
        self.assertIsNone(self.server.itworks)

        # check 'violate' method works itself
        self.server.violate()
        self.assertTrue(self.server.itworks)

    def test_different_methods(self):
        self.server.on_message(self.__msg('methodA'), None)
        self.server.on_message(self.__msg('methodB'), None)
        self.assertEqual(sorted(self.server.method), sorted(['MethodA', 'MethodB']))

    # tests for receive/ack/nack are not needed here
    # since them will repeat those in 'test_server' and 'test_connection_prcs'
