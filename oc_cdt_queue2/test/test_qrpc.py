import unittest
from oc_cdt_queue2.queue_rpc import QueueRPC


class _TestClass(QueueRPC):
    def __init__(self, *args, **kwargs):
        super(_TestClass, self).__init__(*args, **kwargs)
        self.published = ['ping', 'methodA', 'methodB']
        self.msg_buffer = list()

    # replace 'send' method to collect messages
    def send(self, body):
        self.msg_buffer.append(body)


class _ConnectionMock(object):
    def __init__(self):
        self.is_open = True


class _ChannelMock(object):
    pass


class QueueRpcTest(unittest.TestCase):
    def test_published_calls(self):
        _client = _TestClass()
        _client.connection = _ConnectionMock()
        _client.channel = _ChannelMock()
        _client.ping()
        _client.methodA('arg1', arg2=2)
        _client.methodB(arg1=1)

        # this method is not published:
        with self.assertRaises(AttributeError):
            _client.violate()

        # check messages send
        self.assertEqual(len(_client.msg_buffer), 3)
        # buffer is list, processing is to be done in reverse order
        self.assertEqual(_client.msg_buffer.pop(), ['methodB', (), {'arg1': 1}])
        self.assertEqual(_client.msg_buffer.pop(), ['methodA', ('arg1',), {'arg2': 2}])
        self.assertEqual(_client.msg_buffer.pop(), ['ping', (), {}])
