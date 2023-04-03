import unittest
from oc_cdt_queue2.queue_server import QueueServer
from oc_cdt_queue2.ipc_messages import IpcExcMsg
from oc_cdt_queue2.ipc_messages import IpcMessage
from oc_cdt_queue2.ipc_messages import IpcMessageResult
import argparse
import logging
import pika
import time
import json
from .mocks.queue_t import JoinableQueue

## BEG:MOCKS

class _ConnectionMock(object):
    def __init__(self, parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None):
        self.params = parameters
        self.callbacks = self._CbList()
        self.callbacks.on_open = on_open_callback
        self.callbacks.on_open_error = on_open_error_callback
        self.callbacks.on_close = on_close_callback


class _ConnectionPrcsMock(object):
    def __init__(self, connection, params, prefetch_count, queue, deads_disabled,
                 declare, ipc_q_out, ipc_q_in):
        self._Connection = connection
        self._connection = None
        self.params = params
        self.prefetch_count = prefetch_count
        self.queue = queue
        self.deads_disabled = deads_disabled
        self.declare = declare
        self.ipc_q_out = ipc_q_out
        self.ipc_q_in = ipc_q_in
        self.__is_alive = False
        # use time as pid - surely it will not be the same
        # does not matter it is float, for our test this is enough
        self.__pid = time.time()

    def start(self):
        self.__is_alive = True

    def terminate(self):
        self.__is_alive = False

    def is_alive(self):
        return self.__is_alive

    @property
    def pid(self):
        return self.__pid


class _MockServerOnMsg(QueueServer):
    """
    special mock for testin 'on_message_raw'
    it is moved here since used in more than one test
    """

    def __init__(self, *argv, **argp):
        self._on_message_called = 0
        self._on_message_body = None
        self._on_message_props = None
        super(_MockServerOnMsg, self).__init__(*argv, **argp)

    def on_message(self, body, properties):
        self._on_message_called += 1
        self._on_message_body = body
        self._on_message_props = properties
## END:MOCKS

# to get rid of logging output from imported classes
logging.getLogger().propagate = False
logging.getLogger().disabled = True


class QueueServerTest(unittest.TestCase):
    def setUp(self):
        self.__assign_server(QueueServer)
        self.queue = 'test.input'

    def __assign_server(self, server_class):
        self.server = server_class()
        self.server._Connection = _ConnectionMock
        self.server._JoinableQueue = JoinableQueue
        self.server._QueueConnectionProcess = _ConnectionPrcsMock

    def __setup_server(self):
        # configure server with some defaults
        parser = argparse.ArgumentParser(description='test parser')
        self.server.basic_args(parser)
        args = parser.parse_args('--amqp-url amqp://127.0.0.1 --reconnect-tries 1 --reconnect-delay 0 --prefetch-count 4 --queue test.input --declare yes'.split(' '))
        self.server.setup_from_args(args)
        self.server.connect()

    def test_setup_from_args(self):
        parser = argparse.ArgumentParser(description='test parser')
        self.server.basic_args(parser)
        args = parser.parse_args('--amqp-url amqp://203.0.113.1 --reconnect-tries 10 --reconnect-delay 0 --prefetch-count 11'.split(' ')) # 203.0.113.0/24 is example network: RFC5737
        self.server.setup_from_args(args)
        self.assertEqual(self.server.reconnect_tries, 10)
        self.assertEqual(self.server.reconnect_delay, 0)
        self.assertEqual(self.server.connection_parameters.host, '203.0.113.1')
        self.assertEqual(self.server.prefetch_count, 11)

    def test_default_prefetch_count(self):
        self.assertEqual(self.server.prefetch_count, self.server.default_prefetch_count)

    def test_setup(self):
        self.server.setup(prefetch_count=111)
        self.assertEqual(self.server.prefetch_count, 111)

    def test_stop(self):
        self.assertFalse(self.server._stop)
        self.server.stop()
        self.assertTrue(self.server._stop)

    def test_connect(self):
        # connection is to be established for the first time
        # child process is to be created with a mocks given above
        parser = argparse.ArgumentParser(description='test parser')
        self.server.basic_args(parser)
        args = parser.parse_args(('--amqp-url amqp://127.0.0.1 --reconnect-tries 1 --reconnect-delay 0 --prefetch-count 4 --queue %s --declare yes' % self.queue).split(' '))
        self.server.setup_from_args(args)
        self.server.connect()
        self.assertIsNotNone(self.server._connection_prcs)
        self.assertIsInstance(self.server._connection_prcs, _ConnectionPrcsMock)
        self.assertTrue(self.server._connection_prcs.is_alive())
        self.assertIsInstance(self.server._ipc_q_in, JoinableQueue)
        self.assertIsInstance(self.server._ipc_q_out, JoinableQueue)

        # compare connection process parameters with those given here above
        self.assertEqual(self.server._connection_prcs.params.host, '127.0.0.1')
        self.assertIsInstance(self.server._connection_prcs.prefetch_count, int)
        self.assertEqual(self.server._connection_prcs.prefetch_count, 4)
        self.assertEqual(self.server._connection_prcs.queue, self.queue)
        self.assertFalse(self.server._connection_prcs.deads_disabled)
        self.assertEqual(self.server._connection_prcs.declare, self.server.queue_declare)

    def test_reconnect(self):
        # once connected 'connect' is called once agian
        # we are not interesting in 'disconnect' results, so simply mock it
        class _MockServerConnect(QueueServer):
            def __init__(self, *args, **kwargs):
                super(_MockServerConnect, self).__init__(*args, **kwargs)
                self._disconnect_called = False

            def disconnect(self):
                self._disconnect_called = True
                self._connection = None

        self.__assign_server(_MockServerConnect)
        self.__setup_server()
        self.assertIsNotNone(self.server._connection_prcs)
        self.assertTrue(self.server._connection_prcs.is_alive())
        _first_pid = self.server._connection_prcs.pid
        self.assertFalse(self.server._disconnect_called)
        self.assertIsInstance(self.server._ipc_q_in, JoinableQueue)
        self.assertIsInstance(self.server._ipc_q_out, JoinableQueue)

        # put some messages in both queues. After restart we can assure those messages are not in memory
        for _ind in range(0, 30):
            self.server._ipc_q_in.put(_ind)
            self.server._ipc_q_out.put(_ind)

        self.assertFalse(self.server._ipc_q_in.empty())
        self.assertFalse(self.server._ipc_q_out.empty())

        # call connect while disconnect is not done
        self.server.connect()
        self.assertIsNotNone(self.server._connection_prcs)
        self.assertTrue(self.server._disconnect_called)
        self.assertTrue(self.server._connection_prcs.is_alive())
        self.assertNotEqual(_first_pid, self.server._connection_prcs.pid)
        self.assertIsInstance(self.server._ipc_q_in, JoinableQueue)
        self.assertIsInstance(self.server._ipc_q_out, JoinableQueue)
        self.assertTrue(self.server._ipc_q_in.empty())
        self.assertTrue(self.server._ipc_q_out.empty())

    # disconnect test cases:
    # - no child
    # - child exit OK
    # - child exit with error
    def test_disconnect(self):
        parser = argparse.ArgumentParser(description='test parser')
        self.server.basic_args(parser)
        args = parser.parse_args('--amqp-url amqp://127.0.0.1 --reconnect-tries 1 --reconnect-delay 0 --prefetch-count 4 --queue test.input --declare yes'.split(' '))
        self.server.setup_from_args(args)
        self.server.connect()  # to create connection process and set 'is_alive' property
        self.assertIsNotNone(self.server._connection_prcs)
        self.assertTrue(self.server._connection_prcs.is_alive())
        _conn_prcs = self.server._connection_prcs
        _ipc_q = _conn_prcs.ipc_q_in  # we shall need these two for assertions below
        self.assertIsInstance(self.server._ipc_q_in, JoinableQueue)
        self.assertIsInstance(self.server._ipc_q_out, JoinableQueue)

        # we are ready to call 'disconnect'
        self.server.disconnect()
        self.assertIsNone(self.server._connection_prcs)
        self.assertIsNone(self.server._ipc_q_in)
        self.assertIsNone(self.server._ipc_q_out)

        # see the termination message was send to our child
        self.assertFalse(_ipc_q.empty())
        _qmsg = _ipc_q.get()
        self.assertIsInstance(_qmsg, IpcExcMsg)

        # see our queue was closed and process was terminated
        self.assertTrue(_ipc_q.joined)
        self.assertFalse(_conn_prcs.is_alive())

    def test_prc_ipc_q(self):
        # we do not want real message processing, so mock the corresponding method
        class _MockServerIQ(QueueServer):
            def __init__(self, *argv, **argp):
                self._item_list = list()
                super(_MockServerIQ, self).__init__(*argv, **argp)

            def _prcs_ipc_q_pop(self, do_process=True):
                _item = self._ipc_q_in.get()
                self._item_list.append(_item)
                self._ipc_q_in.task_done()

        self.__assign_server(_MockServerIQ)
        self.__setup_server()
        self.server.connect()
        _ipc_q = self.server._ipc_q_in

        # construct some messages to test on and put it to output queue
        _our_msg_list = list()
        for delivery_tag in range(0, 10):
            _our_msg_list.append(IpcMessage(delivery_tag,
                                            {'test_property_%d' %
                                                delivery_tag: 'property_%d' % delivery_tag},
                                            "{'test_body_%d' : 'body_%d'}" % (delivery_tag, delivery_tag)))

        # append some exception and put it to queue
        _our_msg_list.append(IpcExcMsg(Exception("Test exception")))

        for _msg in _our_msg_list:
            _ipc_q.put(_msg)

        self.server._prcs_ipc_q()
        self.assertEqual(len(self.server._item_list), len(_our_msg_list))
        self.assertTrue(_ipc_q.empty())

        # check every message has been received
        for _msg in _our_msg_list:
            self.assertIn(_msg, self.server._item_list)

    def test_prc_ipc_q_pop(self):
        # here we can replace a queue and see what is going on by stacking calls
        class _MockServerIQ(QueueServer):
            def __init__(self, *argv, **argp):
                self.item_list = list()
                super(_MockServerIQ, self).__init__(*argv, **argp)

            def _process_message(self, delivery_tag, properties, body):
                self.item_list.append({delivery_tag: [properties, body]})

        self.__assign_server(_MockServerIQ)
        self.__setup_server()
        _ipc_q = self.server._ipc_q_in

        # put some messages to queue
        # put one exception so on
        for delivery_tag in range(0, 11):
            if not delivery_tag:
                # put an exception
                _ipc_q.put(IpcExcMsg(Exception("Test exception")))
            else:
                #put normal message
                _msg_props = {'test_property_%d' % delivery_tag: 'property_%d' % delivery_tag}
                _msg_body = "{'test_body_%d' : 'body_%d'}" % (delivery_tag, delivery_tag)
                _ipc_q.put(IpcMessage(delivery_tag, _msg_props, _msg_body))

            self.assertFalse(_ipc_q.empty())
            # not supported in python 2: self.server.item_list.clear()
            self.assertEqual(0, len(self.server.item_list))
            self.server._prcs_ipc_q_pop(do_process=(delivery_tag != 10))
            # if we put an exception - it is to be logged only
            # if we do not process messages (delivery_tag == 10) - too
            if not delivery_tag or delivery_tag == 10:
                self.assertEqual(len(self.server.item_list), 0)
                self.assertTrue(_ipc_q.empty())
                continue

            # all other cases message is to be processed
            # compare its attributes to the original
            self.assertTrue(_ipc_q.empty())
            self.assertEqual(len(self.server.item_list), 1)
            _item = self.server.item_list.pop()
            self.assertIn(delivery_tag, _item)
            self.assertEqual(_item.get(delivery_tag)[0], _msg_props)
            self.assertEqual(_item.get(delivery_tag)[1], _msg_body)

    def test_run(self):
        # here we are interesting in suquence of calls
        class _MockServerRun(QueueServer):
            def __init__(self, *argv, **argp):
                self.reset()
                super(_MockServerRun, self).__init__(*argv, **argp)

            def reset(self):
                self._counter = 0
                self._disconnect_called = None
                self._prcs_ipc_q_pop_called = None
                self._increase_ipc_delay_called = None
                self._stop = False

            def disconnect(self):
                self._disconnect_called = self._counter
                self._counter += 1

            def _prcs_ipc_q_pop(self, do_process=True):
                self._prcs_ipc_q_pop_called = self._counter
                self._ipc_q_in.get()
                self._counter += 1
                self._ipc_q_in.task_done()

            def _increase_ipc_delay(self):
                self._increase_ipc_delay_called = self._counter
                self._counter += 1
                # we want to break the loop when queue is empty
                self._stop = True

        ###1 no connection process
        self.__assign_server(_MockServerRun)
        # NOTE: we do not need to setup_server here
        self.server._connection_prcs = None
        self.server.run()
        # Nothing sould happen
        self.assertEqual(0, self.server._counter)

        ###2 now we have some connection process, but it is dead
        self.__setup_server()
        self.server.reset()
        self.server._connection_prcs.terminate()
        # should be one disconnect only
        self.assertFalse(self.server._connection_prcs.is_alive())
        self.server.run()
        self.assertEqual(1, self.server._counter)
        self.assertEqual(0, self.server._disconnect_called)

        ###3 a queue is not empty: should process everything then call disconnect
        self.__setup_server()
        self.server.reset()
        _ipc_q = self.server._ipc_q_in
        # put 2 messages
        _ipc_q.put(IpcMessage(0, {'property': "blablabla"}, "{'body' : 'blablabody'}"))
        _ipc_q.put(IpcMessage(1, {'property': "non-blablabla"}, "{'body' : 'nonblablabody'}"))
        self.assertFalse(_ipc_q.empty())
        self.server.run()
        self.assertTrue(_ipc_q.empty())
        self.assertEqual(self.server._counter, 4)
        # first two calls are to process our messages
        # then one call to increase delay
        # and last one is disconnect since we set _stop in _increase_ipc_delay
        self.assertEqual(1, self.server._prcs_ipc_q_pop_called)
        self.assertEqual(2, self.server._increase_ipc_delay_called)
        self.assertEqual(3, self.server._disconnect_called)

    def test_process_message_ok(self):
        # re-define on_message_raw to do something and return OK
        # check _ipc_delay decreased
        # check _report_message_result called with ack = True (replace _ipc_q_out and catch it)
        # check _on_ack called: counter increased, counter_good increased, _nacks reset
        class _MockServerMsgPrcs(QueueServer):
            def on_message_raw(self, body, properties):
                for __itg in body:
                    __itg = None

        # construct a message
        _msgbody = "{'the way I off': 'blablabla'}"
        _msgprops = pika.BasicProperties(content_type='application/json',
                                         content_encoding='ascii',
                                         headers={'x-header-q': 'Babs'},
                                         type='Pea',
                                         priority=1)

        self.__assign_server(_MockServerMsgPrcs)
        self.__setup_server()
        _ipc_q = self.server._ipc_q_out
        self.server.deads_disabled = True
        self.assertTrue(_ipc_q.empty())
        __ipc_delay = self.server._ipc_delay
        __counter_msgs = self.server.counter_messages
        __counter_good = self.server.counter_good
        __counter_bad = self.server.counter_bad
        self.server._process_message(0, _msgprops, _msgbody)
        self.assertFalse(_ipc_q.empty())
        self.assertTrue(self.server._ipc_delay < __ipc_delay)
        self.assertTrue(self.server.counter_good > __counter_good)
        self.assertEqual(self.server.counter_bad, __counter_bad)
        self.assertEqual(self.server._nacks, 0)
        self.assertTrue(self.server.counter_messages > __counter_msgs)

        __item = _ipc_q.get()
        _ipc_q.task_done()
        self.assertTrue(__item.ack)
        self.assertEqual(__item.delivery_tag, 0)
        self.assertTrue(__item.time_delta > 0)

    def test_process_message_fail(self):
        # re-define on_message_raw to do something and return OK
        # check _ipc_delay not changed
        # check _report_message_result called with ack = False (replace _ipc_q_out and catch it)
        # check _on_nack called: counter increased, counter_bad increased, _nacks increased
        class _MockServerMsgPrcs(QueueServer):
            def on_message_raw(self, body, properties):
                raise Exception("Test exception: message processing failed")

        # construct a message
        _msgbody = "{'the way I off': 'blablabla'}"
        _msgprops = pika.BasicProperties(content_type='application/json',
                                         content_encoding='ascii',
                                         headers={'x-header-q': 'Babs'},
                                         type='Pea',
                                         priority=1)

        self.__assign_server = (_MockServerMsgPrcs)
        self.__setup_server()
        self.server.deads_disabled = True
        _ipc_q = self.server._ipc_q_out
        self.assertTrue(_ipc_q.empty())
        __ipc_delay = self.server._ipc_delay
        __counter_msgs = self.server.counter_messages
        __counter_good = self.server.counter_good
        __counter_bad = self.server.counter_bad
        self.server._nacks = 1
        self.server._process_message(0, _msgprops, _msgbody)
        self.assertFalse(_ipc_q.empty())
        self.assertFalse(self.server._ipc_delay < __ipc_delay)
        self.assertEqual(self.server.counter_good, __counter_good)
        self.assertTrue(self.server.counter_bad > __counter_bad)
        self.assertEqual(self.server._nacks, 2)
        self.assertTrue(self.server.counter_messages > __counter_msgs)

        __item = _ipc_q.get()
        self.assertFalse(__item.ack)
        self.assertEqual(__item.delivery_tag, 0)

    def test_on_ack(self):
        ## counter increased + on_ack called
        # we do not care for body and properties yet so may simply pass None
        __counter_good = self.server.counter_good
        __counter_bad = self.server.counter_bad
        self.server._nacks = 10
        self.server._on_ack(body=None, properties=None)
        self.assertEqual(__counter_good + 1, self.server.counter_good)
        self.assertEqual(__counter_bad, self.server.counter_bad)
        self.assertEqual(0, self.server._nacks)

    def test_on_nack(self):
        ## counter increased + on_nack called
        # we do not care for body and properties yet so may simply pass None
        __counter_good = self.server.counter_good
        __counter_bad = self.server.counter_bad
        self.server._nacks = 3
        __nacks = self.server._nacks
        self.server._on_nack(body=None, properties=None, result=None)
        self.assertEqual(__counter_good, self.server.counter_good)
        self.assertEqual(__counter_bad + 1, self.server.counter_bad)
        self.assertEqual(__nacks + 1, self.server._nacks)
        # do not verify sleep here, it is a separate unit test

    def test_report_msg_result(self):
        # simple put a message to queue
        # queue may be replaced, multiprocessing is not needed here
        self.__setup_server()
        _ipc_q = self.server._ipc_q_out
        self.assertTrue(_ipc_q.empty())
        _delivery_tag = 777
        _ack = False
        _requeue = True
        _time_delta = 77.777
        self.server._report_message_result(_delivery_tag, _ack, _requeue, _time_delta)
        self.assertFalse(_ipc_q.empty())

        _item = _ipc_q.get()
        _ipc_q.task_done()
        self.assertIsInstance(_item, IpcMessageResult)
        self.assertEqual(_item.delivery_tag, _delivery_tag)
        self.assertEqual(_item.ack, _ack)
        self.assertEqual(_item.requeue, _requeue)
        self.assertEqual(_item.time_delta, _time_delta)

    def test_on_message_raw_ok(self):
        # check content-type
        # transfer data to json
        # call 'on_message'
        self.__assign_server(_MockServerOnMsg)
        self.__setup_server()
        _props = pika.BasicProperties(content_type='application/json')
        _body = '{"test_body" : "test_value"}'
        self.assertEqual(self.server._on_message_called, 0)
        self.server.on_message_raw(_body, _props)
        self.assertEqual(self.server._on_message_called, 1)
        self.assertEqual(_props.content_type, self.server._on_message_props.content_type)
        self.assertEqual(json.loads(_body), self.server._on_message_body)

    def test_on_message_raw_fail(self):
        # check content-type and raise an exception
        # no call 'on_message'

        self.__assign_server(_MockServerOnMsg)
        self.__setup_server()
        _props = pika.BasicProperties(content_type='application/zip')
        _body = "{'test_body' : 'test_value'}"
        self.assertEqual(self.server._on_message_called, 0)

        with self.assertRaises(ValueError):
            self.server.on_message_raw(_body, _props)

        self.assertEqual(self.server._on_message_called, 0)
        self.assertIsNone(self.server._on_message_props)
        self.assertIsNone(self.server._on_message_body)
