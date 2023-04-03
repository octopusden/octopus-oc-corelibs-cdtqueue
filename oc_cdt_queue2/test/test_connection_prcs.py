import unittest
from oc_cdt_queue2.queue_connection_prcs import QueueConnectionProcess
from oc_cdt_queue2.ipc_messages import IpcMessage
from oc_cdt_queue2.ipc_messages import IpcMessageResult
from oc_cdt_queue2.ipc_messages import IpcExcMsg
from .mocks.queue_t import JoinableQueue
import pika
import logging

# this will remove exaustive logging output from our testing console
logging.getLogger().propagate = False
logging.getLogger().disabled = True


class _MockMethod(object):
    def __init__(self, delivery_tag):
        self.delivery_tag = delivery_tag


class _IOLoopMock(object):
    def __init__(self):
        self.__started = True
        self.call_buffer = list()

    def call_later(self, delay, callback):
        self.call_buffer.append({'delay': delay, 'call': callback})

    def start(self):
        self.__started = True

    def stop(self):
        self.__started = False

    @property
    def is_running(self):
        return self.__started


class _ConnectionMock(object):
    class _CbList(object):
        on_open = None
        on_close = None
        on_open_error = None
        on_channel = None

    def __init__(self, parameters=None,
                 on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None):
        self.params = parameters
        self.callbacks = self._CbList()
        self.callbacks.on_open = on_open_callback
        self.callbacks.on_open_error = on_open_error_callback
        self.callbacks.on_close = on_close_callback
        self.ioloop = _IOLoopMock()
        self.__is_open = True
        self.__closing = False
        self.channels = list()

    @property
    def is_open(self):
        return self.__is_open

    @property
    def is_closed(self):
        return not self.__is_open

    @property
    def is_closing(self):
        return self.is_closed

    def close(self):
        self.__is_open = False

    def channel(self, on_open_callback):
        _chan = _ChannelMock()
        self.callbacks.on_channel = on_open_callback
        self.channels.append(_chan)


class _ChannelMock(object):
    """
    Mock for some channel callback testing
    """

    def __init__(self):
        self.calls = list()
        self._temp_attr = None

    def __attrcall(self, *args, **kwargs):
        self.calls.append({self._temp_attr: [args, kwargs]})
        self._temp_attr = None

    def __getattr__(self, attr):
        self._temp_attr = attr
        return self.__attrcall


class QueueConnectionTest(unittest.TestCase):
    def setUp(self):
        self._url = 'amqp://localhost'
        self._queue_prd = 'test_q.input'
        self._queue_dds = 'test_q.deads'
        self._params = pika.URLParameters(self._url)
        self._ipc_q_in = JoinableQueue()
        self._ipc_q_out = JoinableQueue()
        self._connection_timeout = 1.15  # sec

    # during init we do not want to start a separate process
    def test_init_no_connection_class(self):
        with self.assertRaises(TypeError):
            _cn = QueueConnectionProcess(
                connection=None,
                params=self._params,
                prefetch_count=1,
                queue=self._queue_prd,
                deads_disabled=False,
                declare='yes',
                ipc_q_in=self._ipc_q_out,
                ipc_q_out=self._ipc_q_in)

    def test_init_prefetch_wrong(self):
        with self.assertRaises(TypeError):
            _cn = QueueConnectionProcess(
                connection=_ConnectionMock,
                params=self._params,
                prefetch_count='1',
                queue=self._queue_prd,
                deads_disabled=False,
                declare='yes',
                ipc_q_in=self._ipc_q_out,
                ipc_q_out=self._ipc_q_in)

    def test_init_no_queue(self):
        with self.assertRaises(ValueError):
            _cn = QueueConnectionProcess(
                connection=_ConnectionMock,
                params=self._params,
                prefetch_count=1,
                queue='',
                deads_disabled=False,
                declare='yes',
                ipc_q_in=self._ipc_q_out,
                ipc_q_out=self._ipc_q_in)

    def test_init_wrong_params(self):
        with self.assertRaises(TypeError):
            _cn = QueueConnectionProcess(
                connection=_ConnectionMock,
                params=self._url,
                prefetch_count=7,
                queue=self._queue_prd,
                deads_disabled=False,
                declare='yes',
                ipc_q_in=self._ipc_q_out,
                ipc_q_out=self._ipc_q_in)

    def test_init_deads_enabled(self):
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='no',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        self.assertEqual(_cn._rmq_main, self._queue_prd)
        self.assertEqual(_cn._rmq_deads, self._queue_dds)

    def test_init_deads_disabled(self):
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=True,
            declare='no',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        self.assertEqual(_cn._rmq_main, self._queue_prd)
        self.assertIsNone(_cn._rmq_deads)

    def test_init_declare_unsupported(self):
        with self.assertRaises(ValueError):
            _cn = QueueConnectionProcess(
                connection=_ConnectionMock,
                params=self._params,
                prefetch_count=1,
                queue=self._queue_prd,
                deads_disabled=False,
                declare='blablabla',
                ipc_q_in=self._ipc_q_out,
                ipc_q_out=self._ipc_q_in)

    def test_put_out_exception(self):
        err_text = "blablabla"
        err = Exception(err_text)
        err.reply_code = 200

        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        self.assertTrue(self._ipc_q_in.empty())
        _cn._put_out_exception(err)
        self.assertFalse(self._ipc_q_in.empty())
        _item = self._ipc_q_in.get()
        self._ipc_q_in.task_done()
        self.assertEqual(repr(err), _item.msg)
        self.assertEqual(_item.reply_code, err.reply_code)

    def test_connect_ok(self):
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _cn.connect()
        self.assertIsNotNone(_cn._connection)
        self.assertTrue(_cn._connection.ioloop.is_running)
        self.assertEqual(_cn._connection.callbacks.on_open, _cn.on_connection_open)
        self.assertEqual(_cn._connection.callbacks.on_open_error, _cn.on_connection_open_error)
        self.assertEqual(_cn._connection.callbacks.on_close, _cn.on_connection_closed)

    def test_connect_start_fail(self):
        class _ConnectionMockExc(_ConnectionMock):
            def __init__(self, parameters=None,
                         on_open_callback=None,
                         on_open_error_callback=None,
                         on_close_callback=None):
                super(_ConnectionMockExc, self).__init__(parameters,
                                                         on_open_callback, on_open_error_callback, on_close_callback)
                # to emulate an error we can simply assign 'None' to ioloop
                self.ioloop = None

        _cn = QueueConnectionProcess(
            connection=_ConnectionMockExc,
            params=pika.URLParameters('amqp://203.0.113.0'),
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _cn.connect()
        self.assertTrue(_cn._connection.is_closing)
        self.assertFalse(self._ipc_q_in.empty())
        self.assertIsInstance(self._ipc_q_in.get(), IpcExcMsg)

    def test_on_connection_open(self):
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _cns = _ConnectionMock()
        _cn._connection = _cns
        _cn.on_connection_open(_cns)
        self.assertEqual(len(_cns.channels), 1)
        self.assertEqual(_cns.callbacks.on_channel, _cn.on_channel_open)
        self.assertTrue(_cns.ioloop.is_running)

    def test_on_connection_open_error(self):
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _err_text = "Test Error"
        _exc = pika.exceptions.ConnectionClosed(406, _err_text)

        _cns = _ConnectionMock()
        _cn._connection = _cns
        _cn.on_connection_open_error(_cns, _exc)
        self.assertFalse(self._ipc_q_in.empty())
        while not self._ipc_q_in.empty():
            __item = self._ipc_q_in.get()
            self._ipc_q_in.task_done()
            self.assertIsInstance(__item, IpcExcMsg)
            self.assertEqual(__item.reply_code, 406)
            self.assertEqual(__item.type, type(_exc))

        self.assertFalse(_cns.ioloop.is_running)
        self.assertIsNone(_cn._connection)

    def test_on_channel_open_ok(self):
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _chan = _ChannelMock()
        _cn.on_channel_open(_chan)
        self.assertEqual(_chan, _cn._channel)
        self.assertEqual(len(_chan.calls), 3)
        # note on reverse stack analysis
        # third call
        _elmnt = _chan.calls.pop()
        self.assertIn('exchange_declare', _elmnt)
        _kwargs = _elmnt.get('exchange_declare')[1]
        self.assertEqual(_kwargs.get('exchange'), self._queue_dds)
        self.assertEqual(_kwargs.get('exchange_type'), 'direct')
        self.assertTrue(_kwargs.get('durable'))
        self.assertEqual(_kwargs.get('callback'), _cn.on_deads_exchange_declared)
        # second call
        _elmnt = _chan.calls.pop()
        self.assertIn('add_on_cancel_callback', _elmnt)
        self.assertEqual(_elmnt.get('add_on_cancel_callback')[0][0], _cn.disconnect)
        # first call
        _elmnt = _chan.calls.pop()
        self.assertIn('add_on_close_callback', _elmnt)
        self.assertEqual(_elmnt.get('add_on_close_callback')[0][0], _cn.on_channel_closed)

    def test_on_channel_open_no_deads(self):
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=True,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _chan = _ChannelMock()
        _cn.on_channel_open(_chan)
        self.assertEqual(_chan, _cn._channel)
        self.assertEqual(len(_chan.calls), 3)
        # note on reverse stack analysis
        # third call
        _elmnt = _chan.calls.pop()
        self.assertIn('queue_declare', _elmnt)
        _kwargs = _elmnt.get('queue_declare')[1]
        self.assertEqual(_kwargs.get('queue'), self._queue_prd)
        self.assertEqual(_kwargs.get('arguments'), {'x-max-priority': 3})
        self.assertTrue(_kwargs.get('durable'))
        self.assertEqual(_kwargs.get('callback'), _cn.on_main_queue_declared)
        # second call
        _elmnt = _chan.calls.pop()
        self.assertIn('add_on_cancel_callback', _elmnt)
        self.assertEqual(_elmnt.get('add_on_cancel_callback')[0][0], _cn.disconnect)
        # first call
        _elmnt = _chan.calls.pop()
        self.assertIn('add_on_close_callback', _elmnt)
        self.assertEqual(_elmnt.get('add_on_close_callback')[0][0], _cn.on_channel_closed)

    def test_on_channel_open_no_declare(self):
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='no',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _chan = _ChannelMock()
        _cn.on_channel_open(_chan)
        self.assertEqual(_chan, _cn._channel)
        self.assertEqual(len(_chan.calls), 3)
        # note on reverse stack analysis
        # third call
        _elmnt = _chan.calls.pop()
        self.assertIn('basic_qos', _elmnt)
        _kwargs = _elmnt.get('basic_qos')[1]
        self.assertEqual(_kwargs.get('prefetch_count'), 1)
        self.assertEqual(_kwargs.get('callback'), _cn.on_prefetch_set_ok)
        # second call
        _elmnt = _chan.calls.pop()
        self.assertIn('add_on_cancel_callback', _elmnt)
        self.assertEqual(_elmnt.get('add_on_cancel_callback')[0][0], _cn.disconnect)
        # first call
        _elmnt = _chan.calls.pop()
        self.assertIn('add_on_close_callback', _elmnt)
        self.assertEqual(_elmnt.get('add_on_close_callback')[0][0], _cn.on_channel_closed)

    def test_on_channel_open_declare_only(self):
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='only',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _chan = _ChannelMock()
        _cn._connection = _ConnectionMock()
        _cn.on_channel_open(_chan)
        self.assertEqual(_chan, _cn._channel)
        self.assertEqual(len(_chan.calls), 3)
        # note on reverse stack analysis
        # third call is to be 'exchange declare' here
        _elmnt = _chan.calls.pop()
        self.assertIn('exchange_declare', _elmnt)
        _kwargs = _elmnt.get('exchange_declare')[1]
        self.assertEqual(_kwargs.get('exchange'), self._queue_dds)
        self.assertEqual(_kwargs.get('exchange_type'), 'direct')
        self.assertTrue(_kwargs.get('durable'))
        self.assertEqual(_kwargs.get('callback'), _cn.on_deads_exchange_declared)
        # second call
        _elmnt = _chan.calls.pop()
        self.assertIn('add_on_cancel_callback', _elmnt)
        self.assertEqual(_elmnt.get('add_on_cancel_callback')[0][0], _cn.disconnect)
        # first call
        _elmnt = _chan.calls.pop()
        self.assertIn('add_on_close_callback', _elmnt)
        self.assertEqual(_elmnt.get('add_on_close_callback')[0][0], _cn.on_channel_closed)

    def test_on_deads_exchange_declared(self):
        # should only call 'queue_declare' with correct arguments
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _chan = _ChannelMock()
        _cn._channel = _chan
        _cn.on_deads_exchange_declared()
        self.assertEqual(len(_chan.calls), 1)
        # only call is to be 'queue declare' here
        _elmnt = _chan.calls.pop()
        self.assertIn('queue_declare', _elmnt)
        _kwargs = _elmnt.get('queue_declare')[1]
        self.assertEqual(_kwargs.get('queue'), self._queue_dds)
        self.assertEqual(_kwargs.get('arguments').get('x-max-priority'), 3)
        self.assertTrue(_kwargs.get('durable'))
        self.assertEqual(_kwargs.get('callback'), _cn.on_deads_queue_declared)

    def test_on_deads_queue_declared(self):
        # should only call 'queue_bind'
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _chan = _ChannelMock()
        _cn._channel = _chan
        _cn.on_deads_queue_declared()
        self.assertEqual(len(_chan.calls), 1)
        # only call is to be 'queue_bind' here
        _elmnt = _chan.calls.pop()
        self.assertIn('queue_bind', _elmnt)
        _kwargs = _elmnt.get('queue_bind')[1]
        self.assertEqual(_kwargs.get('queue'), self._queue_dds)
        self.assertEqual(_kwargs.get('exchange'), self._queue_dds)
        self.assertEqual(_kwargs.get('routing_key'), self._queue_dds)
        self.assertEqual(_kwargs.get('callback'), _cn.on_deads_queue_bind)

    def test_on_deads_queue_bind_enbld(self):
        # should only call 'queue_declare' with correct arguments
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _chan = _ChannelMock()
        _cn._channel = _chan
        _cn.on_deads_queue_bind()
        self.assertEqual(len(_chan.calls), 1)
        # only call is to be 'queue declare' here
        _elmnt = _chan.calls.pop()
        self.assertIn('queue_declare', _elmnt)
        _kwargs = _elmnt.get('queue_declare')[1]
        self.assertEqual(_kwargs.get('queue'), self._queue_prd)
        self.assertEqual(_kwargs.get('arguments').get('x-max-priority'), 3)
        self.assertEqual(_kwargs.get('arguments').get('x-dead-letter-exchange'), self._queue_dds)
        self.assertEqual(_kwargs.get('arguments').get('x-dead-letter-routing-key'), self._queue_dds)
        self.assertTrue(_kwargs.get('durable'))
        self.assertEqual(_kwargs.get('callback'), _cn.on_main_queue_declared)

    def test_on_deads_queue_bind_disbld(self):
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=True,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _chan = _ChannelMock()
        _cn._channel = _chan
        _cn.on_deads_queue_bind()
        self.assertEqual(len(_chan.calls), 1)
        # only call is to be 'queue declare' here
        _elmnt = _chan.calls.pop()
        self.assertIn('queue_declare', _elmnt)
        _kwargs = _elmnt.get('queue_declare')[1]
        self.assertEqual(_kwargs.get('queue'), self._queue_prd)
        self.assertEqual(_kwargs.get('arguments').get('x-max-priority'), 3)
        self.assertIsNone(_kwargs.get('arguments').get('x-dead-letter-exchange'))
        self.assertIsNone(_kwargs.get('arguments').get('x-dead-letter-routing-key'))
        self.assertTrue(_kwargs.get('durable'))
        self.assertEqual(_kwargs.get('callback'), _cn.on_main_queue_declared)

    def test_on_main_queue_declared_yes(self):
        # should call basic_qos
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _chan = _ChannelMock()
        _cn._channel = _chan
        _cn.on_main_queue_declared()
        self.assertEqual(len(_chan.calls), 1)
        # only call is to be 'queue declare' here
        _elmnt = _chan.calls.pop()
        self.assertIn('basic_qos', _elmnt)
        _kwargs = _elmnt.get('basic_qos')[1]
        self.assertEqual(_kwargs.get('prefetch_count'), 1)
        self.assertEqual(_kwargs.get('callback'), _cn.on_prefetch_set_ok)

    def test_on_main_queue_declared_only(self):
        # should disconnect
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='only',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _chan = _ChannelMock()
        _cn._channel = _chan
        _cn._connection = _ConnectionMock()
        _cn.on_main_queue_declared()
        self.assertEqual(len(_chan.calls), 1)
        # only call is to be 'queue declare' here
        _elmnt = _chan.calls.pop()
        self.assertIn('close', _elmnt)

    def test_on_prefetch_set_ok(self):
        # just call basic_consume
        # no ipc_queue_process should be called because we are not returning consumer tag here
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='only',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _chan = _ChannelMock()
        _cn._channel = _chan
        _cn.on_prefetch_set_ok()
        self.assertEqual(len(_chan.calls), 1)
        # only call is to be 'queue declare' here
        _elmnt = _chan.calls.pop()
        self.assertIn('basic_consume', _elmnt)
        _kwargs = _elmnt.get('basic_consume')[1]
        self.assertEqual(_kwargs.get('queue'), self._queue_prd)
        self.assertEqual(_kwargs.get('on_message_callback'), _cn.on_message)

    def test_on_consuming_cancel(self):
        # consuming tag has to be dropped
        # disconnect should be called
        class _MockedSingleRun(QueueConnectionProcess):
            _disconnect_called = False

            def disconnect(self):
                self._disconnect_called = True

        _cn = _MockedSingleRun(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='only',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _cn._consumer_tag = "test_tag"
        _cn.on_consuming_cancel()
        self.assertTrue(_cn._disconnect_called)
        self.assertIsNone(_cn._consumer_tag)

    def test_on_connection_closed_ok(self):
        # 1. Reason exception in _ipc_q_in
        # 2. _connection is to be None

        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        # Our error we emulate
        _err_text = "Test Normal Shutdown"
        _exc = pika.exceptions.ConnectionClosed(0, _err_text)

        _cns = _ConnectionMock()
        _cn._connection = _cns
        _cn.on_connection_closed(_cns, _exc)

        self.assertFalse(self._ipc_q_in.empty())
        while not self._ipc_q_in.empty():
            __item = self._ipc_q_in.get()
            self._ipc_q_in.task_done()
            self.assertIsInstance(__item, IpcExcMsg)
            self.assertEqual(__item.reply_code, 0)
            self.assertEqual(__item.type, type(_exc))

        # rest part of verifications may be done without multiprocessing
        _cn._connection = _cns
        self.assertFalse(_cns.ioloop.is_running)
        _cn.on_connection_closed(_cns, _exc)
        self.assertIsNone(_cn._connection)

    def test_on_connection_closed_error(self):
        # 1. Two exceptions in _ipc_q_in
        # 2. _connection is to be None
        # 1. Reason exception in _ipc_q_in
        # 2. _connection is to be None

        # to catch an exception we have to start separate process
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        # Error to be emulated
        _err_text = "Test Error"
        _exc = pika.exceptions.ConnectionClosed(406, _err_text)

        _cns = _ConnectionMock()

        # to emulate full-featured failure we may drop ioloop
        _cns.ioloop = None
        _cn._connection = _cns
        _cn.on_connection_closed(_cns, _exc)

        self.assertFalse(self._ipc_q_in.empty())

        _count_msgs = 0
        while not self._ipc_q_in.empty():
            _count_msgs += 1
            __item = self._ipc_q_in.get()
            self._ipc_q_in.task_done()
            self.assertIsInstance(__item, IpcExcMsg)
            self.assertIn(__item.type, [type(_exc), AttributeError])

            if __item.type == type(_exc):
                self.assertEqual(__item.reply_code, 406)

        self.assertEqual(_count_msgs, 2)
        self.assertIsNone(_cn._connection)

    def test_on_channel_closed(self):
        # should put an exception and call disconnect
        # _channel have to be dropped
        class _QueueConnectionDiscMock(QueueConnectionProcess):
            # we do not want full-featured 'disconnect' here,
            # so catching the fact it is called is enough
            def __init__(self,
                         connection,
                         params,
                         prefetch_count,
                         queue,
                         deads_disabled,
                         declare,
                         ipc_q_in,
                         ipc_q_out):
                super(_QueueConnectionDiscMock, self).__init__(
                    connection=connection,
                    params=params,
                    prefetch_count=prefetch_count,
                    queue=queue,
                    deads_disabled=deads_disabled,
                    declare=declare,
                    ipc_q_in=ipc_q_in,
                    ipc_q_out=ipc_q_out)
                self._disconnect_called = False

            def disconnect(self):
                self._disconnect_called = True

        _cn = _QueueConnectionDiscMock(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='only',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _chan = _ChannelMock()
        _cn._channel = _chan
        _err_text = "Test Channel Error"
        _exc = pika.exceptions.ChannelClosed(406, _err_text)
        _cn.on_channel_closed(_chan, _exc)

        _exc_cathed = False
        self.assertFalse(self._ipc_q_in.empty())
        while not self._ipc_q_in.empty():
            __item = self._ipc_q_in.get()
            self._ipc_q_in.task_done()
            self.assertIsInstance(__item, IpcExcMsg)

            if __item.type == type(_exc):
                self.assertEqual(__item.reply_code, 406)
                _exc_catched = True

        self.assertTrue(_exc_catched)
        self.assertTrue(_cn._disconnect_called)
        self.assertIsNone(_cn._channel)

    def test_run(self):
        # this is overrided method just to call 'connect'
        # it was designed due to 'multiprocessing.Process' inheritance
        # WARNING: please note we do inherit 'QueueConnectionProcess'
        #          since 'run' is overrided in 'QueueConnectionProcess'
        class _MockedSingleRun(QueueConnectionProcess):
            _connect_called = False
            pid = 0  # since we are debugging it

            def connect(self):
                self._connect_called = True

        _cn = _MockedSingleRun(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='only',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        self.assertFalse(_cn._connect_called)
        _cn.run()
        self.assertTrue(_cn._connect_called)

    def test_ipc_queue_process(self):
        # here we shall send our messages back to us

        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        # send out messages
        _conn = _ConnectionMock()
        _chan = _ChannelMock()
        _cn._channel = _chan
        _cn._connection = _conn
        _ipc_delay_before = _cn._ipc_delay
        _msglist = [IpcMessageResult(0, True, False, 0.1), IpcMessageResult(1, False, True, 0.05),
                    IpcMessageResult(2, True, True, 0.025), IpcMessageResult(3, False, False, 0.0125)]

        for _msgout in _msglist:
            self._ipc_q_out.put(_msgout)

        self.assertEqual(self._ipc_q_out.qsize(), 4)

        _cn.ipc_queue_process()
        self.assertEqual(self._ipc_q_out.qsize(), 0)

        # we have to catch basic_ack/basic_nack calls in channel
        self.assertEqual(len(_chan.calls), 4)
        # NOTE: since call stack is a list we have to pop and process calls in reverse order
        _msglist.reverse()
        for _msgin in _msglist:
            _call = _chan.calls.pop()

            _call_name = list(_call.keys())[0] # NOTE: for python 3 since 'dict_keys' is separate type there
                                               #       in contraverse to python 2
            _call_parms = _call.get(_call_name)[1]

            if _msgin.ack:
                self.assertEqual(_call_name, 'basic_ack')
            else:
                self.assertEqual(_call_name, 'basic_nack')
                self.assertEqual(_msgin.requeue, _call_parms.get('requeue'))

            self.assertEqual(_msgin.delivery_tag, _call_parms.get('delivery_tag'))

        # _ipc_delay is to be decreased
        self.assertTrue(_cn._ipc_delay < _ipc_delay_before)

        # 'ioloop' call for 'ipc_queue_process' is to be scheduled
        self.assertEqual(1, len(_conn.ioloop.call_buffer))
        self.assertEqual(_cn.ipc_queue_process, _conn.ioloop.call_buffer.pop().get('call'))

    def test_on_message(self):
        # message is to be sent to us
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _body = b"{'msg_body': 'Test body'}"
        _properties = {'property': 'Test property'}
        _delivery_tag = 0
        _method = _MockMethod(_delivery_tag)
        _chan = _ChannelMock()
        _cn.on_message(channel=_chan, method=_method, properties=_properties, body=_body)

        # we have to recieve the message and compare its properties
        self.assertFalse(self._ipc_q_in.empty())
        _cmsg = self._ipc_q_in.get()
        self.assertEqual(type(_cmsg), IpcMessage)
        self.assertEqual(_cmsg.delivery_tag, _delivery_tag)
        self.assertEqual(_cmsg.properties, _properties)
        self.assertEqual(_cmsg.body, _body)

    def test_report_msg_result_ack(self):
        # basic_ack should be called for channel
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _delivery_tag = 0
        _qmsg = IpcMessageResult(_delivery_tag, True, False)
        _chan = _ChannelMock()
        _cn._channel = _chan
        _cn.report_msg_result(rslt=_qmsg)

        # see basic_ack has been called with correct arguments
        self.assertEqual(len(_chan.calls), 1)
        # note on reverse stack analysis
        # third call
        _elmnt = _chan.calls.pop()
        self.assertIn('basic_ack', _elmnt)
        self.assertEqual(_elmnt.get('basic_ack')[1].get('delivery_tag'), _delivery_tag)

    def test_report_msg_result_nack_requeue(self):
        # basic_nack should be called for channel
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _delivery_tag = 1
        _qmsg = IpcMessageResult(_delivery_tag, False, True)
        _chan = _ChannelMock()
        _cn._channel = _chan
        _cn.report_msg_result(rslt=_qmsg)

        # see basic_ack has been called with correct arguments
        self.assertEqual(len(_chan.calls), 1)
        # note on reverse stack analysis
        # third call
        _elmnt = _chan.calls.pop()
        self.assertIn('basic_nack', _elmnt)
        self.assertEqual(_elmnt.get('basic_nack')[1].get('delivery_tag'), _delivery_tag)
        self.assertTrue(_elmnt.get('basic_nack')[1].get('requeue'))

    def test_report_msg_result_nack_no_requeue(self):
        # basic_nack should be called for channel
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _delivery_tag = 1
        _qmsg = IpcMessageResult(_delivery_tag, False, False)
        _chan = _ChannelMock()
        _cn._channel = _chan
        _cn.report_msg_result(rslt=_qmsg)

        # see basic_ack has been called with correct arguments
        self.assertEqual(len(_chan.calls), 1)
        # note on reverse stack analysis
        # third call
        _elmnt = _chan.calls.pop()
        self.assertIn('basic_nack', _elmnt)
        self.assertEqual(_elmnt.get('basic_nack')[1].get('delivery_tag'), _delivery_tag)
        self.assertFalse(_elmnt.get('basic_nack')[1].get('requeue'))

    def test_stop(self):
        # should be called disconnect and close_all_ipc_q
        class _QueueConnectionStopMock(QueueConnectionProcess):
            def __init__(self, *args, **kwargs):
                super(_QueueConnectionStopMock, self).__init__(*args, **kwargs)
                self._disconnect_call = None
                self._close_all_ipc_q_call = None
                self.__counter = 0

            def disconnect(self):
                self._disconnect_call = self.__counter
                self.__counter += 1

            def _close_all_ipc_q(self):
                self._close_all_ipc_q_call = self.__counter
                self.__counter += 1

        _cn = _QueueConnectionStopMock(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _cn._stop()
        self.assertEqual(_cn._disconnect_call, 0)
        self.assertEqual(_cn._close_all_ipc_q_call, 1)

    def test_close_all_ipc_q(self):
        # _ipc_q_out should be empty and closed even if we pass anything there
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        self.assertFalse(self._ipc_q_in.joined)
        self.assertFalse(self._ipc_q_out.joined)

        for __cnt in range(0, 200):
            self._ipc_q_out.put("anything %d" % __cnt)
            self._ipc_q_in.put("%d nothing" % __cnt)

        _cn._close_all_ipc_q()
        self.assertFalse(self._ipc_q_in.empty()) # YES, IT IS TRUE, 
                                                 # we have to close and join _in_ queue on our side and the child 
                                                 # have to do the same with our _out_ queue (_in_ for child)

        ## NOTE: we are not call .join() for any queue

    # disconnection tests
    # just verify we call the methods in the right order
    # regarding the conditions
    # we may use the same Mock for channel and connection
    # just collecting the calls
    def test_disconnect_no_connection(self):
        # no calls to be done for channel
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _cn._connection = None
        _cn._channel = _ChannelMock()
        _cn.disconnect()
        self.assertEqual(len(_cn._channel.calls), 0)

    def test_disconnect_consumer_tag(self):
        # channel is now None
        # consumer tag is to be dropped
        # 'close' is to be done for connection
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _cn._connection = _ChannelMock()
        _cn._channel = None
        _cn._consumer_tag = 'test_tag'
        _cn._connection.is_closed = False
        _cn._connection.is_closing = False
        _cn.disconnect()
        self.assertIsNone(_cn._consumer_tag)
        self.assertTrue(_cn._connection.is_open)

    def test_disconnect_consumer_chan(self):
        # the same as above but channel given
        # no calls for connection but 'basic_cancel' for channel
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _cn._connection = _ConnectionMock()
        _cn._channel = _ChannelMock()
        _cn._consumer_tag = 'test_tag'
        _cn.disconnect()
        # here the tag should not be dropped, it is done in callback
        self.assertEqual(_cn._consumer_tag, 'test_tag')
        self.assertEqual(len(_cn._channel.calls), 1)
        _elmnt = _cn._channel.calls.pop()
        self.assertIn('basic_cancel', _elmnt)
        _kwargs = _elmnt.get('basic_cancel')[1]
        self.assertEqual(_kwargs.get('consumer_tag'), 'test_tag')
        self.assertEqual(_kwargs.get('callback'), _cn.on_consuming_cancel)
        self.assertTrue(_cn._connection.is_open)

    def test_disconnect_channel(self):
        # no consumer tag, but channel is given
        # 'close' should be call for channel
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _cn._connection = _ConnectionMock()
        _cn._channel = _ChannelMock()
        _cn._consumer_tag = None
        _cn._connection.close()
        self.assertTrue(_cn._connection.is_closed)
        _cn.disconnect()
        self.assertIsNone(_cn._consumer_tag)
        self.assertTrue(_cn._connection.is_closed)
        self.assertEqual(len(_cn._channel.calls), 1)
        _elmnt = _cn._channel.calls.pop()
        self.assertIn('close', _elmnt)

    def test_disconnect_closing_connection(self):
        # NOTE: to emulate this situation we are using _ChannelMock instead of _ConnectionMock
        #       to make sure we call nothing

        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _cn._connection = _ChannelMock()  # WARNING: YES, THIS IS TRUE, see NOTE above
        _cn._channel = None
        _cn._consumer_tag = None
        _cn._connection.is_closed = True
        _cn._connection.is_closing = False
        _cn.disconnect()
        self.assertIsNone(_cn._consumer_tag)
        self.assertEqual(len(_cn._connection.calls), 0)

    def test_disconnect_closed_connection(self):
        # the same as above
        _cn = QueueConnectionProcess(
            connection=_ConnectionMock,
            params=self._params,
            prefetch_count=1,
            queue=self._queue_prd,
            deads_disabled=False,
            declare='yes',
            ipc_q_in=self._ipc_q_out,
            ipc_q_out=self._ipc_q_in)

        _cn._connection = _ChannelMock()
        _cn._channel = None
        _cn._consumer_tag = None
        _cn._connection.is_closed = False
        _cn._connection.is_closing = True
        _cn.disconnect()
        self.assertIsNone(_cn._consumer_tag)
        self.assertEqual(len(_cn._connection.calls), 0)
