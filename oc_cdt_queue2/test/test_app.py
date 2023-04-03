import unittest
from oc_cdt_queue2.queue_application import QueueApplication
import pika
import logging
import argparse

# to get rid of logging output from imported classes
logging.getLogger().propagate = False
logging.getLogger().disabled = True


class QueueApplicationTest(unittest.TestCase):
    def test_setup(self):
        _app = QueueApplication()
        _app.setup(reconnect=True)
        self.assertTrue(_app.reconnect)
        _app.setup(reconnect=False)
        self.assertFalse(_app.reconnect)

    def test_setup_from_args(self):
        _app = QueueApplication()
        _prs = _app.prepare_parser()
        _prs = _app.basic_args(_prs)
        _args = _prs.parse_args(['--reconnect'])
        _app.setup_from_args(_args)
        self.assertTrue(_app.reconnect)

    def test_prepare_parser(self):
        self.assertIsInstance(QueueApplication().prepare_parser(), argparse.ArgumentParser)

    def __setup_app(self, app):
        _prs = app.prepare_parser()
        _prs = app.basic_args(_prs)
        _args = _prs.parse_args('--amqp-url amqp://127.0.0.1 --queue test.input'.split(' '))
        app.setup_from_args(_args)
        return app

    def test_connect_and_run_os_error(self):
        ### OSerror occur in 'connect'
        ## connect called
        ## exception raised
        class _MockAppConnectAndRun(QueueApplication):
            def __init__(self, *args, **kwargs):
                super(_MockAppConnectAndRun, self).__init__(*args, **kwargs)
                self.connect_called = False

            def connect(self):
                self.connect_called = True
                raise OSError("Test memory leak")

        _app = _MockAppConnectAndRun()
        _app = self.__setup_app(_app)
        self.assertFalse(_app.connect_called)

        with self.assertRaises(OSError):
            _app._connect_and_run()

        self.assertTrue(_app.connect_called)

    def test_connect_and_run_connect_error(self):
        ### Any other exception occur in 'connect'
        ## connect called
        ## exception not raised, but return value is 2
        class _MockAppConnectAndRun(QueueApplication):
            def __init__(self, *args, **kwargs):
                super(_MockAppConnectAndRun, self).__init__(*args, **kwargs)
                self.connect_called = False

            def connect(self):
                self.connect_called = True
                raise pika.exceptions.ConnectionClosedByBroker(406, "Test connection error")

        _app = _MockAppConnectAndRun()
        _app = self.__setup_app(_app)
        self.assertFalse(_app.connect_called)

        self.assertEqual(2, _app._connect_and_run())
        self.assertTrue(_app.connect_called)

    def test_connect_and_run_run_error(self):
        ### exception occur in 'run'
        ## connect called
        ## disconnect called
        ## exception not raised, return value is 1
        class _MockAppConnectAndRun(QueueApplication):
            def __init__(self, *args, **kwargs):
                super(_MockAppConnectAndRun, self).__init__(*args, **kwargs)
                self._counter = 0
                self.connect_call = None
                self.disconnect_call = None
                self.run_call = None

            def run(self):
                self.run_call = self._counter
                self._counter += 1
                raise Exception("Test something wrong")

            def connect(self):
                self.connect_call = self._counter
                self._counter += 1

            def disconnect(self):
                self.disconnect_call = self._counter
                self._counter += 1

        _app = _MockAppConnectAndRun()
        _app = self.__setup_app(_app)
        self.assertIsNone(_app.connect_call)
        self.assertIsNone(_app.disconnect_call)
        self.assertIsNone(_app.run_call)

        self.assertEqual(1, _app._connect_and_run())
        self.assertEqual(0, _app.connect_call)
        self.assertEqual(1, _app.run_call)
        self.assertEqual(2, _app.disconnect_call)
        self.assertEqual(3, _app._counter)

    def test_connect_and_run_declare_only(self):
        ### everything OK
        ## declare == 'only' - return value is 0
        class _MockAppConnectAndRun(QueueApplication):
            def __init__(self, *args, **kwargs):
                super(_MockAppConnectAndRun, self).__init__(*args, **kwargs)
                self._counter = 0
                self.connect_call = None
                self.disconnect_call = None
                self.run_call = None
                self.queue_declare = 'only'

            def run(self):
                self.run_call = self._counter
                self._counter += 1

            def connect(self):
                self.connect_call = self._counter
                self._counter += 1

            def disconnect(self):
                self.disconnect_call = self._counter
                self._counter += 1

        _app = _MockAppConnectAndRun()
        _app = self.__setup_app(_app)
        self.assertIsNone(_app.connect_call)
        self.assertIsNone(_app.disconnect_call)
        self.assertIsNone(_app.run_call)

        self.assertEqual(0, _app._connect_and_run())
        self.assertEqual(0, _app.connect_call)
        self.assertEqual(1, _app.run_call)
        self.assertEqual(2, _app.disconnect_call)
        self.assertEqual(3, _app._counter)

    def test_connect_and_run_reconnect_true(self):
        ### everything OK
        ## reconnect is true - return value is 3
        class _MockAppConnectAndRun(QueueApplication):
            def __init__(self, *args, **kwargs):
                super(_MockAppConnectAndRun, self).__init__(*args, **kwargs)
                self._counter = 0
                self.connect_call = None
                self.disconnect_call = None
                self.run_call = None
                self.queue_declare = 'yes'

            def run(self):
                self.run_call = self._counter
                self._counter += 1

            def connect(self):
                self.connect_call = self._counter
                self._counter += 1

            def disconnect(self):
                self.disconnect_call = self._counter
                self._counter += 1
        
        _app = _MockAppConnectAndRun()
        _app = self.__setup_app(_app)
        _app.reconnect = True
        self.assertIsNone(_app.connect_call)
        self.assertIsNone(_app.disconnect_call)
        self.assertIsNone(_app.run_call)

        self.assertEqual(3, _app._connect_and_run())
        self.assertEqual(0, _app.connect_call)
        self.assertEqual(1, _app.run_call)
        self.assertEqual(2, _app.disconnect_call)
        self.assertEqual(3, _app._counter)

    def test_connect_and_run_reconnect_false(self):
        ### everything OK
        ## no reconnect
        ## declare is not 'only'
        ## return value is 0
        ## calls : connect, run, disconnnect
        class _MockAppConnectAndRun(QueueApplication):
            def __init__(self, *args, **kwargs):
                super(_MockAppConnectAndRun, self).__init__(*args, **kwargs)
                self._counter = 0
                self.connect_call = None
                self.disconnect_call = None
                self.run_call = None
                self.queue_declare = 'yes'

            def run(self):
                self.run_call = self._counter
                self._counter += 1

            def connect(self):
                self.connect_call = self._counter
                self._counter += 1

            def disconnect(self):
                self.disconnect_call = self._counter
                self._counter += 1

        _app = _MockAppConnectAndRun()
        _app = self.__setup_app(_app)
        _app.reconnect = False
        self.assertIsNone(_app.connect_call)
        self.assertIsNone(_app.disconnect_call)
        self.assertIsNone(_app.run_call)

        self.assertEqual(0, _app._connect_and_run())
        self.assertEqual(0, _app.connect_call)
        self.assertEqual(1, _app.run_call)
        self.assertEqual(2, _app.disconnect_call)
        self.assertEqual(3, _app._counter)

    def test_main_url_wrong(self):
        class _MockAppMain(QueueApplication):
            def __init__(self, *args, **kwargs):
                super(_MockAppMain, self).__init__(*args, **kwargs)
                self._counter = 0
                self.setup_from_args_call = None

            def setup_from_args(self, args=None):
                self.setup_from_args_call = self._counter
                self._counter += 1
                raise(TypeError("Test wrong args"))

        _app = _MockAppMain()
        self.assertIsNone(_app.setup_from_args_call)
        self.assertEqual(2, _app.main(['--amqp-url', 'amqp://blablabla']))
        self.assertEqual(0, _app.setup_from_args_call)
        self.assertEqual(1, _app._counter)

    def test_main_reconnect(self):
        class _MockAppMain(QueueApplication):
            def __init__(self, *args, **kwargs):
                super(_MockAppMain, self).__init__(*args, **kwargs)
                self._counter = 0
                self.setup_from_args_call = None
                self.connect_and_run_call = None

            def setup_from_args(self, args=None):
                self.setup_from_args_call = self._counter
                self._counter += 1

            def _connect_and_run(self):
                self.connect_and_run_call = self._counter
                self._counter += 1
                return 3 - self._counter

        _app = _MockAppMain()
        _app.reconnect = True
        self.assertIsNone(_app.setup_from_args_call)
        self.assertIsNone(_app.connect_and_run_call)
        self.assertEqual(0, _app.main(cmdline=['--amqp-url', 'amqp://blablabla', '--reconnect']))

        self.assertEqual(0, _app.setup_from_args_call)
        # _connect_and_run is to be called twice, so value is not 1 but 2
        self.assertEqual(2, _app.connect_and_run_call)
        self.assertEqual(3, _app._counter)

    def test_main_single_pass(self):
        class _MockAppMain(QueueApplication):
            def __init__(self, *args, **kwargs):
                super(_MockAppMain, self).__init__(*args, **kwargs)
                self._counter = 0
                self.setup_from_args_call = None
                self.connect_and_run_call = None

            def setup_from_args(self, args=None):
                self.setup_from_args_call = self._counter
                self._counter += 1

            def _connect_and_run(self):
                self.connect_and_run_call = self._counter
                self._counter += 1
                return 3 - self._counter

        _app = _MockAppMain()
        _app.reconnect = False
        self.assertIsNone(_app.setup_from_args_call)
        self.assertIsNone(_app.connect_and_run_call)
        self.assertEqual(0, _app.main(cmdline=['--amqp-url', 'amqp://blablabla', '--reconnect']))

        self.assertEqual(0, _app.setup_from_args_call)
        # _connect_and_run is to be called once here
        self.assertEqual(1, _app.connect_and_run_call)
        self.assertEqual(2, _app._counter)
