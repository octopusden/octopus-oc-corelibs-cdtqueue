#!/usr/bin/env python

# this thing should have minimum dependencies
import pika
import json
from .queue_base import QueueBase
from .queue_connection_prcs import QueueConnectionProcess
from .ipc_messages import IpcMessage
from .ipc_messages import IpcMessageResult
from .ipc_messages import IpcExcMsg
import logging
import time
import multiprocessing

# this is a version-specific import of JoinableQueue due to Python3 features
# see https://bugs.python.org/issue21367 for details
try:
    from multiprocessing import JoinableQueue
except ImportError:
    from multiprocessing.queues import JoinableQueue

from sys import version_info


class QueueServer(QueueBase):
    default_prefetch_count = 1      # Default prefetch count

    # One can re-define it for debugging purposes or to implement another protocol driver
    # See queue_loopback.py as an somewhat dirty example
    _Connection = pika.SelectConnection
    _JoinableQueue = staticmethod(JoinableQueue)
    _QueueConnectionProcess = staticmethod(QueueConnectionProcess)

    def __init__(self, *argv, **argp):
        super(QueueServer, self).__init__(*argv, **argp)
        self._nacks = 0
        self.counter_messages = 0
        self.counter_good = 0
        self.counter_bad = 0
        self.prefetch_count = self.default_prefetch_count
        self.max_sleep = 16
        # additional process for asynchronious connection
        self._connection_prcs = None
        self._ipc_q_in = None
        self._icp_q_out = None
        self._stop = False
        self._ipc_delay = 0.2     
        self._terminate_delay = 3

    def basic_args(self, parser=None):
        """
        Call this to add specific AMQP arguments to your argparse parser.
        Uses internal argparse object by default. Should be used with setup_from_args()
        See QueueBase for details.

        :param parser: argparse object
        :returns: Modified parser
        """
        parser = super(QueueServer, self).basic_args(parser)
        parser.add_argument('--prefetch-count', help='Pre-fetch count',
                            default=self.default_prefetch_count, type=int)
        return parser

    def setup_from_args(self, args=None):
        """
        This is an aid for your argparse-driven commandline application.
        Converts argparse resulting namespace to ConnectionParameters you got
        by parsing arguments using basic_args()-extended parser to connection
        parameters.

        :param args:    Argparse namespace, self.args used if not specified
        :returns:    args used for setup
        :raises:    See setup() method
        """
        args = super(QueueServer, self).setup_from_args(args)
        self.setup(prefetch_count=args.prefetch_count)
        return args

    def setup(self, *args, **argv):
        """
        Sets things up. Takes all argument QueueBase.setup() accepts and few additional

        :param routing_key:    Routing key. Uses queue name as routing key if not specified
        :param exchange:    RabbitMQ exchange name. Uses default exchange if not specified
        :param priority:    Messages priority for sending

        All the rest params are the same as QueueBase.setup(). If used without url - no
        basic parameters touched, only client-specific are set
        """
        prefetch_count = argv.pop(
            'prefetch_count', self.default_prefetch_count)
        if prefetch_count is not None:
            self.prefetch_count = prefetch_count

        if len(args) > 0 or 'url' in argv:
            super(QueueServer, self).setup(*args, **argv)
        elif len(argv) > 0:
            raise(TypeError('Unexpected arguments to setup() call'))

    def on_ack(self, body, properties):
        pass

    def on_nack(self, body, properties, result):
        pass

    def on_message(self, data, properties):
        """
        Redefine this to add your own processing
        """
        pass

    def on_message_raw(self, body, properties):
        """
        Redefine this to do your own raw message processing
        """

        if properties.content_type != 'application/json':
            raise ValueError("invalid message: content-type should be application/json")

        data = json.loads(body)
        return self.on_message(data, properties)

    def _sleep_on_nack(self):
        """
        Sleep between messages if nack occured
        Used if deads are disabled only
        """
        sleep = 2 ** self._nacks
        if sleep > self.max_sleep:
            sleep = self.max_sleep
        if sleep > 0:
            time.sleep(sleep)

    def _set_ipc_delay(self, delay):
        """
        Check if we may reduce _ipc_q check interval
        :param delay: new delay
        :type delay: float
        """
        logging.debug("IPC delay set call with argument %f" % delay)

        if delay > 0.0 and delay < self._ipc_delay:
            self._ipc_delay = delay
            logging.debug("Ipc delay is set to %f sec" % self._ipc_delay)

    def _increase_ipc_delay(self):
        """
        Increase _ipc_delay to reduce CPU usage when queue is emty for a long time
        """
        _new_delay = self._ipc_delay * 1.5

        # the optimal check for an empty queue is 0.2 sec
        if _new_delay > 0.2:
            return

        self._ipc_delay = _new_delay
        logging.debug("Ipc delay is increased to %f sec" % self._ipc_delay)

    def _report_message_result(self, delivery_tag, ack=False, requeue=True, time_delta=0):
        """
        Report message processing result
        :param delivery_tag: message delivery tag
        :type delivery_tag: int
        :param ack: do the ack
        :type ack: boolean
        :param requeue: do_requeue
        :type requeue: boolean
        :param time_delta: time took to process message
        :type time_delta: float
        """
        self._ipc_q_out.put(IpcMessageResult(
            delivery_tag=delivery_tag, ack=ack, requeue=requeue, time_delta=time_delta))

    def _on_nack(self, body, properties, result):
        """
        Background actions if nack occured
        :param body: message body
        :type body: bytes
        :param properties: message properties
        :type properties: pika.Spec.BasicProperties
        :param result: message processing result
        :type result: Exception
        """
        logging.exception(result)
        self.counter_bad += 1
        self._nacks += 1
        self.on_nack(body, properties, result)
        if self.max_sleep > 0 and self.deads_disabled:
            self._sleep_on_nack()

    def _on_ack(self, body, properties):
        """
        Background actions if ack occured
        :param body: message body
        :type body: bytes
        :param properties: message properties
        :type properties: pika.Spec.BasicProperties
        """
        self.counter_good += 1
        self._nacks = 0
        self.on_ack(body, properties)

    def __debug_message(self, properties, body):
        logging.debug("stats: total %d good %d bad %d", self.counter_messages, self.counter_good, self.counter_bad)
        logging.debug("Received message: %s", body)
        logging.debug("Message properties - content-type: %s (encoding: %s) type: %s priority: %s",
                      properties.content_type, properties.content_encoding, properties.type, properties.priority)
        if hasattr(properties, 'headers') and properties.headers and len(properties.headers) > 0:
            logging.debug("Message headers: %s", repr(properties.headers))

    def _process_message(self, delivery_tag, properties, body):
        """
        Process message
        :param delivery_tag: delivery tag
        :type delivery_tag: int
        :param properties: properties
        :type properties: pika.BasicProperties
        :param body: message body
        :type body: bytes
        """
        self.counter_messages += 1
        self.__debug_message(properties, body)

        try:
            _start_t = time.time()
            self.on_message_raw(body, properties)
            _delta_t = time.time() - _start_t
            logging.debug("Message processing took %f" % _delta_t)
            self._set_ipc_delay(_delta_t)
            self._report_message_result(delivery_tag=delivery_tag, ack=True, requeue=False, time_delta=_delta_t)
            self._on_ack(body, properties)
        except Exception as e:
            # this block should be actived only if message processing result throws an exception
            # so we have to nack it unconditionally
            self._report_message_result(delivery_tag=delivery_tag, ack=False, requeue=self.deads_disabled)
            self._on_nack(body, properties, result=e)

    def run(self):
        """
        Run the main loop
        """
        logging.debug("Running")

        if not self._connection_prcs:
            logging.error("Running attempt with empty connection process")
            return

        while True:
            if self._stop:
                logging.debug("Stop flag is set somehow, breaking main loop")
                break

            if not self._connection_prcs.is_alive():
                logging.debug("Connection prcs is not active, breaking main loop")
                break

            # sleeping if inbound queue is empty
            if self._ipc_q_in.empty():
                time.sleep(self._ipc_delay)
                # if queue is emty for a long time
                # we may sleep much more next time
                # to decrease CPU usage
                self._increase_ipc_delay()
                continue

            self._prcs_ipc_q_pop()

        self.disconnect()

    def _prcs_ipc_q_pop(self, do_process=True):
        """
        Pop single message from queue and process it
        :param do_process: do actually process the message
        :type do_process: boolean
        """
        __msg = self._ipc_q_in.get()
        self._ipc_q_in.task_done()
        logging.debug("recieved from ipc_q_in: item_type : %s" % type(__msg))

        if isinstance(__msg, IpcExcMsg):
            logging.debug("Exception received, type: %s" % str(__msg.type))

            if __msg.reply_code is not None and __msg.reply_code in [0, 200]:
                # normal shutdown
                logging.info(repr(__msg))
            else:
                logging.exception(__msg)
            return

        if not do_process:
            # do not actually porcess the message if we are shutting down
            return

        if isinstance(__msg, IpcMessage):
            self._process_message(__msg.delivery_tag, __msg.properties, __msg.body)

    def _prcs_ipc_q(self, do_process=True):
        """
        Process all messages in ipc_queue_in
        :param do_process: do actually process the messages
        :type do_process: boolean
        """
        while not self._ipc_q_in.empty():
            self._prcs_ipc_q_pop(do_process=do_process)

    def connect(self):
        """
        Connect to RabbitMQ
        """
        logging.debug("Creating connection process")

        if self._connection_prcs:
            logging.error("Connection process is not destroyed while trying to connect!")
            self.disconnect()

        # creating queues for interprocess communication
        # may be safely re-created without additional checks
        # old ipc_queues will be garbage-collected
        self._stop = False
        self._ipc_q_in = self._JoinableQueue()
        self._ipc_q_out = self._JoinableQueue()
        self._connection_prcs = self._QueueConnectionProcess(
            connection=self._Connection,
            params=self.connection_parameters,
            prefetch_count=self.prefetch_count,
            queue=self.queue,
            deads_disabled=self.deads_disabled,
            declare=self.queue_declare,
            ipc_q_out=self._ipc_q_in,  # note on queue direction across each other
            ipc_q_in=self._ipc_q_out
        )

        logging.debug("Connection subprocess is ready to start")
        self._connection_prcs.start()
        logging.info("Connection subprocess started")

    def stop(self):
        """
        Scheduler normal stopping
        Do not call it from main loop
        """
        self._stop = True

    def disconnect(self):
        """
        Disconnect.
        """
        # since this method is called from '__del__'
        # we need to verify if 'logging' oject still exist
        # before log anything
        if logging is not None: logging.debug("Closing the connection process")

        # catch chid process finish
        if not self._connection_prcs:
            if logging is not None: logging.debug("Disconnection has been done already")
            return

        if self._connection_prcs.is_alive():
            if logging is not None: logging.debug("Connection process is alive, stopping it normally")
            # this 'if' should not work when RabbitMQ connection was reset
            # by any network reason since the process is to be terminated itself
            self._ipc_q_out.put(IpcExcMsg("DisconnectCommand"))
            time.sleep(self._terminate_delay)

        # if connection subprocess is failed to disconnect then terminate it hardcorely
        if self._connection_prcs.is_alive():
            if logging is not None: logging.error("Normal disconnection has been failed within %d seconds, terminating hardly" % self._terminate_delay)
            self._connection_prcs.terminate()

        # close/join the process and free all resources got by it
        try:
            if version_info.major >= 3 and version_info.minor >= 7:
                self._connection_prcs.close()
            else:
                self._connection_prcs.join()
        except Exception as e:
            if logging is not None: logging.exception(e)
        finally:
            self._connection_prcs = None

        # catch and process all messages from interprocess queue
        if logging is not None: logging.debug("ipc queue size is: %s" % self._ipc_q_in.qsize())
        self._prcs_ipc_q(do_process=False)

        # closing all interprocess queues
        if self._ipc_q_in:
            self._ipc_q_in.close()
            self._ipc_q_in.join_thread()
            self._ipc_q_in = None

        if self._ipc_q_out:
            # queue should be closed by receiver only
            # self._ipc_q_out.close()
            self._ipc_q_out.join_thread()
            self._ipc_q_out = None
