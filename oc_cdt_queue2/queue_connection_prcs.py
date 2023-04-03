#!/usr/bin/env python2.7

import pika
import multiprocessing
import logging
from .ipc_messages import IpcMessage
from .ipc_messages import IpcMessageResult
from .ipc_messages import IpcExcMsg

"""
This is helper proxy-type class which is to be run in separate process
It is responsible for direct rabbit communication while main process
handles the message in parallel.
"""


class QueueConnectionProcess(multiprocessing.Process):
    """
    Process-based class to hold RabbitMQ connection.
    Should be started as separate process-like thread.
    """

    _ipc_delay = 0.2    # pause between interporcess inbout queue checks, in seconds, float

    _Connection = None
    _connection = None
    _channel = None
    _ipc_q_out = None
    _ipc_q_in = None
    _connection_params = None
    _rmq_main = None
    _rmq_deads = None
    _declare = None
    _prefetch_count = None
    _consumer_tag = None

    def __init__(self,
                 connection,
                 params,
                 prefetch_count,
                 queue,
                 deads_disabled,
                 declare,
                 ipc_q_out,
                 ipc_q_in):
        """
        The process initialization.
        :param connection: connection class reference
        :type connection: pika.SelectConnection class, not instance of
        :param params: connection parameters 
        :type params: pika.URLParams
        :param prefetch_count: how many messages to prefetch
        :type prefetch_count: int
        :param deads_disabled: disable deads queues routing
        :type deads_disabled: boolean
        :param declare: tristate queue declare parameter ('yes', 'no', 'only')
        :type declare: str
        :param ipc_q_out: python queue for messages and exceptions
        :type ipc_q_out: multiprocessing.JoinableQueue
        :param ipc_q_in: python queue for results
        :type ipc_q_in: multiprocessing.JoinableQueue
        """
        if not connection:
            raise TypeError("Please specify the connection class")

        self._Connection = connection
        self._connection = None
        self._channel = None
        self._ipc_q_out = ipc_q_out
        self._ipc_q_in = ipc_q_in

        if not isinstance(params, pika.URLParameters):
            raise TypeError("Incorrect type for connection parameters, should be pika.URLParameters")

        self._connection_params = params

        if not isinstance(prefetch_count, int):
            raise TypeError("Incorrect type for prefetch_count value")

        if not queue:
            raise ValueError("Queue name is mandatory")

        # define deads queue name if enabled
        # otherwise set it to None, this will be also used to detect 'deads_disabled' value
        self._rmq_main = queue
        self._rmq_deads = None

        if not deads_disabled:
            self._rmq_deads = queue

            if '.' in self._rmq_deads:
                # pick last suffix off to replace it with '.deads'
                self._rmq_deads = queue.rsplit('.', 1)[0]

            self._rmq_deads = '.'.join([self._rmq_deads, 'deads'])
            logging.debug("Deads queue name is: %s" % self._rmq_deads)

        if not isinstance(declare, str):
            raise TypeError("'declare' parameter has wrong type")

        if declare not in ['yes', 'no', 'only']:
            raise ValueError("Unsupported 'declare' value: %s" % declare)

        self._declare = declare
        self._prefetch_count = prefetch_count
        self._consumer_tag = None

        super(QueueConnectionProcess, self).__init__()
        logging.debug("Initialization done")

    #### BEG: CONNECT-RELATED CALLS AND CALLBACKS
    def connect(self):
        """
        Connect command
        """
        logging.info("Trying to connect")

        # creating connection itself
        self._connection = self._Connection(
            parameters=self._connection_params,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

        # try to start ioloop
        # transfer exceptoin to parrent if it fails, then destroy ourself
        try:
            self._connection.ioloop.start()
        except Exception as e:
            self._put_out_exception(e)
            self.disconnect()

    def on_connection_open(self, connection):
        """
        Callback for successful connection open
        :param connection: connection just opened
        :type connection: pika.SelectConnection
        """
        # define channel
        logging.debug("Connection open success")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def _put_out_exception(self, err):
        """
        This is workaround for bug in transer exception instance via IPC queue.
        The bug is that _ipc_q.get() tries to call a constructor (__init__) unexpectedly
        with no arguments. This is illegal and 
        :param err: error to log in parent
        :type err: Exception
        """
        if hasattr(err, 'reply_code') and err.reply_code in (0, 200):
            logging.info(repr(err))
        else:
            logging.exception(err)

        self._ipc_q_out.put(IpcExcMsg(err))

    def on_connection_open_error(self, connection, err):
        """
        Callback for connection open error
        Destroy ourself if it is
        :param connection: connection failed to open
        :type connection: pika.SelectConnection
        :param err: error description
        :type err: Exception
        """
        logging.debug("Connection was failed to open: %s" % type(err))
        self._put_out_exception(err)

        # This is the main error. Nothing was done yet, so simply stop main loop
        try:
            self._connection.ioloop.stop()
        except Exception as e:
            self._put_out_exception(e)

        self._connection = None

    def on_channel_open(self, channel):
        """
        Callback for successful channel opening
        :param channel: channel opened
        :type channel: pika.Channel
        """
        logging.debug("Channel opened")
        self._channel = channel
        logging.debug("Adding on_close and on_cancel callback")
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._channel.add_on_cancel_callback(self.disconnect)

        # declaring queues if we are forced to do so
        if self._declare == 'no':
            logging.debug("Declaration of queues is disabled, try to bind to an existing queue %s" % self._rmq_main)
            self.on_main_queue_declared()
            return

        if not self._rmq_deads:
            # assming deads queue creation OK and bind to main queue or disconnect
            self.on_deads_queue_bind()
            return

        # if deads are disabled - this will never proceed
        logging.debug("Declaring deads exchange")
        self._channel.exchange_declare(exchange=self._rmq_deads,
                                       exchange_type='direct',
                                       durable=True,
                                       callback=self.on_deads_exchange_declared)

    def on_deads_exchange_declared(self, frame=None):
        """
        Callback for successful deads exchange declaration
        :param frame: result frame from pika, unused
        :type frame: pika.Frame
        """
        logging.debug("Declared deads exchange, %s queue declaring." % self._rmq_deads)

        self._channel.queue_declare(queue=self._rmq_deads,
                                    durable=True,
                                    arguments={'x-max-priority': 3},
                                    callback=self.on_deads_queue_declared)

    def on_deads_queue_declared(self, frame=None):
        """
        Callback for successful deads queue declaration
        :param frame: result frame from pika, unused
        :type frame: pika.Frame
        """

        # assuming this will never called if we do not declare deads
        logging.debug("Deads queue declared, binding it to deads exchange")
        self._channel.queue_bind(queue=self._rmq_deads,
                                 exchange=self._rmq_deads,
                                 routing_key=self._rmq_deads,
                                 callback=self.on_deads_queue_bind)

    def on_deads_queue_bind(self, frame=None):
        """
        Callback for successful deads queue bind
        May be called if 'deads' are disabled
        :param frame: result frame from pika, unused
        :type frame: pika.Frame
        """
        logging.debug("Declaring main queue %s" % self._rmq_main)

        __rmq_args = {'x-max-priority': 3}

        if self._rmq_deads:
            __rmq_args['x-dead-letter-exchange'] = self._rmq_deads
            __rmq_args['x-dead-letter-routing-key'] = self._rmq_deads

        self._channel.queue_declare(queue=self._rmq_main,
                                    durable=True,
                                    arguments=__rmq_args,
                                    callback=self.on_main_queue_declared)

    def on_main_queue_declared(self, frame=None):
        """
        Callback for successful queue declaration.
        May be called if queue is not to be declared'
        :param frame: result frame from pika, unused
        :type frame: pika.Frame
        """

        if self._declare == 'only':
            logging.debug("Declare is set to 'only' value, stopping")
            self.disconnect()
            return

        logging.debug("Main queue %s has been declared, setting prefetch_count to %d" % (self._rmq_main, self._prefetch_count))
        self._channel.basic_qos(prefetch_count=self._prefetch_count, callback=self.on_prefetch_set_ok)

    def on_prefetch_set_ok(self, frame=None):
        """
        Callback for basic_qos with prefetch count
        :param frame: result frame from pika, unused
        :type frame: pika.Frame
        """

        # now we are ready to consuming messages
        # this method should never be called if we are set with --declare='only'
        # so checking it is skipped
        # This function can raise an exception, so 'on_channel_closed' will be run asynchroniously
        # with that exception as 'reason' argument
        # This case consumer tag may be empty or None
        self._consumer_tag = self._channel.basic_consume(queue=self._rmq_main, on_message_callback=self.on_message)

        # if we are not failed with 'basic_consume' - scheduler _ipc_q_in checks
        if self._consumer_tag:
            self.ipc_queue_process()
    #### END: CONNECT-RELATED CALLS AND CALLBACKS

    #### BEG: DISCONNECT-RELATED CALLS AND CALLBACKS
    def disconnect(self):
        """
        Disconnecting from pika if not done yet
        """

        # check if connection is closed already.
        # do nothing if so
        if not self._connection:
            logging.debug("Already disconnected, nothing to do")
            return

        # if consuming was set to 'on' then first need to cancel it
        if self._consumer_tag:
            logging.debug("Consuming is to be stopped")

            if self._channel:
                self._channel.basic_cancel(consumer_tag=self._consumer_tag, callback=self.on_consuming_cancel)
                # when stopping will be done we have to call this method agian
                # but with _consumer_tag droped
                # it will be done from callback
                return

            # otherwise consumer tag is to be simply dropped since consuming start was failed
            self._consumer_tag = None

        # if self._channel is not closed yet: close it
        # in the callback this method will be called again
        # but this condition will be passed further
        if self._channel:
            self._channel.close()
            return

        # may be we are asked to drop connection already
        # but callback was not called yet
        # this case we have nothing to do also
        if self._connection.is_closing or self._connection.is_closed:
            logging.debug("Connection is now closing, nothing to do")
            return

        logging.debug("Connection is alive, stopping it")
        self._connection.close()

        # rest part is to be done in callback assigned while connection has been created

    def on_consuming_cancel(self, frame=None):
        """
        Callback for consuming cancel from our side
        :param frame: result frame from pika, unused
        :type frame: pika.Frame
        """
        logging.debug("Consuming has been cancelled")
        self._consumer_tag = None
        self.disconnect()

    def on_connection_closed(self, connection, reason):
        """
        Callback on connection has been closed event.
        :param connection: connection which has been closed.
        :type connection: pika.SelectConnection
        :param reason: representing reason for loss of connection.
        :type reason: Exception
        """

        # send our exception to parent to let it know the reason
        self._put_out_exception(reason)

        # connection is to be re-created
        try:
            self._connection.ioloop.stop()
        except Exception as e:
            self._put_out_exception(e)

        self._connection = None
        # Reconnection is to be implemented in parent process

    def on_channel_closed(self, channel, reason):
        """
        Callback for channel closed event.
        :param channel: channel just closed
        :type channel: pika.Channel
        :param reason: close reason
        :type reason: Exception
        """
        logging.debug("Caught channel closed event, disconnecting")
        self._put_out_exception(reason)
        self._channel = None
        # safe to call agin from here since self._channel is dropped
        # this prevents us from infinite loop
        self.disconnect()

    #### END: DISCONNECT-RELATED CALLS AND CALLBACKS

    #### BEG: main-loop-related methods
    def run(self):
        """
        Main run loop
        """
        logging.debug("Started a subprocess, pid is: %d" % self.pid)
        self.connect()

    def _set_ipc_delay(self, msg):
        """
        Check if we may reduce _ipc_q check interval
        :param msg: message process result
        :type msg: IpcMessageResult
        """

        if not msg.ack:
            # we are not interesting in failed messages
            return

        # set _ipc_q check pause to the minimum of message processing time
        if msg.time_delta > 0.0 and msg.time_delta < self._ipc_delay:
            self._ipc_delay = msg.time_delta
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

    def ipc_queue_process(self):
        """
        Callback for interprocess queue checking
        """

        # if we recieve something other than IpcMessageResult instance
        # we consider it as a signal for correct shutdown
        _shutdown = False
        while not self._ipc_q_in.empty():
            _item = self._ipc_q_in.get()

            if not _item:
                raise ValueError("BUG: unexpected item was transferred via interprocess queue")
                # surely this never happend

            if isinstance(_item, IpcMessageResult):
                # we have got message processin result
                # report it to server
                self.report_msg_result(_item)
                self._set_ipc_delay(_item)
            else:
                # otherwise it is something wrong.
                # consider as shutdown signal
                _shutdown = True

            self._ipc_q_in.task_done()

        # if we recieve shutdown signal - disconnect immediately without self re-scheduling
        if _shutdown:
            # it is safe to call disconnect here since nothing will be done if we are disconnected already
            # this prevents us from main loop
            self.disconnect()
            return

        # all OK, schedule new check
        self._connection.ioloop.call_later(delay=self._ipc_delay, callback=self.ipc_queue_process)
        # if the queue is empty for a long time we may safely increase pause between queue checks
        # to reduce the CPU
        self._increase_ipc_delay()

    def on_message(self, channel, method, properties, body):
        """
        Callback for recieved message. Re-deliver message to the main worker via _ipc_q
        :param channel: channel via message was recieved from RabbitMQ, unused
        :type channel: pika.Channel
        :param method: delivery method with exchange, routing key, delivery tag and a redelivered flag
        :type method: pika.Spec.Basic.Deliver
        :param properties: message properties
        :type properties: pika.Spec.BasicProperties
        :param body: message body
        :type body: bytes
        """
        _qmsg = IpcMessage(method.delivery_tag, properties, body)
        self._ipc_q_out.put(_qmsg)

    def report_msg_result(self, rslt):
        """
        Method called to ack/nack message
        :param rslt: result of message process (with message itself)
        :type rslt: IpcMessageResult
        """
        if not rslt:
            raise ValueError("BUG: report_msg_result is called without result itself")

        # acknowledge if all OK
        if rslt.ack:
            logging.info("Acking message with delivery tag %d" % rslt.delivery_tag)
            self._channel.basic_ack(delivery_tag=rslt.delivery_tag)
            return

        logging.info("Nacking message with delivery tag %d, requeue is %s" % (rslt.delivery_tag, rslt.requeue))
        self._channel.basic_nack(delivery_tag=rslt.delivery_tag, requeue=rslt.requeue)

    def _stop(self):
        """
        Calling if the process is to be killed with all its resources
        Do not call it in main loop
        """
        logging.debug("Stopping the connection process")

        # disconnect from RabbitMQ first
        # exceptions are catched there, no need to catch it here
        self.disconnect()

        # close interprocess communications
        self._close_all_ipc_q()

    def _close_all_ipc_q(self):
        """
        Close all ipc queues before terminating.
        This method is to be called in case of object destroying
        """
        # cleanup empty inbound queue
        # since we are destroying ourself - do not process items, just log them
        if self._ipc_q_in:
            try:
                while not self._ipc_q_in.empty():
                    _item = self._ipc_q_in.get()
                    self._ipc_q_in.task_done()

                # we are not able to send exceptions to parent
                # because queues are to be closed
                # so log them here
                self._ipc_q_in.close()
            except:
                pass

        self._ipc_q_out = None
        self._ipc_q_in = None

    def __del__(self):
        self._stop()
    #### END: main-loop-related methods
