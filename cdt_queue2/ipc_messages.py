import logging

"""
Classes for interprocess queues exchange
"""


class IpcExcMsg(object):
    def __init__(self, exc):
        """
        Main intialization
        """
        ## transferring an exception by multiprocessin queue
        ## is very buggy and stucks often
        ## text version is safe to report only, so convert
        ## everything to basic types: text, number, None 
        self.type = type(exc)
        self.reply_code = None

        if hasattr(exc, 'reply_code'):
            self.reply_code = exc.reply_code

        self.msg = repr(exc)

    def __repr__(self):
        return self.msg

    def __str__(self):
        return self.msg


class IpcMessage(object):
    """
    Helper class to excnange messages between processes
    """

    def __init__(self, delivery_tag, properties, body):
        """
        Main initialization
        :param delivery_tag: delivery tag
        :type delivery_tag: int
        :param properties: message properties
        :type properties: pika.Spec.BasicProperties
        :param body: message body
        :type body: bytes
        """
        self.delivery_tag = delivery_tag
        self.properties = properties
        self.body = body


class IpcMessageResult(object):
    """
    Helper class to set message processing result
    """

    def __init__(self, delivery_tag, ack, requeue, time_delta=0):
        """
        Main initialization
        :param delivery_tag: the message delivery tag
        :type delivery_tag: int
        :param ack: ack the message
        :type ack: boolean
        :param requeue: do requeue the message
        :type requeue: boolean
        :param time_delta:
        :type time_delta: float
        """
        self.delivery_tag = delivery_tag
        self.ack = ack
        self.requeue = requeue
        self.time_delta = time_delta
