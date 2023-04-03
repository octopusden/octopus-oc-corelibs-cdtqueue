#!/usr/bin/env python

from oc_cdt_queue2.queue_client import QueueClient


class _CallbackHelper(object):
    """
    This is a helper class for sending actual messages to the queue
    Do not use it directly
    """

    def __init__(self, parent, name, fmt=None):
        self.name = name
        self.fmt = fmt
        self.parent = parent

    def action(self, *argv, **argp):
        if isinstance(self.fmt, list):
            for k in argp.keys():
                if k not in fmt:
                    raise TypeError('No such key ' + k)
        self.parent.send([self.name, argv, argp])


class QueueRPC(QueueClient):
    """
    This class can be supclassed for defining interface for your queue-driven application
    """

    published = ['ping']  # add names of your methods here

    def __getattr__(self, attr):
        if self.connection is None or not self.connection.is_open or self.channel == None:
            raise AttributeError("not initialized")
        if self.published is not None and attr not in self.published:
            raise AttributeError("no attribute " + str(attr))
        fmt = None
        if isinstance(self.published, dict):
            fmt = self.published[attr]
        return _CallbackHelper(self, attr, fmt).action
