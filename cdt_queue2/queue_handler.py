#!/usr/bin/env python

from .queue_server import QueueServer
import warnings


class QueueHandler(QueueServer):

    published = []  # add functions you wish to call via queue here

    def on_message(self, data, properties):
        """
        Redefine this to add your own processing
        """
        if not isinstance(data, list) or len(data) != 3 or not isinstance(data[1], list) or not isinstance(data[2], dict):
            raise ValueError("invalid message format: expected: [ \"function\", [ arguments ], { parameters } ]")

        (function, arguments, parameters) = data

        if function in self.published and function in dir(self):
            call = getattr(self, function)
        else:
            raise ValueError("invalid message: unknown function %s (known functions are: %s)" % (function, self.published))

        r = call(*arguments, **parameters)
        if r is not None:
            if r == 1:
                warnings.warn(
                    "Do not use return 1 in your methods to indicate success anymore. This is deprecated now. Also, use exceptions to indicate failure")
            else:
                raise ValueError(
                    "Call to method returned unexpected value %s" % str(r))
