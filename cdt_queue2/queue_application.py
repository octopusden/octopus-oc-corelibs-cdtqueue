#!/usr/bin/env python

import pika
import argparse
import logging
import time
from .queue_handler import QueueHandler


class QueueApplication(QueueHandler):
    """
    This class is intended for creating queue consumer application from scratch.
    It takes everything needed including command line arguments processing except actual queue processing
    that must be done by your own methods supplied by subclassing this class
    """

    def _connect_and_run(self):
        logging.info('connecting to %s:%d queue %s as user %s', self.connection_parameters.host, self.connection_parameters.port, self.queue, self.connection_parameters.credentials.username)

        try:
            self.connect()
        except OSError as q:
            # This happens if a serous problem with memory allocation
            # or free disk space occurs. Reconnecting with such a condition is a crime.
            logging.exception(q)
            raise
        except Exception as e:
            logging.exception(e)
            return 2

        # 'run' implements main loop
        # exceptions are catched in 'run' loop
        # but in case of big failure we have to shutdown ourself
        try:
            self.run()
        except Exception as e:
            logging.exception(e)
            self.disconnect()
            return 1

        # do the normal 'disconnection' when run finished with exceptions catched normally
        self.disconnect()

        if self.queue_declare == 'only':
            logging.debug("Queue declaration only specified, stopping normaly")
            return 0

        if self.reconnect:
            logging.debug("Reconnect was specified")
            return 3

        logging.info("Normal connection stopping, but reconnect is not set")
        return 0

    def prepare_parser(self):
        """
        Customization point for creating ArgumentParser object.
        Re-define this method if you want to define your own argparser

        :returns: argparse.ArgumentParser
        """
        return argparse.ArgumentParser(description='Queue handler')

    def init(self, args):
        """
        Put your custom initialization here

        :param args: argparse namespace with pre-parsed command-line options
        """
        pass

    def custom_args(self, parser):
        """
        Put your custom command-line arguments definition here

        :param parser: arpgarse parser object to add your own arguments
        """
        pass

    def basic_args(self, parser=None):
        parser = super(QueueApplication, self).basic_args(parser)
        self.custom_args(parser)
        parser.add_argument('--reconnect', '-r', help='Reconnect on failure', default=False, action='store_true')
        parser.add_argument('--verbose', '-v', help='Verbose output (repeat to get more verbosity)', default=0, action='count')
        parser.add_argument('--log', help='write log file', default=None)
        return parser

    def setup_from_args(self, args=None):
        """
        Setup from arguments from argparse
        :param args: arguments parsing result from argparse.ArgumentParser.parse_args()
        :type args: agparse.
        """
        args = super(QueueApplication, self).setup_from_args(args)
        self.setup(reconnect=args.reconnect)

        return args

    def setup(self, *args, **argv):
        """
        Sets things up.
        :param reconnect: do the reconnect if connection fails
        :type reconnect: boolean

        All other params are the same as in parent classes
        """

        self.reconnect = argv.pop('reconnect', False)

        if len(args) > 0 or len(argv) > 0:
            super(QueueApplication, self).setup(*args, **argv)

    def main(self, cmdline=None):
        """
        Run this as your application's main() function

        :param cmdline: Command line parameters array for debug purposes
        :returns:    return code for os.exit. Quit your application with this code
        """
        self.parser = self.prepare_parser()
        self.basic_args()
        self.args = self.parser.parse_args(cmdline)

        if self.args.verbose == 1:
            logging.basicConfig(level=logging.INFO, filename=self.args.log,
                                format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
        elif self.args.verbose > 1:
            logging.basicConfig(level=logging.DEBUG, filename=self.args.log,
                                format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
        elif self.args.verbose == 0:
            logging.basicConfig(level=logging.WARNING, filename=self.args.log,
                                format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')

        if self.args.verbose < 3:
            logging.getLogger('pika').propagate = False

        logging.captureWarnings(True)

        try:
            self.setup_from_args()
        except (TypeError, ValueError) as e:
            logging.error("Malformed URL: %s", str(e))
            return 2

        self.init(self.args)

        while True:
            logging.debug("Calling _connect_and_run")
            ret = self._connect_and_run()
            logging.debug("Result of _connect_and_run: %d, reconnect is: %s" % (ret, self.reconnect))
            if self.reconnect and ret != 0:
                time.sleep(self._terminate_delay)
                logging.warning('Reconnecting...')
                continue
            break

        logging.info('exiting')
        return 0
