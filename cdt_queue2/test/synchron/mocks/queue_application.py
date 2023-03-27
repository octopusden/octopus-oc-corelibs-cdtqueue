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
        urlparams = self.connection_parameters
        logging.info('connecting to %s:%d queue %s as user %s', urlparams.host, urlparams.port, self.queue, urlparams.credentials.username)
#       queue_declare = ( declare == 'yes' or declare == 'only' )
        try:
            self.connect()
        except pika.exceptions.AMQPConnectionError as e:
            logging.error("can't connect to %s:%d - %s", urlparams.host, urlparams.port, repr(e))    
            return 2

        if self.args.declare == 'only':
            logging.info("queue has been created, --declare=only - no actions required, exiting")
            return 0

        try:
            self.prepare()
        except pika.exceptions.ChannelClosed as e:
            logging.error("error on preparation: %s", repr(e))
            return 1

        logging.info('waiting for messages')
        try:
            self.run()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosed) as e:
#           if self.args.verbose > 1: logging.exception("Connection closed exception happened")
            logging.error("Connection closed: %s", repr(e))
            return 1

        return 0

    def prepare_parser(self):
        """
        Customization point for creating ArgumentParser object.
        Re-define this method if you want to define your own argparser

        :returns: argparse.ArgumentParser
        """
        return argparse.ArgumentParser(description = 'Queue handler')

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

    def basic_args(self, parser = None):
        parser = super(QueueApplication, self).basic_args(parser)
        self.custom_args(parser)
        parser.add_argument('--reconnect', '-r', help = 'Reconnect on failure', default = False, action='store_true')
        parser.add_argument('--verbose', '-v', help = 'Verbose output (repeat to get more verbosity)', default = 0, action = 'count')
        parser.add_argument('--log',help='write log file',default=None)
        return parser


    def main(self, cmdline = None):
        """
        Run this as your application's main() function

        :param cmdline: Command line parameters array for debug purposes
        :returns:    return code for os.exit. Quit your application with this code
        """
        self.parser = self.prepare_parser()
        self.basic_args()
        self.args = self.parser.parse_args(cmdline)

        if self.args.verbose == 1: logging.basicConfig(level=logging.INFO, filename = self.args.log, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
        elif self.args.verbose > 1: logging.basicConfig(level=logging.DEBUG, filename = self.args.log, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
        elif self.args.verbose == 0: logging.basicConfig(level=logging.WARNING, filename = self.args.log, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')

        if self.args.verbose < 3: logging.getLogger('pika').propagate = False
        logging.captureWarnings(True)

        try:
            self.setup_from_args()
        except (TypeError, ValueError) as e:
            logging.error("Malformed URL: %s", str(e))
            return 2

        self.init(self.args)

        while True:
            ret = self._connect_and_run()
            if self.args.reconnect and ret != 0: 
                time.sleep(1)
                logging.warning('Reconnecting...')
                continue
            break

        logging.info('exiting')
        return 0

#class QApp(QueueApplication):
#    published = ['ping']
#
#    def ping(self):
#        logging.info("ping message received")
#        return 1


#if __name__ == '__main__':
#    exit (QApp().main())


