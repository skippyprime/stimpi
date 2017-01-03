#!/usr/bin/env python
#
# Copyright 2015 Geoff MacGill
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

import coloredlogs
import logging
import signal

import tornado.ioloop
import tornado.gen

from stimpi import (
    URLParameters,
    ConnectionError,
    ShutdownError
)
from stimpi.handlers import (
    ErrorHandler,
    MessageHandler
)
from stimpi.adapters.tornado import TornadoConnector


class UnhandledErrorHandler(ErrorHandler):
    @tornado.gen.coroutine
    def handle(self):
        logger = logging.getLogger(__name__)
        logger.error(self.error)
        self.connection.disconnect()


class SampleMessageHandler(MessageHandler):
    def initialize(self):
        self._logger = logging.getLogger(__name__)

    @tornado.gen.coroutine
    def received(self):
        print(self.message)


class MainApplication:
    def __init__(self, connection):
        self._connection = connection
        self._logger = logging.getLogger(__name__)

    @tornado.gen.coroutine
    def start(self):
        # start the connection
        yield self._connection.connect(start=True)

        # subscribe to the foo queue
        subscription = yield self._connection.subscribe(
            '/queue/foo',
            [
                (r'/queue/foo', SampleMessageHandler)
            ])

        # publish messages to the foo queue
        try:
            for x in xrange(1, 6):
                yield self._connection.send('/queue/foo', json_data={'hello': x})
        except ShutdownError as e:
            self._logger.error(e.message)
            return
        except ConnectionError as e:
            self._logger.exception(e)
            return

        # start a transaction
        transaction = yield self._connection.begin()
        yield transaction.send('/queue/foo', json_data={'transaction': 1})
        yield transaction.send('/queue/foo', json_data={'transaction': 2})
        yield transaction.commit()

        # unsubscribe
        yield self._connection.unsubscribe(subscription)

def main():
    logger = logging.getLogger()
    coloredlogs.install(level=logging.WARN)

    ioloop = tornado.ioloop.IOLoop.current()
    connection = TornadoConnector(
        URLParameters('stomp://localhost/localhost'),
        UnhandledErrorHandler)
    app = MainApplication(connection)

    def on_disconnect(result):
        logger.info('Disconnected')
        logger.info('Stopping the main event loop')
        tornado.ioloop.IOLoop.current().stop()
        logger.info('Stopped')

    def shutdown():
        logger.info('Shutting down')
        if connection:
            logger.info('Disconnecting from broker')
            connection.disconnect(callback=on_disconnect)

    def signal_handler(signum, frame):
        if signum != signal.SIGTERM and\
                signum != signal.SIGINT:
            return
        logger.info('Processing shutdown request')
        ioloop.add_callback_from_signal(shutdown)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # register our app to send messages
    app.start()

    # start the main event loop -- blocks
    ioloop.start()

if __name__ == "__main__":
    main()
