Stimpi: STOMP Asynchronous Client
=================================
Stimpi is an Apache2 Licensed STOMP client library, written in Python.  It is
intended to be used for asynchronous programming and currently offers a Tornado
connector.

While there exist many STOMP client libraries for Python, I could not find one
built for Tornado or Gevent.  The goal of this project is to separate the
core protocol as much as possible from the underlying asynchronous library to
quickly create additional connectors.

Another goal is to make this easy to use for developers, by allowing
subscriptions to queues to be easily mapped to a designated handler via
regular expressions.  This concept is similar to how most modern web frameworks
design and route HTTP requests to a handler.

Overly simple example

.. code:: python
    from stimpi.connection import URLParameters
    from stimpi.adapters.tornado import TornadoConnector
    from stimpi.handlers import MessageHandler


    class SampleMessageHandler(MessageHandler):

        def initialize(self):
            self._logger = logging.getLogger(__name__)

        @tornado.gen.coroutine
        def received(self):
            self._logger.info('Received a message!')

    connection = TornadoConnector(
        URLParameters('stomp://localhost/localhost'),
        UnhandledErrorHandler)

    connection.connect(start=True)

    connection.subscribe(
        '/queue/foo',
        [
            (r'/queue/foo', SampleMessageHandler)
        ])


See examples for complete and runnable applications.


Features
--------

- STOMP 1.0, 1.1, and 1.2 compliance
- Subscriptions
- Transactions
- Receipts (enabled by default for guaranteed delivery)
- Subscription Handlers (mapping done via regular expressions)
- Custom headers
- Global and per subscription handlers
- SSL/TLS (via Tornado IOStreams)
- SSL/TLS parameters (may be specified in a connection string)


Installation
------------

To install Stimpi, simply:

.. code-block:: bash

    $ pip install stimpi


Documentation
-------------

Coming soon


TODO
----

- Heartbeats
- Unit tests
- System tests
