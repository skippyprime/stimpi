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

import logging
import os
import collections
import datetime

import six

import tornado.ioloop
import tornado.gen
import tornado.tcpclient

try:
    # Tornado 4.2+
    import tornado.locks as locks
    from tornado.gen import TimeoutError
except ImportError:
    import toro as locks
    from toro import Timeout as TimeoutError

try:
    import simplejson as json
except ImportError:
    import json  # slower

try:
    from flufl.enum import IntEnum
except ImportError:
    from enum import IntEnum

from .base import BaseConnector
from ..frames.base import Frame
from ..handlers import DestinationSpec
from ..transaction import Transaction
from ..errors import (
    ConnectionError,
    ShutdownError,
    FrameError,
    SubscriptionError,
    StompError
)


class TornadoConnector(BaseConnector):
    """STOMP connector for use with a torndao ioloop."""
    class ConnectionState(IntEnum):
        init = 1
        connecting = 2
        connected = 3
        pre_disconnecting = 4
        disconnecting = 5
        disconnected = 6
        shutdown = 7

    def __init__(self,
                 connection_params,
                 error_handler,
                 message_handlers=None,
                 version='1.1',
                 heartbeat=(0, 0),
                 reconnect_attempts=5,
                 reconnect_delay=0.5,
                 io_loop=None):
        """Initializes a new connector for tornado.

        The message handlers specified for this connector are a list of
        iterables (i.e. a list of tuples).  Each handler specified in the list
        must contain at least 2 members, and an additional mapping type
        may be provided to initialize the handler.  Examples:
            1. Basic: (r'^/queue/foo$', FooHandler)
            2. Keyed Args: (r'^/queue/foo$', FooHandler, {'param':'value'})

        Each message handler must extend the base
        :class:`stimpi.handlers.MessageHandler` class.

        Arguments:
            connection_params
                (:class:`stimpi.connection.ConnectionParameters`):
                required connection parameters specifying remote host, port,
                vhost, credentials, etc.
            error_handler (:class:`stimpi.handlers.ErrorHandler`, optional): An
                optional handler for server specified errors that are not
                mappable to a sent frame.  This can usually occur if receipts
                are disabled for one or more messages.

        Keyed Arguments:
            message_handlers (list, optional): A list of iterables defining
                the unbound handlers for MESSAGE frames received from the
                broker for one or more subscriptions.  These handlers are
                unbound in the sense that they could handle messages for any
                subcription.
            version (str): The STOMP protocol version.  One of '1.0', '1.1',
                '1.2'.  Default '1.1'.
            heartbeat (tuple): The transmit and receive requested heartbeats.
                Default (0, 0).  Currently not supported.
            reconnect_attempts (int): Number of attempts to connect to broker
                before giving up.  Default 5.
            reconnect_delay (float): Number of seconds to delay between
                reconnect attempts.  Default 0.5.
            io_loop (:class:`tornado.ioloop.IOLoop`): Depricated
        """
        super(TornadoConnector, self).__init__(
            connection_params,
            version=version,
            heartbeat=heartbeat,
            reconnect_attempts=reconnect_attempts,
            reconnect_delay=reconnect_delay
        )

        self._logger = logging.getLogger(__name__)

        self._io_loop = io_loop or tornado.ioloop.IOLoop.current()

        self._stream = None
        self._client = tornado.tcpclient.TCPClient()

        self._write_lock = locks.BoundedSemaphore(value=1)
        self._connected_event = locks.Event()
        self._disconnected_event = locks.Event()

        self._connection_state = TornadoConnector.ConnectionState.init
        self._clean_shutdown = False

        self._error_handler = error_handler
        self._unbound_message_handlers = []
        self._bound_message_handlers = {}
        self._waiting_receipts = {}
        self._error_receipts = {}
        self._disconnect_receipt_timeout = datetime.timedelta(seconds=5)

        self.add_handlers(None, message_handlers)

    @property
    def connected(self):
        """Checks the connected stated of the underlying socket connection
        to the broker.

        Returns:
            True: Connected to broker
            False: Not connected to broker
        """
        return self._connection_state ==\
            TornadoConnector.ConnectionState.connected

    def add_handlers(self, subscription, handlers):
        if handlers:
            for handler in handlers:
                self.add_handler(subscription, *handler)

    def add_handler(self, subscription, pattern, handler, kwargs=None):
        """Add the specified handler for an optional subscription.  If
        subscription if None or empty, the handler is registered as
        an unbound message handler.

        Arguments:
            subscription (str, optional): The subscription id that this
                handler should be registered against.  This is useful
                to have handlers for messages when there are multiple
                subscriptions.
            pattern (str): The destination pattern to match and route messages.
                This must be a compilable regular expression and should
                match to the end of the string using '$'.
            handler (:class:`stimpi.handlers.MessageHandler`): Handler class
                reference to construct and pass invocation.

        Keyed Arguments
            kwargs (:class:`collections.Mapping`, optional): Initialization
                parameters for the handler post construction.  This should
                be a mapping type for keyed arguments.  Arguments are passed
                to the MessageHandler.initialize(*args, **kwargs) function.
        """
        registered_handler = DestinationSpec(pattern, handler, kwargs)

        if subscription:
            subscription_handlers =\
                self._bound_message_handlers.get(subscription, None)
            if not subscription_handlers:
                subscription_handlers = []
                self._bound_message_handlers[subscription] =\
                    subscription_handlers
            subscription_handlers.append(registered_handler)
        else:
            self._unbound_message_handlers.append(registered_handler)

    @tornado.gen.coroutine
    def _send_frame(self,
                    frame,
                    wait_connected=True,
                    receipt_event=None):
        """Sends a single frame, and optional body to the server.  At most one
        concurrent write operation can be performed, so we will use a
        bounded semaphore to ensure that only one write occurs at a give time.

        Arguments:
            frame (:class:`stimpi.frames.base.Frame`): An implementation of
                a frame to send across the wire.

        Keyed Arguments:
            wait_connected (bool): Wait for the connection to come up.
                Default True.
            receipt_event (Event): Optionally wait for the receive loop to
                notify a RECEIPT for the frame.  Default None.
        """
        if wait_connected:
            yield self._ensure_connected()

        yield self._write_lock.acquire()
        try:
            # check if shutdown for any waiting writes
            #if self._is_shutdown:
            #    raise ShutdownError('Connection shutdown')

            #if isinstance(frame, self.protocol.module.Disconnect):
            #    self._is_shutdown = True

            # the frame encapsulates the verb and headers
            # avoid building a string with the body, instead we will
            # write it directly to the stream
            yield self._stream.write(frame.dumps(with_body=False))

            if frame.body:
                yield self._stream.write(frame.body)

            # all frames end with a null and line ending
            yield self._stream.write(self.protocol.body_end_delim)

            self._logger.debug('Sent frame: %s', frame.definition.verb)
            self._logger.debug('Raw frame:\n%s', str(frame))
        finally:
            # always release the semaphore on completion (success or error)
            # or suffer a deadlock
            self._write_lock.release()

        if receipt_event:
            yield receipt_event.wait()

    @tornado.gen.coroutine
    def _ensure_connected(self, start=True):
        """Ensures the connection is up or connects to the broker.

        Keyed Arguements:
            start (bool): Start the receive loop once connected.  Default True.
        """
        if self._connection_state >\
                TornadoConnector.ConnectionState.connected:
            raise ShutdownError('Connection shutdown')

        if self._connection_state ==\
                TornadoConnector.ConnectionState.connected:
            # already connected
            return

        if self._connection_state ==\
                TornadoConnector.ConnectionState.connecting:
            # wait for connection to complete
            self._logger.debug('Waiting for pending connection')
            yield self._connected_event.wait()
            if self._connection_state !=\
                    TornadoConnector.ConnectionState.connected:
                raise ConnectionError('Failed to connect')
            return

        # need to connect
        # immediately set the state to connecting to avoid two attempts
        # because we will yield to the ioloop here
        self._connection_state = TornadoConnector.ConnectionState.connecting

        # open the tcp connection
        yield self._open_socket()

        # send the connect frame
        try:
            yield self._send_frame(self._build_connect_frame(),
                                   wait_connected=False)
        except tornado.iostream.StreamClosedError as e:
            six.raise_from(
                ConnectionError('Failed to send connect frame'), e)

        # receive the connected or error frame
        self._logger.debug('Waiting for CONNECTED frame')
        try:
            frame = yield self._receive_frame(wait_connected=False)
        except tornado.iostream.StreamClosedError as e:
            six.raise_from(
                ConnectionError('Failed to receive CONNECTED frame'), e)

        self._handle_connected_frame(frame, start)

    def _handle_connected_frame(self, frame, start):
        """Process the CONNECTED or ERROR frames in response to a CONNECT
        frame.

        CONNECTED frame means we successfully connected.
        ERROR frame means we were rejected a connection.
        Any other frame is an error condition.

        Arguments:
            frame (:class:`stimpi.frames.Frame`): A received frame in response
                to the CONNECT frame.  Should be either a
                :class:`stimpi.frames.impl.connected.Connected` or
                :class:`stimpi.frames.impl.error.Error` frame.
            start (bool): Start the receive loop if successfully connected
                to the broker.
        """
        if isinstance(frame, self.protocol.module.Connected):
            # successfully connected, notify waiting operations
            self._connection_state = TornadoConnector.ConnectionState.connected
            self._connect_attempts = 0
            self._disconnected_event.clear()
            self._connected_event.set()

            # start the receive loop
            if start:
                self._logger.debug('Starting receive loop')
                future_result = self._receive_frame()
                self._io_loop.add_future(future_result, self._on_receive_frame)
        elif isinstance(frame, self.protocol.module.Error):
            # failed to connect
            self._connection_state =\
                TornadoConnector.ConnectionState.disconnected
            self._connected_event.set()  # let blocked requests through
            raise ConnectionError(
                'Failed to connect to server:  {:s}'.format(
                    frame.headers.get('message', 'No error provided')
                ))
        else:
            # this is not a valid frame
            self._connection_state =\
                TornadoConnector.ConnectionState.disconnected
            self._connected_event.set()
            raise ConnectionError(
                'Received unexpected frame in response to connect')

    @tornado.gen.coroutine
    def _open_socket(self):
        """Opens the TCP socket to the broker.  This does not send any
        messages.
        """
        self._logger.debug('Opening socket connection',
                           extra={
                               'host': self.connection_params.host,
                               'port': self.connection_params.port
                           })
        while True:
            try:
                self._stream = yield self._client.connect(
                    self.connection_params.host,
                    self.connection_params.port)
                break
            except IOError as e:
                self._stream = None
                self._connect_attempts += 1
                if self._connect_attempts == self._reconnect_attempts:
                    self._logger.debug('Connection failed, giving up')
                    six.raise_from(
                        ConnectionError(
                            'Failed to open connection to '
                            'host "{:s}:{:d}"'.format(
                                self.connection_params.host,
                                self.connection_params.port)),
                        e)
                else:
                    self._logger.debug('Connection failed, retrying in %f '
                                       'seconds', self._reconnect_delay)
                    yield tornado.gen.sleep(self._reconnect_delay)

    @tornado.gen.coroutine
    def _receive_frame(self, wait_connected=True):
        """Read a single frame from the socket.

        Keyed Arguements:
            wait_connected (bool): Wait for the connection to come up fully.
                Default True.
        """
        frame = None

        if wait_connected:
            yield self._ensure_connected()

        # ignore all EOL values
        while True:
            data = yield self._stream.read_until_regex(
                self.protocol.line_end_pattern)
            if self.protocol.line_end_pattern.match(data):
                if self._receiving_heartbeats:
                    # this is a heartbeat -- connection is alive
                    pass
                # we are not heartbeating, but the protocol allows for
                # EOLs between frames -- ignore them
                continue
            else:
                break

        # at this point, we should have read a command
        data = self._decode_frame_headers(data)
        frame = Frame.loads_command(self.protocol, data)

        # load the headers, if any
        while True:
            data = yield self._stream.read_until_regex(
                self.protocol.line_end_pattern)
            if self.protocol.line_end_pattern.match(data):
                # end of headers / beginning of body
                break
            else:
                data = self._decode_frame_headers(data)
                frame.loads_header(data)

        # load the body
        content_length = frame.headers.get('content-length', 0)
        if content_length:
            # read content length + null byte
            data = yield self._stream.read_bytes(content_length + 1)
        else:
            # read until the null byte
            data = yield self._stream.read_until(
                self.protocol.body_end_delim)

        # trim the null byte (inefficient?)
        frame.body = data[:-1]

        self._logger.debug('Received frame: "{}"'.format(
            frame.definition.verb))
        self._logger.debug('Raw frame:\n%s', str(frame))

        raise tornado.gen.Return(frame)

    def _on_receive_frame(self, future):
        # check if the error (if any specified) is recoverable
        error = future.exception()
        frame = None
        if error:
            if not self._is_receive_error_recoverable(error):
                # this is a fatal error, stop receiving frames
                return
        else:
            frame = future.result()

        # if still connected, queue the next read.  this essentially creates
        # an infinite read loop until disconnected
        if self.connected:
            next_result = self._receive_frame()
            self._io_loop.add_future(next_result, self._on_receive_frame)

        # if there is not frame, nothing to process
        if not frame:
            return

        # handle basic processing
        is_error, has_receipt = self._process_received_frame(frame)

        # if this is an error, shutdown
        if is_error:
            close_result = self._close_socket()
            self._io_loop.add_future(close_result, self._on_receive_close)
            if not has_receipt:
                self._dispatch_error(StompError('Unexpected error', frame))

    def _is_receive_error_recoverable(self, error):
        if not error:
            return True

        if isinstance(error, ShutdownError):
            # shutdown errors occur when the client disconnects cleanly
            # at least (usually)
            return False
        elif isinstance(error, tornado.iostream.StreamClosedError):
            if self._connection_state >\
                    TornadoConnector.ConnectionState.connected or\
                    self._clean_shutdown:
                # this is in response to a disconnect/close operation
                # swallow this exception
                return False
            # the socket went down unexpectedly, log and dispatch an error
            self._logger.error('Connection lost')
            close_result = self._close_socket()
            self._io_loop.add_future(close_result, self._on_receive_close)
            self._dispatch_error(ConnectionError('Connection lost', error))
            return False
        elif isinstance(error, FrameError):
            # Failed to parse a server frame, try to continue
            self._logger.error('Failure in parsing frame', error)
            self._dispatch_error(error)
            return True

        # we got an error that we didn't expect
        self._logger.exception(error)
        self._dispatch_error(error)
        return False

    def _on_receive_close(self, future):
        error = future.exception()

        if not error:
            return

        if isinstance(error, ShutdownError):
            # shutdown errors occur when the client disconnects cleanly
            # at least (usually)
            return
        elif isinstance(error, tornado.iostream.StreamClosedError):
            # we were already shuting down, so ignore
            return

        # we got an error that we didn't expect
        self._logger.exception(error)
        self._dispatch_error(error)

    def _process_received_frame(self, frame):
        """Process a received frame for common cases of receipts and messages.
        If an error frame is received, save the frame to raise a meaningful
        exception to the caller.

        Arguments:
            frame (:class:`stimpi.frames.Frame`): Received frame to handle.
        """
        is_error = isinstance(frame, self.protocol.module.Error)
        has_receipt = False

        # notify anyone blocking for a receipt
        receipt_id = frame.headers.get('receipt-id', None)
        if receipt_id:
            receipt_event = self._waiting_receipts.pop(receipt_id, None)
            if receipt_event:
                has_receipt = True
                if is_error:
                    self._error_receipts[receipt_id] = frame
                receipt_event.set()

        # if this is a message, dispatch to the correct handler
        if isinstance(frame, self.protocol.module.Message):
            self._dispatch_message(frame)

        return is_error, has_receipt

    def _find_message_handler(self, frame):
        """Find a handler (if exists) for the message based on subscription
        and destination.

        Arguments:
            frame (:class:`stimpi.frames.Frame`): Received message frame.
        """
        destination = frame.headers.get('destination', None)
        subscription = frame.headers.get('subscription', None)
        handlers = None

        if subscription:
            handlers = self._bound_message_handlers.get(subscription, None)

        handlers = handlers or self._unbound_message_handlers

        if not handlers:
            return None, None

        for handler in handlers:
            if handler.pattern.match(destination):
                return subscription, handler

        return subscription, None

    @tornado.gen.coroutine
    def _dispatch_message(self, frame):
        """Dispatch a received message to the correct handler.  This prefers
        handlers for the designated subscription, but will use the global
        unbound handlers if none are registered for the designated
        subscription.

        Arguments:
            frame (:class:`stimpi.frames.Frame`): Received message frame.
        """
        subscription, handler = self._find_message_handler(frame)

        if not handler:
            self._logger.warn('No handler for message')
            self._dispatch_error(SubscriptionError('No handler for message'))
            return

        handler_impl = handler.handler(self, frame)
        handler_impl.initialize(**handler.kwargs)

        # process the message through the handler
        try:
            yield tornado.gen.maybe_future(handler_impl.received())
        except Exception as e:
            self._logger.exception(e)
            if subscription:
                if self._subscriptions.get(subscription, 'auto') != 'auto':
                    # we are not auto acking, so nack on error
                    yield self.nack(frame, subscription)
        else:
            if subscription:
                if self._subscriptions.get(subscription, 'auto') != 'auto':
                    # we are not auto acking, so ack on success
                    yield self.ack(frame, subscription)

    def _dispatch_error(self, error):
        """Dispatch an error to the error handler or raise if no handler
        registered."""
        if self._error_handler:
            impl = self._error_handler(self, error)
            impl.handle(error)
        else:
            six.reraise(error)

    @tornado.gen.coroutine
    def _close_socket(self):
        """Close the TCP socket."""
        if self._connection_state >=\
                TornadoConnector.ConnectionState.disconnected:
            return

        self._logger.debug('Starting disconnect')

        if self._connection_state ==\
                TornadoConnector.ConnectionState.disconnecting:
            self._logger.debug('Waiting for existing disconnect request')
            yield self._disconnected_event.wait()
            return

        self._connection_state =\
            TornadoConnector.ConnectionState.disconnecting

        self._connected_event.clear()

        if self._stream:
            self._logger.debug('Closing socket')
            self._stream.close()
            self._stream = None

        self._connection_state =\
            TornadoConnector.ConnectionState.disconnected
        self._disconnected_event.set()
        self._logger.debug('Disconnected')

    def _create_receipt_event(self, frame, with_receipt):
        """Create and register a receipt event for the frame if requested.
        If the frame has a receipt header, use that ID.  Otherwise create
        a random receipt ID.

        Arguments:
            frame (:class:`stimpi.frames.Frame`): Client frame.
            with_receipt (bool): Requires/wants receipt.
        """
        if not with_receipt:
            return None

        id = '{0:s}-{1:s}'.format(
            frame.definition.verb, os.urandom(16).encode('hex'))
        frame.headers['receipt'] = id

        # use an event here because we don't want to get stuck waiting
        # in the event that the receive loop notifies before we wait on
        # the condition
        receipt_event = locks.Event()
        self._waiting_receipts[id] = receipt_event

        return receipt_event

    def _check_receipt_error(self, frame, message):
        """Check if there was an ERROR frame received instead of a RECEIPT
        frame.  If there was an ERROR frame received, raise the error
        for the caller.

        Arguments:
            frame (:class:`stimpi.frames.Frame`): Client frame requiring
                receipt.
            message (str): Error message if error exists.
        """
        id = frame.headers.get('receipt', None)
        if not id:
            return

        error = self._error_receipts.pop(id, None)

        if error:
            raise StompError(
                '{0:s}: {1:s}'.format(
                    message,
                    error.headers.get('message', 'unspecified error')),
                error)

    @tornado.gen.coroutine
    def disconnect(self,
                   headers=None,
                   with_receipt=True):
        """Disconnect from the broker gracefully.  Accepts a callback or
        returns a future if no callback provided.

        It is recommended to always ask for a receipt.

        Keyed Arguemnts:
            headers (:class:`collections.Mapping`, optional): Additional
                headers to supply with the frame.  Default None.
            with_receipt (bool, optional): Request a receipt acknowledgement
                from the broker.  Default True.
            callback (func, optional): Callback upon completion.  Default None.

        Returns:
            A Future if no callback if specified.
        """
        frame = self._build_disconnect_frame(headers)

        # disconnect and wait for receipt, if requested
        receipt_event = self._create_receipt_event(
            frame,
            with_receipt and self.protocol.disconnect_ack)
        self._clean_shutdown = True
        yield self._send_frame(frame)

        # attempt to wait for receipt, but no guaruntees
        if receipt_event:
            try:
                yield receipt_event.wait(self._disconnect_receipt_timeout)
            except TimeoutError:
                self._logger.debug('Timeout waiting for disconnect receipt')

        self._connection_state =\
            TornadoConnector.ConnectionState.pre_disconnecting

        # close the socket
        yield self._close_socket()

    @tornado.gen.coroutine
    def connect(self,
                start=False):
        """Connect to a broker.  Accepts a callback or returns a Future if
        no callback provided.

        Keyed Arguemnts:
            start (bool): Start the frame receive loop.
            callback (func, optional): Callback upon completion.  Default None.

        Returns:
            A Future if no callback if specified.

        Raises:
            :class:`stimpi.errors.ConnectionError` if the connection was
            could not be established.
        """
        super(TornadoConnector, self).connect()
        yield self._ensure_connected(start=start)

    @tornado.gen.coroutine
    def start(self):
        """Start the frame receive loop.  Accepts a callback or returns a Future if
        no callback provided.

        Keyed Arguemnts:
            callback (func, optional): Callback upon completion.  Default None.

        Returns:
            A Future if no callback if specified.
        """
        yield self._ensure_connected(start=False)
        future_result = self._receive_frame()
        self._io_loop.add_future(future_result, self._on_receive_frame)

    @tornado.gen.coroutine
    def subscribe(self,
                  destination,
                  message_handlers,
                  id=None,
                  ack='auto',
                  selector=None,
                  headers=None,
                  with_receipt=True):
        """Subscribe to a queue or topic.  Accepts a callback or returns a
        future if no callback provided.

        It is recommended to always ask for a receipt.

        Arguments:
            destination (str): Queue or topic to subscribe.
            message_handlers (list, optional): A list of iterables defining
                the unbound handlers for MESSAGE frames received from the
                broker for one or more subscriptions.  These handlers are
                unbound in the sense that they could handle messages for any
                subcription.

        Keyed Arguemnts:
            id (str, optional): Unique identifier for this subscription across
                this connection/session.  If not provided, one is automatically
                generated.  Default None (i.e. generated).
            ack (str): Message acknowledgement pattern.  One of 'auto' or
                'client'.  Default 'auto'.
            selector (str, optional): SQL92 selector pattern.  Default None.
            headers (:class:`collections.Mapping`, optional): Additional
                headers to supply with the frame.  Default None.
            with_receipt (bool, optional): Request a receipt acknowledgement
                from the broker.  Default True.
            callback (func, optional): Callback upon completion.  Default None.

        Returns:
            A Future if no callback if specified.  Result is the unique
            identifier for the subscription.

        Raises:
            :class:`stimpi.errors.StompError` if the broker returned an error
            frame with the matching receipt id.  If with_receipt is False, no
            StompError will ever be raised.
            :class:`stimpi.errors.ShutdownError` if the connection was closed
            by the client side.
            :class:`stimpi.errors.ConnectionError` if the connection was
            unexpectedly closed or lost.
        """
        frame = self._build_subscribe_frame(destination,
                                            id,
                                            ack,
                                            selector,
                                            headers)
        id = frame.headers['id']

        if id in self._bound_message_handlers and message_handlers:
            raise KeyError('Multiple subscriptions with the same '
                           'id: {0:s}'.format(
                               id))

        # register the message handlers for this idetified subscription
        # if any are provided.  do this before sending the message
        # to avoid missing messages before the ioloop gives us back
        # control.
        self._subscriptions[id] = ack
        self.add_handlers(id, message_handlers)

        # subscribe and wait for receipt, if requested
        receipt_event = self._create_receipt_event(frame, with_receipt)
        try:
            yield self._send_frame(frame, receipt_event=receipt_event)
            self._check_receipt_error(frame, 'Error subscribing')
        except Exception as e:
            self._subscriptions.pop(id, None)
            self._bound_message_handlers.pop(id, None)
            six.reraise(e)

        raise tornado.gen.Return(id)

    @tornado.gen.coroutine
    def unsubscribe(self,
                    id,
                    headers=None,
                    with_receipt=True):
        """Unsubscribe from a queue or topic (i.e. cancel an existing
        subscription).  Accepts a callback or returns a future if no callback
        provided.

        It is recommended to always ask for a receipt.

        Arguments:
            id (str): Unique identifier for the subscription to cancel.

        Keyed Arguemnts:
            headers (:class:`collections.Mapping`, optional): Additional
                headers to supply with the frame.  Default None.
            with_receipt (bool, optional): Request a receipt acknowledgement
                from the broker.  Default True.
            callback (func, optional): Callback upon completion.  Default None.

        Returns:
            A Future if no callback if specified.  Result is the unique
            identifier for the subscription.

        Raises:
            :class:`stimpi.errors.StompError` if the broker returned an error
            frame with the matching receipt id.  If with_receipt is False, no
            StompError will ever be raised.
            :class:`stimpi.errors.ShutdownError` if the connection was closed
            by the client side.
            :class:`stimpi.errors.ConnectionError` if the connection was
            unexpectedly closed or lost.
        """
        frame = self._build_unsubscribe_frame(id, headers)

        # unsubscribe and wait for receipt, if requested
        receipt_event = self._create_receipt_event(frame, with_receipt)
        yield self._send_frame(frame, receipt_event=receipt_event)
        self._check_receipt_error(frame, 'Error unsubscribing')

        # remove handlers -- ignore if not found
        self._bound_message_handlers.pop(id, None)
        self._subscriptions.pop(id, None)

    @tornado.gen.coroutine
    def send(self,
             destination,
             data=None,
             json_data=None,
             transaction=None,
             headers=None,
             with_receipt=True):
        """Send (i.e. publish) a message to a queue or topic.  Accepts a
        callback or returns a future if no callback provided.

        If data is provided (i.e. pre-encoded message body), the following
        headrs should be specified:
            * content-type: MIME type and optional charset
            * content-length: byte count of data

        If using json_data, content-type and content-length headers will be
        automatically added when the data is encoded.

        It is recommended to always ask for a receipt.

        Arguments:
            destination (str): Queue or topic to receive message.

        Keyed Arguemnts:
            data (str, optional): Body of the message.  Use this if the content
                is already encoded or binary.
            json_data (:class:`collections.Mapping`, optional): Body of message
                to be automatcially encoded as a JSON object.
            selector (str, optional): SQL92 selector pattern.  Default None.
            headers (:class:`collections.Mapping` or
                :class:`collections.Sequence`, optional): Additional
                headers to supply with the frame.  Default None.
            with_receipt (bool, optional): Request a receipt acknowledgement
                from the broker.  Default True.
            callback (func, optional): Callback upon completion.  Default None.

        Returns:
            A Future if no callback if specified.

        Raises:
            :class:`stimpi.errors.StompError` if the broker returned an error
            frame with the matching receipt id.  If with_receipt is False, no
            StompError will ever be raised.
            :class:`stimpi.errors.ShutdownError` if the connection was closed
            by the client side.
            :class:`stimpi.errors.ConnectionError` if the connection was
            unexpectedly closed or lost.
        """
        frame = self._build_send_frame(destination, transaction, headers)

        if json_data and data:
            raise ValueError('Cannot specify both data and json_data to send')

        if json_data:
            if not isinstance(json_data, collections.Mapping) and\
                    not isinstance(json_data, collections.Sequence):
                raise ValueError('json_data is not a mapping or a sequence')
            data = json.dumps(json_data)
            frame.headers['content-type'] = 'application/json;charset=utf-8'

        frame.headers['content-length'] = len(data)

        frame.body = data

        # send and wait for receipt, if requested
        receipt_event = self._create_receipt_event(frame, with_receipt)
        yield self._send_frame(frame, receipt_event=receipt_event)
        self._check_receipt_error(frame, 'Error sending')

    @tornado.gen.coroutine
    def ack(self,
            message_id,
            subscription=None,
            transaction=None,
            headers=None,
            with_receipt=True):
        frame = self._build_ack_frame(message_id,
                                      subscription,
                                      transaction,
                                      headers)

        # ack and wait for receipt, if requested
        receipt_event = self._create_receipt_event(frame, with_receipt)
        yield self._send_frame(frame, receipt_event=receipt_event)
        self._check_receipt_error(frame, 'Error acking message')

    @tornado.gen.coroutine
    def nack(self,
             message_id,
             subscription=None,
             transaction=None,
             headers=None,
             with_receipt=True):
        frame = self._build_nack_frame(message_id,
                                       subscription,
                                       transaction,
                                       headers)

        # ack and wait for receipt, if requested
        receipt_event = self._create_receipt_event(frame, with_receipt)
        yield self._send_frame(frame, receipt_event=receipt_event)
        self._check_receipt_error(frame, 'Error nacking message')

    @tornado.gen.coroutine
    def abort(self,
              transaction,
              headers=None,
              with_receipt=True):
        frame = self._build_abort_frame(transaction,
                                        headers)

        # ack and wait for receipt, if requested
        receipt_event = self._create_receipt_event(frame, with_receipt)
        yield self._send_frame(frame, receipt_event=receipt_event)
        self._check_receipt_error(frame, 'Error aborting transaction')

    @tornado.gen.coroutine
    def commit(self,
               transaction,
               headers=None,
               with_receipt=True):
        frame = self._build_commit_frame(transaction,
                                         headers)

        # ack and wait for receipt, if requested
        receipt_event = self._create_receipt_event(frame, with_receipt)
        yield self._send_frame(frame, receipt_event=receipt_event)
        self._check_receipt_error(frame, 'Error commiting transaction')

    @tornado.gen.coroutine
    def begin(self,
              transaction=None,
              headers=None,
              with_receipt=True):
        if not transaction:
            transaction =\
                'transaction-{0:s}'.format(os.urandom(16).encode('hex'))

        frame = self._build_begin_frame(transaction,
                                        headers)

        # ack and wait for receipt, if requested
        receipt_event = self._create_receipt_event(frame, with_receipt)
        yield self._send_frame(frame, receipt_event=receipt_event)
        self._check_receipt_error(frame, 'Error beginning transaction')

        raise tornado.gen.Return(Transaction(self, transaction))
