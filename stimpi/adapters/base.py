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

import collections

from ..frames.versions import ProtocolVersions
from ..errors import ConnectionError


RegisteredHandler = collections.namedtuple(
    'RegisteredHandler',
    [
        'pattern',
        'handler',
        'args',
        'kwargs'
    ])


class BaseConnector(object):
    def __init__(self,
                 connection_params,
                 version='1.1',
                 heartbeat=(0, 0),
                 reconnect_attempts=5,
                 reconnect_delay=0.5):
        self._connection_params = connection_params
        self._protocol = None
        self._reconnect_attempts = reconnect_attempts or 5
        self._reconnect_delay = reconnect_delay or 0.5
        self._connect_attempts = 0
        self._heartbeat = heartbeat
        self._heartbeat_send = None
        self._heartbeat_receive = None
        self._session = None
        self._logger = logging.getLogger(__name__)
        self._subscriptions = {}

        try:
            self._protocol = ProtocolVersions.load(version or '1.1')
        except KeyError:
            raise ValueError('Unsupported protocol version "{:s}"'.format(
                version))

    @property
    def connection_params(self):
        return self._connection_params

    @property
    def protocol(self):
        return self._protocol

    @property
    def connected(self):
        raise NotImplementedError('Property not implemented: "connected"')

    @property
    def session(self):
        return self._session

    @property
    def subscriptions(self):
        return [k for k in self._subscriptions.keys]

    @property
    def heartbeating(self):
        if not self._protocol.heartbeat:
            return False

        if self._heartbeat_send or self._heartbeat_receive:
            return True

    @property
    def _receiving_heartbeats(self):
        if not self._protocol.heartbeat:
            return False

        if self._heartbeat_receive:
            return True

    def connect(self):
        pass

    def _build_connect_frame(self):
        frame = self.protocol.module.Connect()
        frame.set_vhost(self.connection_params.vhost)
        frame.prepare()
        frame.ensure_headers()
        return frame

    def _build_subscribe_frame(self,
                               destination,
                               id,
                               ack,
                               selector,
                               headers):
        if not destination:
            raise ValueError('Subscribe has no destination')

        frame = self.protocol.module.Subscribe()

        if headers and isinstance(headers, collections.Mapping):
            frame.headers.update(headers)

        frame.headers['destination'] = destination

        if not id:
            id = self.protocol.module.Subscribe.random_id()
        frame.headers['id'] = id

        if ack:
            frame.headers['ack'] = ack
        if selector:
            frame.headers['selector'] = selector

        frame.prepare()
        frame.ensure_headers()
        return frame

    def _build_unsubscribe_frame(self,
                                 id,
                                 headers):
        if not id:
            raise ValueError('Unsubscribe has no id')

        frame = self.protocol.module.Unsubscribe()

        if headers and isinstance(headers, collections.Mapping):
            frame.headers.update(headers)

        frame.headers['id'] = id

        frame.prepare()
        frame.ensure_headers()
        return frame

    def _build_send_frame(self,
                          destination,
                          transaction,
                          headers):
        if not destination:
            raise ValueError('Send has no destination')

        frame = self.protocol.module.Send()

        if headers and isinstance(headers, collections.Mapping):
            frame.headers.update(headers)

        frame.headers['destination'] = destination

        self._set_tranaction(frame, transaction)

        frame.prepare()
        frame.ensure_headers()
        return frame

    def _build_disconnect_frame(self,
                                headers):
        frame = self.protocol.module.Disconnect()

        if headers and isinstance(headers, collections.Mapping):
            frame.headers.update(headers)

        frame.prepare()
        frame.ensure_headers()
        return frame

    def _build_ack_frame(self,
                         message_id,
                         subscription,
                         transaction,
                         headers):
        if not message_id:
            raise ValueError('Ack has no message-id')

        frame = self.protocol.module.Ack()

        if headers and isinstance(headers, collections.Mapping):
            frame.headers.update(headers)

        frame.headers['message-id'] = message_id

        if subscription:
            frame.headers['subscription'] = subscription

        self._set_tranaction(frame, transaction)

        frame.prepare()
        frame.ensure_headers()
        return frame

    def _build_nack_frame(self,
                          message_id,
                          subscription,
                          transaction,
                          headers):
        if not message_id:
            raise ValueError('Nack has no message-id')

        frame = self.protocol.module.Nack()

        if headers and isinstance(headers, collections.Mapping):
            frame.headers.update(headers)

        frame.headers['message-id'] = message_id

        if subscription:
            frame.headers['subscription'] = subscription

        self._set_tranaction(frame, transaction)

        frame.prepare()
        frame.ensure_headers()
        return frame

    def _build_begin_frame(self,
                           transaction,
                           headers):
        if not transaction:
            raise ValueError('Begin has no transaction id')

        frame = self.protocol.module.Begin()

        if headers and isinstance(headers, collections.Mapping):
            frame.headers.update(headers)

        self._set_tranaction(frame, transaction)

        frame.prepare()
        frame.ensure_headers()
        return frame

    def _build_abort_frame(self,
                           transaction,
                           headers):
        if not transaction:
            raise ValueError('Abort has no transaction id')

        frame = self.protocol.module.Abort()

        if headers and isinstance(headers, collections.Mapping):
            frame.headers.update(headers)

        self._set_tranaction(frame, transaction)

        frame.prepare()
        frame.ensure_headers()
        return frame

    def _build_commit_frame(self,
                            transaction,
                            headers):
        if not transaction:
            raise ValueError('Commit has no transaction id')

        frame = self.protocol.module.Commit()

        if headers and isinstance(headers, collections.Mapping):
            frame.headers.update(headers)

        self._set_tranaction(frame, transaction)

        frame.prepare()
        frame.ensure_headers()
        return frame

    def _decode_frame_headers(self, data):
        return data.decode(self.protocol.headers_encoding)

    def _set_tranaction(self, frame, transaction):
        if transaction:
            frame.headers['transaction'] = transaction
