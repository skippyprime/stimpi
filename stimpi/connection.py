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

import ssl
import inspect
import collections

from six.moves.urllib import parse

from .credentials import PasswordCredentials


class ConnectionParameters(object):
    def __init__(self,
                 host='localhost',
                 port=61613,
                 credentials=None,
                 ssl_options=None,
                 vhost=None):
        self._host = host or 'localhost'
        self._port = port or 61613
        self._credentials = credentials
        self._ssl_options = ssl_options
        self._vhost = vhost or host

        if self._port < 1 or self._port > 65535:
            raise ValueError('Port invalid, out of range')

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def vhost(self):
        return self._vhost

    @property
    def credentials(self):
        return self._credentials

    @property
    def ssl_options(self):
        return self._ssl_options


class URLParameters(ConnectionParameters):
    def __init__(self,
                 connection_string='stomp://localhost:61613/localhost'):
        self._connection_string = connection_string or\
            'stomp://localhost:61613/localhost'

        kwargs = URLParameters.parse_connection_string(self._connection_string)

        super(URLParameters, self).__init__(**kwargs)

    @property
    def connection_string(self):
        return self._connection_string

    @classmethod
    def parse_connection_string(cls, connection_string):
        ssl_schemes = set(['stomps', 'stomp+ssl', 'stomp+tls'])
        kwargs = {}

        index = connection_string.find(':')
        scheme = connection_string[:index].lower()

        # pretend that we are http to get parameters supported
        parts = parse.urlparse(connection_string[index+1:], scheme='http')

        # create credentials holder for username and password if provided
        if parts.username or parts.password:
            kwargs['credentials'] = PasswordCredentials(
                parse.unquote_plus(parts.username or ''),
                parse.unquote_plus(parts.password or ''))

        if parts.hostname:
            kwargs['host'] = parts.hostname

        if parts.port:
            kwargs['port'] = parts.port

        if parts.path:
            path = parts.path[1:]
            if path:
                kwargs['vhost'] = path

        params = cls.get_connection_params(parts.params)

        if scheme in ssl_schemes:
            kwargs['ssl_options'] = cls.parse_ssl_options(params)

        return kwargs

    @classmethod
    def parse_ssl_options(cls, params):
        cert_verify_mapping = {
            'NONE': ssl.CERT_NONE,
            'REQUIRED': ssl.CERT_REQUIRED
        }

        ssl_versions = {}
        for protocol_attr in dir(ssl):
            if inspect.ismethod(protocol_attr):
                continue
            if protocol_attr.startswith('PROTOCOL_'):
                ssl_versions[protocol_attr[9:].upper()] =\
                    getattr(ssl, protocol_attr)

        ssl_options = {}
        ssl_options['keyfile'] = params.pop('keyfile', None)
        ssl_options['certfile'] = params.pop('certfile', None)
        ssl_options['ca_certs'] = params.pop('cafile', None)

        cert_verify_param = params.pop('certverify', 'NONE').upper()
        try:
            ssl_options['cert_reqs'] =\
                cert_verify_mapping[cert_verify_param]
        except KeyError:
            raise ValueError(
                '"{}" is an invalid certverify option, valid '
                'values are {}'.format(
                    cert_verify_param,
                    ', '.join(cert_verify_mapping.keys())))

        # get the optional SSL/TLS protocol version
        ssl_version_param = params.pop('sslversion', None)
        if ssl_version_param is not None:
            try:
                ssl_options['ssl_version'] =\
                    ssl_versions[ssl_version_param.upper()]
            except KeyError:
                raise ValueError(
                    '"{}" is an invalid SSL/TLS version, valid '
                    'values are {}'.format(
                        ssl_version_param,
                        ', '.join(ssl_versions.keys())))

        # get the optional cipher list
        ssl_ciphers_param = params.pop('sslciphers', None)
        if ssl_ciphers_param is not None:
            ssl_options['ciphers'] = ssl_ciphers_param

        return ssl_options

    @classmethod
    def get_connection_params(cls, params):
        if not params:
            return {}

        parsed_params = parse.parse_qs(params)

        for key in list(parsed_params.keys()):
            value = parsed_params[key]
            if value is not None and\
                    isinstance(value, collections.Sequence) and\
                    len(value) == 1:
                parsed_params[key] = value[0]

        return parsed_params
