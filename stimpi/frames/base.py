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

try:
    from flufl.enum import Enum
except ImportError:
    # use native enum support -- python 3.4+
    from enum import Enum

import six

from .mapping import StompHeaders
from .versions import ProtocolVersions
from ..errors import FrameError


class FrameSource(Enum):
    # pylint: disable=no-init
    client = 'CLIENT'
    server = 'SERVER'


class FrameDefinition(object):
    def __init__(self,
                 verb,
                 abstract,
                 source,
                 headers_required,
                 escape_headers,
                 protocol=None):
        self.verb = verb
        self.abstract = abstract
        self.source = FrameSource[source or FrameSource.client]
        self.headers_required = headers_required or ()
        self.protocol = protocol
        self.escape_headers = escape_headers


class FrameMeta(type):
    def __new__(cls, name, bases, attrs):
        abstract = attrs.pop('abstract', False)
        version = attrs.pop('version', None)
        headers_required = attrs.pop('headers_required', ())
        source = attrs.pop('source', None)
        verb = attrs.pop('verb', None)
        escape_headers = attrs.pop('escape_headers', None)

        cls_new = super(FrameMeta, cls).__new__(
            cls,
            name,
            bases,
            attrs
        )

        # create the defintion, inheriting elements of the base class(es)
        base_definition = getattr(cls_new, 'definition', None)
        if base_definition is not None:
            headers_required = set(headers_required)
            headers_required.update(base_definition.headers_required)
            source = source or base_definition.source
            verb = verb or base_definition.verb
            if escape_headers is None:
                escape_headers = base_definition.escape_headers

        if escape_headers is None:
            escape_headers = True

        definition = FrameDefinition(verb,
                                     abstract,
                                     source,
                                     tuple(headers_required),
                                     escape_headers)

        setattr(cls_new, 'definition', definition)

        # track this frame against the correct protocol suites
        frame_registry = getattr(cls_new, 'registry', None)
        if frame_registry is None:
            frame_registry =\
                {version: {} for version in ProtocolVersions.versions}
            setattr(cls_new, 'registry', frame_registry)

        if not abstract:
            if version is None or not ProtocolVersions.is_valid(version):
                raise ValueError(
                    'Missing or unsupported protocol version "{}" for'
                    'type "{}"'.format(version, name))
            frame_registry[version][verb] = cls_new
            definition.protocol = ProtocolVersions.load(version)

        # new must return the newly allocated type
        return cls_new


@six.add_metaclass(FrameMeta)
class Frame(object):
    definition = None
    abstract = True

    def __init__(self, headers=None):
        self._headers = None
        self._body = None
        if headers is None:
            self._headers = StompHeaders()
        else:
            self._headers = StompHeaders(headers)

    @property
    def headers(self):
        return self._headers

    @property
    def body(self):
        return self._body

    @body.setter
    def body(self, value):
        self._body = value

    def __str__(self):
        stream = six.BytesIO()
        stream.write(self._dumps_headers(stream=stream))
        if self.body:
            if isinstance(self.body, str):
                stream.write(self.body.decode('latin-1'))
            else:
                stream.write(self.body)
        stream.write('^@')
        return stream.getvalue()

    def ensure_headers(self):
        return set(self.definition.headers_required) <=\
            set(self.headers.keys())

    def _dumps_headers(self, stream=None):
        return_value = stream is None
        stream = stream or six.BytesIO()

        line_end_enc = self.definition.protocol.line_end.encode(
            self.definition.protocol.headers_encoding)

        stream.write(self.definition.verb.encode(
            self.definition.protocol.headers_encoding))
        stream.write(line_end_enc)

        for key, values in self.headers.iter_history():
            for value in values:
                stream.write('{0:s}:{1}'.format(key, value).encode(
                    self.definition.protocol.headers_encoding))
                stream.write(line_end_enc)

        stream.write(line_end_enc)

        if return_value:
            return stream.getvalue()

    def dumps(self, with_body=True):
        stream = six.BytesIO()
        self._dumps_headers(stream)

        if with_body and self.body:
            stream.write(self.body)
            stream.write(self.definition.protocol.body_end_delim)

        return stream.getvalue()

    def prepare(self):
        pass

    @classmethod
    def _unescape_header_octets_repl(cls, match):
        repl = cls.definition.protocol.header_escaped_octets.unescape.get(
            match.group(0), None)
        return repl

    @classmethod
    def _unescape_header_octets(cls, value):
        unescaped_value = value

        # check if the frame and protocol version escape headers
        if cls.definition.escape_headers and\
                cls.definition.protocol.header_escaped_octets.escaping:
            unescaped_value = cls.definition.protocol.\
                header_escaped_octets.unescape_pattern.\
                sub(cls._unescape_header_octets_repl, value)

        return unescaped_value

    def _escape_header_octets(self, value):
        escaped_value = value

        # check if the frame and protocol version escape headers
        if self.definition.escape_headers and\
                self.definition.protocol.header_escaped_octets.escaping:
            pre_escaped_octet = '\\'
            pre_escpaed_seq = self.definition.protocol.\
                header_escaped_octets.escape.get(pre_escaped_octet, None)
            if pre_escpaed_seq is not None:
                escaped_value = escaped_value.replace(pre_escaped_octet,
                                                      pre_escpaed_seq)

            for octet, escaped_seq in self.definition.protocol.\
                    header_escaped_octets.escape.iteritems():
                if octet == pre_escaped_octet:
                    continue
                escaped_value = escaped_value.replace(octet,
                                                      escaped_seq)

        return escaped_value

    def loads_header(self, data, start=0, end=None, protocol=None):
        if not end:
            end = len(data)

        protocol = protocol or self.definition.protocol

        match = protocol.header_pattern.match(data, start, end)
        if not match:
            raise FrameError('Header not found.')
        if match.start() > start:
            raise FrameError('Extra data before header.')

        header_key = self._unescape_header_octets(match.groupdict()['key'])
        header_value = self._unescape_header_octets(match.groupdict()['value'])
        self.headers[header_key] =\
            self._decode_known_header(header_key, header_value)

        return match.end()

    @classmethod
    def _decode_known_header(cls, header, value):
        header = header.lower()
        if header == 'content-length':
            return int(value)

        return value

    @classmethod
    def loads(cls, protocol, data):
        frames = cls.registry[protocol.version]
        last_pos = 0

        # parse the command name
        match = protocol.command_pattern.search(data)
        if not match:
            raise FrameError('Command not found.')
        if match.start() > last_pos:
            raise FrameError('Extra data before command.')
        command = match.groupdict()['command']
        last_pos = match.end()

        # create a frame of that type
        frame = frames[command]()

        # parse the headers
        for match in protocol.header_pattern.finditer(data, last_pos):
            if match.start() > last_pos:
                raise FrameError('Extra data before header.')
            header_key = frame._unescape_header_octets(
                match.groupdict()['key'])
            header_value = frame._unescape_header_octets(
                match.groupdict()['value'])
            frame.headers[header_key] =\
                cls._decode_known_header(header_key, header_value)

            last_pos = match.end()

        # consume the empty line
        match = protocol.line_end_pattern.match(data, last_pos)
        if not match:
            raise FrameError('Missing EOL following last header.')
        last_pos = match.end()

        return frame, last_pos

    @classmethod
    def loads_command(cls, protocol, data):
        frames = cls.registry[protocol.version]

        # parse the command name
        match = protocol.command_pattern.match(data)
        if not match:
            raise FrameError('Command not found.')
        command = match.groupdict()['command']

        # create a frame of that type
        return frames[command]()


class ClientFrame(Frame):
    abstract = True
    source = FrameSource.client


class ServerFrame(Frame):
    abstract = True
    source = FrameSource.server
