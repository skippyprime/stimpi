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

import re
import importlib


class EscapedOctets(object):
    def __init__(self,
                 mapping,
                 unescape_pattern):
        self._escape = mapping or {}
        self._unescape = {
            v: k for k, v in self._escape.items()
        }
        self._unescape_pattern = None

        if unescape_pattern:
            self._unescape_pattern = re.compile(unescape_pattern)

    @property
    def escape(self):
        return self._escape

    @property
    def unescape(self):
        return self._unescape

    @property
    def unescape_pattern(self):
        return self._unescape_pattern

    @property
    def escaping(self):
        return self._unescape_pattern is not None


class ProtocolVersion(object):
    body_end_delim = b'\0'

    def __init__(self,
                 version,
                 module_name,
                 line_end='\n',
                 line_end_pattern=r'\n',
                 heartbeat=False,
                 header_encoding='utf-8',
                 disconnect_ack=False,
                 header_escaped_octets=None,
                 header_unescape_pattern=None):
        self._version = version
        self._module_name = module_name
        self._module = None
        self._line_end = line_end
        self._line_end_pattern = re.compile(line_end_pattern)
        self._heartbeat = heartbeat
        self._disconnect_ack = disconnect_ack
        self._heartbeat_pattern = self._line_end_pattern
        self._command_pattern = re.compile(
            r'(?P<command>[A-Z]+)({0:s})'.format(
                line_end_pattern))
        self._header_pattern = re.compile(
            r'(?P<key>[^{0:s}:]+):(?P<value>[^{0:s}]*)({1:s})'.format(
                self._line_end.encode('unicode_escape'),
                line_end_pattern))
        self._header_end_pattern = self._line_end_pattern
        self._header_encoding = header_encoding
        self._header_escaped_octets = EscapedOctets(
            header_escaped_octets,
            header_unescape_pattern)

    @property
    def version(self):
        return self._version

    @property
    def module(self):
        if not self._module:
            self._module = importlib.import_module(
                '..spec.{0:s}'.format(self._module_name),
                self.__class__.__module__)
        return self._module

    @property
    def line_end(self):
        return self._line_end

    @property
    def line_end_pattern(self):
        return self._line_end_pattern

    @property
    def command_pattern(self):
        return self._command_pattern

    @property
    def header_pattern(self):
        return self._header_pattern

    @property
    def header_end_pattern(self):
        return self._header_end_pattern

    @property
    def headers_encoding(self):
        return self._header_encoding

    @property
    def heartbeat(self):
        return self._heartbeat

    @property
    def disconnect_ack(self):
        return self._disconnect_ack

    @property
    def heartbeat_pattern(self):
        return self._heartbeat_pattern

    @property
    def header_escaped_octets(self):
        return self._header_escaped_octets


class ProtocolVersions(object):
    versions = {
        '1.0': ProtocolVersion(
            '1.0',
            'v1_0'
        ),
        '1.1': ProtocolVersion(
            '1.1',
            'v1_1',
            heartbeat=True,
            disconnect_ack=True,
            header_escaped_octets={
                '\n': r'\n',
                ':': r'\c',
                '\\': r'\\'
            },
            header_unescape_pattern=r'\\(\\|n|c)'
        ),
        '1.2': ProtocolVersion(
            '1.2',
            'v1_2',
            line_end='\r\n',
            line_end_pattern=r'\r?\n',
            heartbeat=True,
            disconnect_ack=True,
            header_escaped_octets={
                '\n': r'\n',
                '\r': r'\r',
                ':': r'\c',
                '\\': r'\\'
            },
            header_unescape_pattern=r'\\(\\|n|c|r)'
        ),
    }

    @classmethod
    def is_valid(cls, version):
        return version in cls.versions

    @classmethod
    def load(cls, version):
        return cls.versions[version]
