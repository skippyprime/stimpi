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
import re
import abc

import six


class BaseHandler(object):
    def __init__(self,
                 connection):
        self._connection = connection

    @property
    def connection(self):
        return self._connection

    def initialize(self, **kwargs):
        if kwargs:
            logger = logging.getLogger(__name__)
            logger.warn('Unhandled initializer arguments for %s',
                        self.__class__.__name__)
        pass


@six.add_metaclass(abc.ABCMeta)
class ErrorHandler(BaseHandler):
    def __init__(self,
                 connection,
                 error):
        super(ErrorHandler, self).__init__(connection)
        self._error = error

    @property
    def error(self):
        return self._error

    @abc.abstractmethod
    def handle(self):
        pass


@six.add_metaclass(abc.ABCMeta)
class MessageHandler(BaseHandler):
    def __init__(self,
                 connection,
                 message):
        super(MessageHandler, self).__init__(connection)
        self._message = message

    @property
    def message(self):
        return self._message

    @abc.abstractmethod
    def received(self):
        pass


class DestinationSpec(object):
    def __init__(self,
                 pattern,
                 handler,
                 kwargs=None):
        if not pattern:
            raise ValueError('No pattern specified')

        if handler is None:
            raise ValueError('No handler specified')

        if not issubclass(handler, MessageHandler):
            raise ValueError(
                'Specified handler does not extend MessageHandler')

        if not pattern.endswith('$'):
            # help the user and match to the end of the destination string
            pattern += '$'

        try:
            self._pattern = re.compile(pattern)
        except re.error as e:
            raise ValueError(
                'Pattern is not a valid regular expression: {}'.format(
                    e.message))

        self._handler = handler
        self._kwargs = kwargs or {}

    @property
    def pattern(self):
        return self._pattern

    @property
    def handler(self):
        return self._handler

    @property
    def kwargs(self):
        return self._kwargs
