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

import collections


class StompHeaders(collections.MutableMapping):
    """A dictionary like collection of STOMP headers.  The keys for each
    header are case insensitive
    """

    def __init__(self, *args, **kwargs):
        self.__headers = dict()
        self.update(*args, **kwargs)

    def __getitem__(self, key):
        return self.__headers[self.__transform_key(key)][0]

    def __setitem__(self, key, value):
        t_key = self.__transform_key(key)
        entries = self.get_history(t_key)
        if entries is None:
            entries = HeaderEntry()
            self.__headers[t_key] = entries
        entries.append(value)

    def __delitem__(self, key):
        del self.__headers[self.__transform_key(key)]

    def __iter__(self):
        return iter(self.__headers)

    def __len__(self):
        return len(self.__headers)

    def __transform_key(self, key):
        """Applies the lowercase transform to all keys before storage.  If the
        supplied key is not a string, it is returned unmodified.
        """
        if isinstance(key, basestring):
            return key.lower()
        else:
            return key

    def get(self, key, default=None):
        result = self.get_history(key)
        if result is not None:
            return result[0]
        return default

    def get_history(self, key, default=None):
        """Like get, but returns a sequence of all values associated with
        this header.  Current value is the first item in the sequence.
        """
        result = None
        try:
            result = self.__headers[self.__transform_key(key)]
        except KeyError:
            pass

        if result is not None:
            return result

        if default is not None:
            if not isinstance(default, collections.Iterable):
                raise TypeError('{:s} is not an iterable'.format(
                    type(default)))

        return None

    def iter_history(self):
        """Like iteritems(), but iterates over the headers and full history
        of values.
        """
        for key, value in self.__headers.iteritems():
            yield key, value

    def set(self, key, value, with_history=False):
        old = self.get_history(key)
        if old is None:
            self[key] = value
            return

        if with_history:
            old.insert(0, value)


class HeaderEntry(collections.MutableSequence):
    """Maintains the complete history of all headers specified for a single
    key.  This is maintained as a sequence, including the current value at
    posution 0.

    As defined in the STOMP protocol, headers may be repeated.  The first
    encountered is considered the current value, with duplicates considered
    history.  The history is maintained in this sequence.
    """
    def __init__(self, iterable=None):
        self.__entries = []
        if iterable is not None:
            self.__entries.extend(iterable)

    def __getitem__(self, index):
        return self.__entries[index]

    def __setitem__(self, index, value):
        self.__entries[index] = value

    def __delitem__(self, index):
        del self.__entries[index]

    def __len__(self):
        return len(self.__entries)

    def __str__(self):
        return str(self.__entries)

    def insert(self, index, value):
        self.__entries.insert(index, value)

    @property
    def value(self):
        """Gets the current value associated with this header."""
        return self.__entries[0]

    @property
    def previous(self):
        """Gets the previous values associsated with this header as an ordered
        set, excluding the current value.
        """
        return self.__entries[1:]
