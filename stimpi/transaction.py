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

from .errors import TransactionError


class Transaction(object):
    def __init__(self,
                 connection,
                 id):
        self._connection = connection
        self._id = id
        self._is_open = True

    @property
    def connection(self):
        return self._connection

    @property
    def id(self):
        return self._id

    def _ensure_open(self):
        if not self._is_open:
            raise TransactionError('Transaction already commited or aborted')

    def abort(self, **kwargs):
        self._ensure_open()
        self._is_open = False
        return self._connection.abort(self._id, **kwargs)
        pass

    def commit(self, **kwargs):
        self._ensure_open()
        self._is_open = False
        return self._connection.commit(self._id, **kwargs)
        pass

    def ack(self, *args, **kwargs):
        self._ensure_open()
        return self._connection.ack(*args, transaction=self._id, **kwargs)

    def nack(self, *args, **kwargs):
        self._ensure_open()
        return self._connection.nack(*args, transaction=self._id, **kwargs)

    def send(self, *args, **kwargs):
        self._ensure_open()
        return self._connection.send(*args, transaction=self._id, **kwargs)
        pass
