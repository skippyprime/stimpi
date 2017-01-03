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

from ..base import ClientFrame


class Connect(ClientFrame):
    abstract = True

    def set_vhost(self, vhost):
        pass


class Connect10(Connect):
    # pylint: disable=no-init
    version = '1.0'
    verb = 'CONNECT'
    escape_headers = False


class Connect11(Connect10):
    # pylint: disable=no-init
    version = '1.1'
    headers_required = ('accept-version', 'host')

    def set_vhost(self, vhost):
        # pylint: disable=E1002
        super(Connect11, self).set_vhost(vhost)
        self.headers['host'] = vhost

    def prepare(self):
        # pylint: disable=E1002
        super(Connect11, self).prepare()
        self.headers['accept-version'] = self.definition.protocol.version


class Connect12(Connect11):
    # pylint: disable=no-init
    version = '1.2'
    verb = 'STOMP'
