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


class Ack(ClientFrame):
    abstract = True


class Ack10(Ack):
    version = '1.0'
    verb = 'ACK'
    headers_required = ('message-id', )


class Ack11(Ack10):
    version = '1.1'
    headers_required = ('subscription', )


class Ack12(Ack11):
    version = '1.2'
