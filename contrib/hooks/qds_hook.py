# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin

from requests import Session
from requests_ntlm import HttpNtlmAuth  # from https://pypi.python.org/pypi/requests_ntlm/0.3.0

from zeep import Client
from zeep.transports import Transport


class QlikviewDistributionServerHook(BaseHook, LoggingMixin):
    """
    Hook for remote execution QlikView Distribution Service tasks using zeep.

    :seealso: https://python-zeep.readthedocs.io/en/master/

    :param qv_conn_id: connection id from airflow Connections from where all
        the required parameters can be fetched like username and password.
        Thought the priority is given to the param passed during init
    :type qv_conn_id: str
    :param remote_host: Remote host to connect to.
        Ignored if `endpoint` is not `None`.
    :type remote_host: str
    :param remote_port: Remote port to connect to.
        Ignored if `endpoint` is not `None`.
    :type remote_port: int
    :param endpoint: When set to `None`, endpoint will be constructed like this:
        'http://{remote_host}:{remote_port}/QMS/Service'
    :type endpoint: str
    :param username: username to connect to the remote_host
    :type username: str
    :param password: password of the username to connect to the remote_host
    :type password: str
    """

    def __init__(self,
                 qv_conn_id=None,
                 remote_host=None,
                 remote_port=4799,
                 endpoint='/QMS/Service',
                 username=None,
                 password=None
                 ):
        super(QlikviewDistributionServerHook, self).__init__(qv_conn_id)
        self.qv_conn_id = qv_conn_id
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.client = None

    def get_conn(self):

        if self.client:
            return self.client

        self.log.debug('Creating QV client for conn_id: %s', self.qv_conn_id)

        if self.qv_conn_id is not None:
            conn = self.get_connection(self.qv_conn_id)
            if self.username is None:
                self.username = conn.login
            if self.password is None:
                self.password = conn.password
            if self.remote_host is None:
                self.remote_host = conn.host
            if self.remote_port is None:
                self.remote_port = conn.port
            if self.endpoint is None:
                self.endpoint = conn.extra

        if not self.remote_host:
            raise AirflowException("Missing required param: remote_host")

        # If endpoint is not set, then build a standard wsdl endpoint from host, port and extra.
        if not self.endpoint:
            self.endpoint = 'http://{0}:{1}{2}'.format(
                self.remote_host,
                self.remote_port,
                self.endpoint
            )

        session = Session()
        session.auth = HttpNtlmAuth(self.username, self.password)

        wsdl = "{0}:{1}{2}".format(self.remote_host, self.remote_port, self.extra)
        self.client = Client(wsdl, transport=Transport(session=session))

        return self.client
