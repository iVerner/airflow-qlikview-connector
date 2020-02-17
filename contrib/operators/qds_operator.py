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

from airflow.contrib.hooks.qds_hook import QlikviewDistributionServerHook

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from zeep.exceptions import Fault
from requests.exceptions import ConnectionError
import time
from datetime import datetime, timedelta


class QDSReloadApplicationOperator(BaseOperator):
    """
       QVReportReloadOperator to execute reload commands on given remote host using the qv_hook.

       :param document_name: path to QlikView application to reload
       :type document_name: str
       :param qv_hook: hook to
       :type qv_hook: :class:QlikviewDistributionServerHook
       :param qv_conn_id: id of connection to QlikView Management System
       :type qv_conn_id: str
       """

    @apply_defaults
    def __init__(self,
                 document_name=None,
                 qv_hook=None,
                 qv_conn_id=None,
                 *args,
                 **kwargs
                 ):
        self.document_name = document_name
        self.qv_hook = qv_hook
        self.qv_conn_id = qv_conn_id
        super(QDSReloadApplicationOperator, self).__init__(*args, **kwargs)

    def execute(self, context):

        if self.qv_conn_id and not self.qv_hook:
            self.log.info("Hook not found, creating...")
            self.qv_hook = QlikviewDistributionServerHook(qv_conn_id=self.qv_conn_id)

        if not self.qv_hook:
            raise AirflowException("Can not operate without qv_hook or qv_conn_id")

        client = self.qv_hook.get_conn()

        service_key = client.service.GetTimeLimitedServiceKey()

        client.transport.session.headers.update({'X-Service-Key': service_key})

        search_task_list = client.service.FindEDX(self.document_name)
        if search_task_list is None:
            raise AirflowException('Task to reload is not finded')
        if len(search_task_list) > 1:
            raise AirflowException('Too many tasks finded')

        reload_task = search_task_list[0]

        self.log.info('Qlikview reload task named: %s', self.document_name)

        execute_status = client.service.TriggerEDXTask(reload_task.QDSID, reload_task.Name, '')
        self.log.info(
            'TriggerEDXTask to task %s. EDXTaskStartResult: %s; EDXTaskStartResultCode: %s; ExecId: %s; Status: %s',
            reload_task.Name,
            execute_status.EDXTaskStartResult,
            execute_status.EDXTaskStartResultCode,
            execute_status.ExecId,
            execute_status.Status)
        if execute_status.EDXTaskStartResult != 'Success':
            raise AirflowException('Task has not started')

        check_sleep_time = 10  # seconds
        check_fail_timeout = 15  # minutes
        last_check_error = None
        while True:
            time.sleep(check_sleep_time)
            status = 'Unknown'
            try:
                service_key = client.service.GetTimeLimitedServiceKey()
                client.transport.session.headers.update({'X-Service-Key': service_key})
                task_status = client.service.GetEDXTaskStatus(reload_task.QDSID, execute_status.ExecId)
                status = task_status.TaskStatus
                last_check_error = None
            except (Fault, ConnectionError,) as e:
                self.log.info("Exception while checking task status. Exception: {0}".format(e))
                if last_check_error is not None:
                    if datetime.now() > last_check_error + timedelta(minutes=check_fail_timeout):
                        raise AirflowException(
                            "Can't check task status for {0} minutes from {1}".format(check_fail_timeout, last_check_error))
                else:
                    last_check_error = datetime.now()

            if status == 'Completed':
                self.log.info('Task completed. StartTime: %s, FinishTime: %s,  TaskStatus: %s; LogFileFullPath: %s',
                              task_status.StartTime,
                              task_status.FinishTime,
                              task_status.TaskStatus,
                              task_status.LogFileFullPath)
                break
            if status == 'Failed':
                self.log.info('Task failed. StartTime: %s, FinishTime: %s,  TaskStatus: %s; LogFileFullPath: %s',
                              task_status.StartTime,
                              task_status.FinishTime,
                              task_status.TaskStatus,
                              task_status.LogFileFullPath)
                raise AirflowException("Task failed with status Failed")
