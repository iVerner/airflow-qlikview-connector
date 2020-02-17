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
# --------------------------------------------------------------------------------
# Written By: Ignat Kudryavtsev
# Caveat: This Dag will not run because of missing QlikView applications.
# The purpose of this is to give you a sample of a real world example DAG!
# --------------------------------------------------------------------------------
"""
### Tutorial Documentation 5
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
# --------------------------------------------------------------------------------
# Load The Dependencies
# --------------------------------------------------------------------------------
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.contrib.operators.qds_operator import QDSReloadApplicationOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 19),
    'email': ['Airflow.Administrator@mycompany.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'pool': 'default_pool',
}

dag = DAG('example_qds_operator',
          description='Reload applications at QlikView Distribution Service',
          catchup=False,
          schedule_interval='0 21 * * 5',
          default_args=default_args)

tasksDict = {
    u'ETL_1.qvw': {},

    u'ETL_2.qvw': {
        'Pool': 'Heavy_ETL_pool',
    },

    u'ETL_3.qvw': {
        'StartTime': [6, 30]},

    u'ETL_4.qvw': {
        'Priority': -5,
        'Dep': [
            u'ETL_1.qvw',
            u'ETL_3.qvw', ]},

    u'ETL_5.qvw': {
        'Dep': [
            u'ETL_2.qvw', ]},

    u'Application_6.qvw': {
        'Dep': [
            u'ETL_1.qvw',
            u'ETL_5.qvw', ]},

    u'Application_7.qvw': {
        'Dep': [
            u'ETL_4.qvw',
            u'ETL_5.qvw',
        ]},
}

airflowTasksDict = {}

for task in tasksDict.keys():
    task_id = task.replace(" ", "_").replace("'", "").replace("/", "_").replace("(", "_").replace(")", "_").replace(",", "_").replace(".qvw", "").replace("__",
                                                                                                                                                          "_")
    AirflowTask = QDSReloadApplicationOperator(document_name=task, task_id=task_id, qv_conn_id='qv_connection', dag=dag)
    airflowTasksDict[task] = AirflowTask

for task in tasksDict.keys():
    if 'Dep' in tasksDict[task]:
        for dep in tasksDict[task]['Dep']:
            airflowTasksDict[task].set_upstream(airflowTasksDict[dep])

    if 'Pool' in tasksDict[task]:
        airflowTasksDict[task].pool = tasksDict[task]['Pool']

    if 'Priority' in tasksDict[task]:
        airflowTasksDict[task].priority_weight = tasksDict[task]['Priority']

    if 'StartTime' in tasksDict[task]:
        hour = tasksDict[task]['StartTime'][0]
        minute = tasksDict[task]['StartTime'][1]
        sensorTime = timedelta(hours=hour, minutes=minute)
        sensorTaskID = u'TimeSensor_{}_{}'.format(hour, minute)

        if sensorTaskID not in airflowTasksDict:
            SensorTask = TimeDeltaSensor(delta=sensorTime, task_id=sensorTaskID, pool='Sensors', dag=dag)
            airflowTasksDict[sensorTaskID] = SensorTask
        airflowTasksDict[task].set_upstream(airflowTasksDict[sensorTaskID])

if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
