# airflow-qlikview-connector

##Description

This is Apache Airflow connector to QlikView Management System, allowing to execute QlikView Reload Tasks via QMS API.
Tested with Apache Airflow 1.10.5 and QlikView 11.2

##Installation
1. Enter into environment where your Apache Airflow installed.

2. Install dependencies of qlikview connector:
python3 -m pip install requests_ntlm zeep

3. Copy contents of "contrib" folder into your Apache Airflow installation "contrib" folder

##Usage

1. Add new connection in Airflow (Admin->Connections)
  * Conn Id: name of connection, for example: qv_connection
  * Host: QMS service address
  * Login and Password: login and password of account that have right to execute EDX tasks via QMS API
  * Port: Standard port is 4799
  * Extra: Path to QMS API endpoint: /QMS/Service
  
2. Use "QDSReloadApplicationOperator" operator as shown in example of DAG code that you can find in contrib/example_dags/example_qds_operator.py 
 
 