'''
* Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* SPDX-License-Identifier: MIT-0
*
* Permission is hereby granted, free of charge, to any person obtaining a copy of this
* software and associated documentation files (the "Software"), to deal in the Software
* without restriction, including without limitation the rights to use, copy, modify,
* merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
* permit persons to whom the Software is furnished to do so.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
* INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
* PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
* HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
* OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
* SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''
 
import os
import pendulum
import re
from airflow import DAG
from airflow import configuration
from airflow.operators.bash_operator import BashOperator
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta
 
EMAIL_GROUP = ["xyz_group@xyz.com"]
AIRFLOW_HOME = os.environ['AIRFLOW_HOME'] + "/dags/mwaa"
EMAILS = ["xyz_group@xyz.com"]
s3_path = "s3://bucket-pilot"
 
BASEURL = configuration.get('webserver', 'base_url')
LOGSBUCKET = s3_path + "/logs"
DAGS_FOLDER = configuration.get('core', 'dags_folder')
local_tz = pendulum.timezone("Australia/Sydney")
 
env = {
    'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI': os.environ['AWS_CONTAINER_CREDENTIALS_RELATIVE_URI'],
    'SPARK_HOME': '/opt/spark',
    'AWS_DEFAULT_REGION': 'ap-southeast-2',
    'AWS_REGION': 'ap-southeast-2',
    'HADOOP_CONF_DIR': '/etc/hadoop/conf',
    'HADOOP_USER_NAME': 'hadoop',
    'SPARK_LOCAL_DIRS': '/sparklocal',
    'EMR_SCRIPTS_FOLDER': "/usr/local/airflow/scheduler-dag/emr",
    'DAG_SCRIPTS_FOLDER': "/usr/local/airflow/dags",
    'DAGS_DIRECTORY' : "/usr/local/airflow/dags",
}
 
crb = configuration.get('core', 'sql_alchemy_conn')
bul = configuration.get('celery', 'broker_url')
 
default_args = {
    'owner': 'xyz',
    'depends_on_past': False,
    'start_date': datetime.strptime('2024-01-01 00:00', '%Y-%m-%d %H:%M').astimezone(local_tz),
    'email': EMAILS,
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),
    'retries': 3,
    'catchup': False
}
 
dag_id = 'example-shareprice'
dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    description='example-shareprice',
    concurrency=4,
    schedule_interval=None,
    catchup=False,
)
 
def prepare_spark_command(task_name="", jarargument="", livyargument=""):
    spark_command = env['DAG_SCRIPTS_FOLDER'] + "/runsparklivy.sh --cluster-id {{ ti.xcom_pull('StartEMR').split('|')[0] }} --master-ip {{ ti.xcom_pull('StartEMR').split('|')[1] }} " + livyargument + " " + jarargument
    execution_command = (spark_command
                         .replace('#{s3_jar_path}', 's3://bucket-pilot/spark-orchestrator/spark-orchestrator-2.1.2-SNAPSHOT.jar')
                         .replace('#{s3_config_path}', 's3://bucket-pilot/spark-config/example_shareprice'))
 
    return BashOperator(
        task_id=task_name,
        bash_command=execution_command,
        dag=dag
        )
 
dag.tags = ["spot emr"]
cluster_name = f'mwaa-{dag_id.replace("_", "-")}'
stack_created_time = datetime.now(tz=local_tz).strftime("%Y%m%d%H%M%S")
stack_name = dag_id.replace("_", "-") + "-" + stack_created_time
stack_env = 'Pilot'
 
start_emr_template = """#!/bin/bash -ex
    until aws emr list-clusters --active --query "Clusters[?Name == \`{ClusterName}\`].Id" >cluster_list; do
      sleep $((($RANDOM % 20) + 1))
    done
  
    if [ "$(jq length cluster_list)" != "0" ]; then
      cluster_ids=$(jq -r .[] cluster_list)
      echo "[ '$cluster_ids' ] Clusters with the same name exists"
      exit 1
    fi
    
    echo "Creating new EMR cluster with spot instances..."
    aws cloudformation create-stack --stack-name {stack_name} --parameters \
    ParameterKey=ClusterName,ParameterValue='{ClusterName}' \
    ParameterKey=OnDemandCapacity,ParameterValue='{OnDemandCapacity}' \
    ParameterKey=SpotCapacity,ParameterValue='{SpotCapacity}' \
    ParameterKey=EnvironmentValue,ParameterValue='{EnvironmentValue}' \
    --template-body file://{scriptsPath}/{cfTemplateFile}
    
    until aws cloudformation wait stack-create-complete --stack-name {stack_name};do echo "Waiting for the Cloudformation stack to finish...";  sleep  $[ ( $RANDOM % 20 ) + 1 ]; done
    
    cluster_id=$(aws cloudformation list-stack-resources --stack-name {stack_name}| jq -r '.StackResourceSummaries[0].PhysicalResourceId')
    masterpublicdns=$(aws emr describe-cluster --cluster-id $cluster_id --query Cluster.MasterPublicDnsName --output text)
    master_ip=$(echo $masterpublicdns|cut -f1 -d\.|sed -e 's/\-/./g' -e 's/ip.//g')
    echo "$cluster_id|$master_ip|{stack_name}"
""".format(stack_name=stack_name,
           ClusterName=cluster_name,
           OnDemandCapacity=16,
           SpotCapacity=32,
           EnvironmentValue=stack_env,
           scriptsPath=env['DAGS_DIRECTORY'],
           cfTemplateFile='emr-spot-cloudformation.yaml')

start_emr = BashOperator(
    task_id='StartEMR',
    env=env,
    execution_timeout=timedelta(minutes=30),
    bash_command=start_emr_template,
    do_xcom_push=True,
    dag=dag)

terminate_emr = BashOperator(
    task_id='TerminateSpotEMR',
    env=env,
    trigger_rule="all_done",
    bash_command="""#!/bin/bash -ex
        stack_name='{{ ti.xcom_pull("StartEMR").split('|')[2] }}'
        if [ "$stack_name" == "None" ]; then echo "Failed to get emr stack_name from xcom!";  exit 1; fi
        aws cloudformation delete-stack --stack-name ${stack_name}
    """,
    dag=dag)
 
register = prepare_spark_command('register',' --args "--executorCores 5 --config_json #{s3_config_path}/register.json" ','--file #{s3_jar_path} --conf spark.sql.broadcastTimeout=3000 --executor-memory 15g --driver-memory 5g --class com.xyz.spark.etl.main.Orchestrator')
calculate_daily_range = prepare_spark_command('calculate_daily_range',' --args "--executorCores 5 --config_json #{s3_config_path}/calculate_daily_range.json" ','--file #{s3_jar_path} --conf spark.sql.broadcastTimeout=3000 --executor-memory 15g --driver-memory 5g --class com.xyz.spark.etl.main.Orchestrator')
scd2_daily_ranges = prepare_spark_command('scd2_daily_ranges',' --args "--executorCores 5 --config_json #{s3_config_path}/scd2_daily_ranges.json" ','--file #{s3_jar_path} --conf spark.sql.broadcastTimeout=3000 --executor-memory 15g --driver-memory 5g --class com.xyz.spark.etl.main.Orchestrator')
fetch_prices = prepare_spark_command('fetch_prices',' --args "--executorCores 5 --config_json #{s3_config_path}/fetch_prices.json" ','--file #{s3_jar_path} --conf spark.sql.broadcastTimeout=3000 --executor-memory 15g --driver-memory 5g --class com.xyz.spark.etl.main.Orchestrator')

start_emr >> fetch_prices >> calculate_daily_range >> register >> scd2_daily_ranges >> terminate_emr