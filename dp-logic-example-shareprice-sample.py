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
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow import configuration
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator

from datetime import datetime
from datetime import timedelta

LAKE_TEAM_EMAIL = ["xyz_group@xyz.com"]
DEPLOY_MODE = os.getenv("DEPLOY_MODE", "ecs")
#AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
AIRFLOW_HOME = os.environ['AIRFLOW_HOME'] + "/dags/mwaa" if DEPLOY_MODE == "mwaa" else os.environ['AIRFLOW_HOME']

ENV = os.environ['ENV']
if ENV == 'pilot':
    EMAILS = ["xyz_group@xyz.com"]
    s3_path = "s3://bucket-pilot"
else:
    EMAILS = LAKE_TEAM_EMAIL
    s3_path = "s3://bucket"

ENV = os.environ['ENV']
BASEURL = configuration.get('webserver', 'base_url')
LOGSBUCKET = s3_path + "/logs" if DEPLOY_MODE == "mwaa" else configuration.get('logging', 'remote_base_log_folder')
DAGS_FOLDER = configuration.get('core', 'dags_folder')
local_tz = pendulum.timezone("Australia/Sydney")


EMR_SCRIPTS_FOLDER = "/usr/local/airflow/scheduler-dag/emr" if DEPLOY_MODE == "mwaa" else "/opt/scheduler-dag/emr"
DAGS_DIRECTORY = "/usr/local/airflow/dags" if DEPLOY_MODE == "mwaa" else "/opt/scheduler-dag/emr"
DAG_SCRIPTS_FOLDER = "bash +x /usr/local/airflow/dags" if DEPLOY_MODE == "mwaa" else "/opt/scheduler-dag/emr"
##########################################
#########

env = {
    'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI': os.environ['AWS_CONTAINER_CREDENTIALS_RELATIVE_URI'],
    'SPARK_HOME': '/opt/spark',
    'AWS_DEFAULT_REGION': 'ap-southeast-2',
    'AWS_REGION': 'ap-southeast-2',
    'HADOOP_CONF_DIR': '/etc/hadoop/conf',
    'HADOOP_USER_NAME': 'hadoop',
    'SPARK_LOCAL_DIRS': '/sparklocal',
    'EMR_SCRIPTS_FOLDER': EMR_SCRIPTS_FOLDER,
    'DAG_SCRIPTS_FOLDER': DAG_SCRIPTS_FOLDER,
    'DAGS_DIRECTORY' : DAGS_DIRECTORY,
}

crb = configuration.get('core', 'sql_alchemy_conn')
bul = configuration.get('celery', 'broker_url')

default_args = {
    'owner': 'EDI',
    'depends_on_past': False,
    'start_date': datetime.strptime('2024-01-01 00:00', '%Y-%m-%d %H:%M').astimezone(local_tz),
    'email': EMAILS,
    'email_on_failure': True,
    'email_on_retry': True,
    'retry_delay': timedelta(minutes=1),
    'retries': 3,
    'catchup': False
}

dag_id = 'dp-logic-example-shareprice-sample'
dag = DAG(
    dag_id=dag_id,
    default_args=default_args,
    description='dp-logic-example-shareprice-sample',
    concurrency=1,
    schedule_interval=None,
    catchup=False,
)


def add_runtime_metrics(jarargument):
    emrClusterId = "{{ ti.xcom_pull('StartEMR').split('|')[0]  if ti.xcom_pull('StartEMR') is not none else 'UNKNOWN' }}"
    emrJobId = "application_xxxxxxxxxxx_xxxx"
    airflowDagName = dag_id
    airflowDagRunId = "{{ run_id }}"
    airflowTaskId = "{{ task.task_id }}"
    run_metrics = f"--emrClusterId {emrClusterId} --emrJobId {emrJobId} --airflowDagName {airflowDagName} --airflowDagRunId {airflowDagRunId} --airflowTaskId {airflowTaskId}"
    parts = re.split('--args\\s+"', jarargument)
    if len(parts) == 2:
        return parts[0] + '--args "' + run_metrics + " " + parts[1]
    else:
        return jarargument


def gen(task_name="", jarargument="", livyargument="", poolslots=1):
    # add metrics from EMR/airflow runtime, to populate soda_job_reconciles columns in spark-orchestrator
    match = re.search(r".+spark-orchestrator-(\d+\.\d+\.\d+).+", "s3://bucket-pilot/shared/spark-orchestrator/spark-orchestrator-2.1.2-SNAPSHOT.jar")
    jar_version = match.group(1) if match else ""
    if "--class au.com.pqr.smg.core.lake.RunTransform" in livyargument and jar_version >= "1.2.0":
        # only run add_runtime_metrics if this dag is generated from dp-framework-lake
        jarargument = add_runtime_metrics(jarargument)

    livy_command = DAG_SCRIPTS_FOLDER + "/runsparklivy.sh --cluster-id {{ ti.xcom_pull('StartEMR').split('|')[0] }} --master-ip {{ ti.xcom_pull('StartEMR').split('|')[1] }} " + livyargument + "  " + jarargument  if DEPLOY_MODE == "mwaa" else EMR_SCRIPTS_FOLDER + "/runsparklivy.sh --cluster-id {{ ti.xcom_pull('StartEMR').split('|')[0] }} --master-ip {{ ti.xcom_pull('StartEMR').split('|')[1] }} " + livyargument + "  " + jarargument
    execution_command = (livy_command.replace('#{myJarPath2}', '')
                         .replace('#{myConfigPath2}', '')
                         .replace('#{myJarPath}', 's3://bucket-pilot/shared/spark-orchestrator/spark-orchestrator-2.1.2-SNAPSHOT.jar')
                         .replace('#{myConfigPath}', 's3://bucket-pilot/shared/dp-logic-example/commit/7c159a26ed90u3t2r86a5c5u18c45ahad83iae6f/scd2_shareprice')
                         .replace('#{myConfigBucket}', 's3://bucket-pilot'))

    return BashOperator(
        task_id=task_name,
        bash_command=execution_command,
        dag=dag,
        pool_slots=poolslots
        )

job_object_names = ['']






dag.tags = ["spot emr"]
cluster_name = f'airflow-{dag_id.replace("_", "-")}'
stack_created_time = datetime.now(tz=local_tz).strftime("%Y%m%d%H%M%S")
stack_name = dag_id.replace("_", "-") + "-" + stack_created_time
stack_env = 'Pilot' if ENV == 'pilot' else 'Prod'

start_emr_template = """#!/bin/bash -ex
    until aws emr list-clusters --active --query "Clusters[?Name == \`{ClusterName}\`].Id" >cluster_list; do
      sleep $((($RANDOM % 20) + 1))
    done
    
    COSTCENTER=$(bash {scriptsPath}/tag.sh {ClusterName})
    echo "CostCenter is set to $COSTCENTER"

    SNOW_GUID=$(bash {scriptsPath}/guidtag.sh {ClusterName})
    echo "Snow GUID is set to $SNOW_GUID"

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
    ParameterKey=CostCenter,ParameterValue=$COSTCENTER \
    ParameterKey=SNowGUId,ParameterValue=$SNOW_GUID \
    --template-body file://{scriptsPath}/{cfTemplateFile}
    
    until aws cloudformation wait stack-create-complete --stack-name {stack_name};do echo "Waiting for the Cloudformation stack to finish...";  sleep  $[ ( $RANDOM % 20 ) + 1 ]; done
    
    cluster_id=$(aws cloudformation list-stack-resources --stack-name {stack_name}| jq -r '.StackResourceSummaries[0].PhysicalResourceId')
    masterpublicdns=$(aws emr describe-cluster --cluster-id $cluster_id --query Cluster.MasterPublicDnsName --output text)
    master_ip=$(echo $masterpublicdns|cut -f1 -d\.|sed -e 's/\-/./g' -e 's/ip.//g')
    echo "$cluster_id|$master_ip|{stack_name}"
""".format(stack_name=stack_name,
           ClusterName=cluster_name,
           OnDemandCapacity=72,
           SpotCapacity=72,
           EnvironmentValue=stack_env,
           scriptsPath=DAGS_DIRECTORY,
           cfTemplateFile='emr-spot-cloudformation_emr615.yaml')

start_emr = BashOperator(
    task_id='StartEMR',
    env=env,
    execution_timeout=timedelta(minutes=40),
    bash_command=start_emr_template,
    do_xcom_push=True,
    dag=dag)

terminate_emr = BashOperator(
    task_id='TerminateSpotEMR',
    env=env,
    trigger_rule="all_done",
    bash_command="""#!/bin/bash -ex
        stack_name='{{ ti.xcom_pull("StartEMR").split('|')[2] }}'
        if [ "$stack_name" == "None" ]; then echo "Failed to get stack_name from xcom!";  exit 1; fi
        aws cloudformation delete-stack --stack-name ${stack_name}
    """,
    dag=dag)




from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

def error_if_task_failed(**kwargs):
    raise AirflowException("Task Failure, because of one or more upstream tasks failed.")

task_monitor_at_job_end = PythonOperator(
    task_id='task_monitor_at_job_end',
    trigger_rule="one_failed",
    python_callable=error_if_task_failed,
    retries=0,
    dag=dag)



daily_ranges_register_glue = gen('daily_ranges_register_glue',' --name daily_ranges_register_glue --args "--executorCores 7 --cloudConf #{myConfigPath}/settings.conf  --logic #{myConfigPath}/daily_ranges_register_glue.json" ','--file #{myJarPath}  --conf spark.sql.broadcastTimeout=3000 --executor-memory 15g --driver-memory 5g  --class au.com.pqr.smg.core.lake.RunTransform ', 1)
daily_ranges_calculate_daily_range = gen('daily_ranges_calculate_daily_range',' --name daily_ranges_calculate_daily_range --args "--executorCores 7 --cloudConf #{myConfigPath}/settings.conf  --logic #{myConfigPath}/daily_ranges_calculate_daily_range.json" ','--file #{myJarPath}  --conf spark.sql.broadcastTimeout=3000 --executor-memory 15g --driver-memory 5g  --class au.com.pqr.smg.core.lake.RunTransform ', 1)
daily_ranges_scd2_daily_ranges = gen('daily_ranges_scd2_daily_ranges',' --name daily_ranges_scd2_daily_ranges --args "--executorCores 7 --cloudConf #{myConfigPath}/settings.conf  --logic #{myConfigPath}/daily_ranges_scd2_daily_ranges.json" ','--file #{myJarPath}  --conf spark.sql.broadcastTimeout=3000 --executor-memory 15g --driver-memory 5g  --class au.com.pqr.smg.core.lake.RunTransform ', 1)
daily_ranges_fetch_prices = gen('daily_ranges_fetch_prices',' --name daily_ranges_fetch_prices --args "--executorCores 7 --cloudConf #{myConfigPath}/settings.conf  --logic #{myConfigPath}/daily_ranges_fetch_prices.json" ','--file #{myJarPath}  --conf spark.sql.broadcastTimeout=3000 --executor-memory 15g --driver-memory 5g  --class au.com.pqr.smg.core.lake.RunTransform ', 1)


daily_ranges_register_glue >> daily_ranges_scd2_daily_ranges
daily_ranges_calculate_daily_range >> daily_ranges_register_glue
start_emr >> daily_ranges_fetch_prices
daily_ranges_scd2_daily_ranges >> task_monitor_at_job_end
daily_ranges_scd2_daily_ranges >> terminate_emr
daily_ranges_fetch_prices >> daily_ranges_calculate_daily_range
