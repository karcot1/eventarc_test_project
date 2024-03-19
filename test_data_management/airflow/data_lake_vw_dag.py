# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from airflow import DAG
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

DAG_ID = "data_lake_vw_dag"
PROJECT_ID = "dataform-test-362521"
REPOSITORY_ID = "test_data_management_example"
WORKSPACE_ID = "dev"
REGION = "us-central1"
GIT_COMMITISH = "main"
CUR_DIR = os.path.abspath(os.path.dirname(__file__))

try: 
    f = open(f"{CUR_DIR}/test_objects.txt")
    data = f.read().replace('\n', '')
    print(data)

except Exception as e:
    print("Unable to read txt file: ",e)

with DAG  (
    dag_id=DAG_ID,
    description="TDM Data Lake example DAG",
    start_date=datetime(2023, 11, 22),
    schedule_interval="@daily",
    catchup=False
    ) as dag:

    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        retries=0,
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "code_compilation_config": {
                "vars": {
                    "test_objects": f"{data}"
                }
            },
            "git_commitish": GIT_COMMITISH,
            "workspace": (
                f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
                f"workspaces/{WORKSPACE_ID}"
            )
        }
    )

    workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='execute_dataform',
        retries=0,
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": {"included_tags":["data_lake"]}
        }
    )

    create_compilation_result >> workflow_invocation
