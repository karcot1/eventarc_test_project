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
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator,
)

PROJECT_ID = "dataform-test-362521"
REPOSITORY_ID = "test_data_management_example"
WORKSPACE_ID = "dev"
REGION = "us-central1"
GIT_COMMITISH = "main"
CUR_DIR = os.path.abspath(os.path.dirname(__file__))

try: 
    f = open(f"{CUR_DIR}/analytical_domains.json")
    dynamic_inputs = json.load(f)

except Exception as e:
    print("Unable to read JSON file: ",e)

def create_dag(PROJECT_ID, REPOSITORY_ID, WORKSPACE_ID, REGION, GIT_COMMITISH, dag_id, vars):
    generated_dag = DAG(dag_id, start_date=datetime(2023, 10, 19), catchup=False)

    with generated_dag:
        create_compilation_result = DataformCreateCompilationResultOperator(
            task_id="create_compilation_result",
            retries=0,
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            compilation_result={
                "code_compilation_config": {
                    "vars": vars
                },
                "git_commitish": GIT_COMMITISH,
                "workspace": (
                    f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
                    f"workspaces/{WORKSPACE_ID}"
                )
            },
        ),

        workflow_invocation = DataformCreateWorkflowInvocationOperator(
            task_id='execute_dataform',
            retries=0,
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation={
                "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
                "invocation_config": {"included_tags":[analytical_domain]}
            },
        )

        create_compilation_result >> workflow_invocation

    return generated_dag

for analytical_domain, configs in dynamic_inputs.items():
    dag_id = "{}_tdm".format(configs["dag_id"])
    schedule = configs["schedule_interval"]

    try: 
        f = open(f"{CUR_DIR}/{analytical_domain}_config.json")
        vars = json.load(f)

    except Exception as e:
        print("Unable to read JSON file: ",e)

    globals()[dag_id] = create_dag(PROJECT_ID, REPOSITORY_ID, WORKSPACE_ID, REGION, GIT_COMMITISH, dag_id, vars)
