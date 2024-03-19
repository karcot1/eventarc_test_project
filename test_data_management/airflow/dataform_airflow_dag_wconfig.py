# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime

from airflow import models
from airflow.models.param import Param
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

DAG_ID = "tdm_datalake_with_config"
PROJECT_ID = "dataform-test-362521"
REPOSITORY_ID = "tdm-dev"
REGION = "us-central1"
GIT_COMMITISH = "main"

with models.DAG(
    DAG_ID,
    schedule_interval=None,
    start_date=datetime(2023, 2, 13),
    catchup=False,
    tags=['dataform'],
) as dag:

    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        params={"workspace_id": "default"}
        retries=0,
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "code_compilation_config": {
                "vars": {
                    ## INSERT RUNTIME VARIABLES HERE ##
                    "example_var": "value1"
                }
            },
            "git_commitish": GIT_COMMITISH,
            "workspace": (
                f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
                f"workspaces/{workspace_id}"
            )
        },
    )

    invoke_workflow = DataformCreateWorkflowInvocationOperator(
        task_id='invoke_workflow',
        retries=0,
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": {"included_tags":["{INCLUDE_TAGS_HERE_IF_NEEDED}"]}
        },
    )

create_compilation_result >> invoke_workflow
