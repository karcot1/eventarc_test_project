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
REPOSITORY_ID = "dataform-etl-test"
WORKSPACE_ID = "dataform-etl-dev"
REGION = "us-central1"
GIT_COMMITISH = "main"

dynamic_inputs = {
    "retail_account":{
        "dag_id": "retail_account_build",
        "schedule_interval": "@daily",
        "catchup": False,
        "table": "retail_account_table"
    },
    "customer": {
        "dag_id": "customer_account_build",
        "schedule_interval": "@daily",
        "catchup": False,
        "table": "customer_table"
    }
}

def create_dag(PROJECT_ID, REPOSITORY_ID, WORKSPACE_ID, REGION, GIT_COMMITISH, dag_id, schedule, catchup, table):
    generated_dag = DAG(dag_id, start_date=datetime(2023, 10, 19))

    with generated_dag:
        create_compilation_result = DataformCreateCompilationResultOperator(
            task_id="create_compilation_result",
            retries=0,
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            compilation_result={
                "code_compilation_config": {
                    "vars": {
                        "analytical_domain_table": table
                    }
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
    dag_id = configs["dag_id"]
    schedule = configs["schedule_interval"]
    catchup = configs["catchup"]
    table = configs["table"]

    globals()[dag_id] = create_dag(PROJECT_ID, REPOSITORY_ID, WORKSPACE_ID, REGION, GIT_COMMITISH, dag_id, schedule, catchup, table)