from airflow import models
from datetime import date, timedelta, datetime
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

PROJECT_ID = "dataform-test-362521"
REPOSITORY_ID = "test_data_management_example"
WORKSPACE_ID = "dev"
REGION = "us-central1"
GIT_COMMITISH = "main"

start_date = date(2024,1,1)
end_date = date(2024,1,19)
concurrency = 3

def get_days_array(start, end, no_of_days):
  current_date = start
  result = []
  while current_date < end:
    end_date = min(current_date + timedelta(days=no_of_days), end)  # Calculate end of range
    result.append(f"{current_date.isoformat()[:10]}:{end_date.isoformat()[:10]}")
    current_date += timedelta(days=no_of_days)  # Move to next start date
  return result
    
def create_compilation_result(count,daylist):
    compilation_result = DataformCreateCompilationResultOperator(
        task_id=f"create_compilation_{count}",
        retries=0,
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "code_compilation_config": {
                "vars": {
                    "daylist": daylist
                }
            },
            "git_commitish": GIT_COMMITISH,
            "workspace": (
                f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{REPOSITORY_ID}/"
                f"workspaces/{WORKSPACE_ID}"
            )
        },
    )

    return compilation_result

def create_workflow_invocation(count):
    workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id=f'execute_dataform_{count}',
        retries=0,
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{" + f"{{ task_instance.xcom_pull('create_compilation_{count}')['name'] }}" + "}",
            "invocation_config": {"included_tags":["scd2_backfill_test"]}
        },
    )

    return workflow_invocation

with models.DAG(
    "scd2_backfill_test",
    schedule_interval=None,
    start_date=datetime(2024, 1, 17),
    catchup=False,
) as dag:
    days_arr = get_days_array(date(2024,1,1),date(2024,1,16),1)
    concurrent_days = [days_arr[i * concurrency:(i + 1) * concurrency] for i in range((len(days_arr) + concurrency - 1) // concurrency )] 

    for array in  concurrent_days:
       print('"' + str(array) + '"')

    task_arr=[]
    count = 0

    for array in concurrent_days:

        globals()["create_compilation_" + str(count)] = create_compilation_result(count,str(concurrent_days[count]))
        globals()["invoke_workflow_" + str(count)] = create_workflow_invocation(count)

        task_arr.append(globals()["create_compilation_" + str(count)])
        task_arr.append(globals()["invoke_workflow_" + str(count)])

        count = count + 1
    
chain(*task_arr)