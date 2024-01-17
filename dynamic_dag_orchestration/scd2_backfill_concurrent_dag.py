from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import date, timedelta, datetime
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

src_database="latika-experiments"
src_schema="dataform"
src_table="covid_staging"
target_database="latika-experiments"
target_schema="dataform_scd_backfill"
target_table="covid_final"
target_hash_unique_col_name="hash_unique"
target_hash_non_unique_col_name="hash_non_unique"
timestampfield="date"
start_from_column_name="eff_date"
end_at_column_name="exp_date"

start_date = date(2024,1,1)
end_date = date(2024,1,16)
concurrency = 3

def get_days_array(start, end, no_of_days):
  current_date = start
  result = []
  while current_date < end:
    end_date = min(current_date + timedelta(days=no_of_days), end)  # Calculate end of range
    result.append(f"{current_date.isoformat()[:10]}:{end_date.isoformat()[:10]}")
    current_date += timedelta(days=no_of_days)  # Move to next start date
  return result

with DAG(dag_id="scd2_backfill_test", start_date=datetime(2024, 1, 16)):
    days_arr = get_days_array(date(2024,1,1),date(2024,1,16),1)
    concurrent_days = [days_arr[i * concurrency:(i + 1) * concurrency] for i in range((len(days_arr) + concurrency - 1) // concurrency )] 

    for array in  concurrent_days:
       print('"' + str(array) + '"')
    
    def create_compilation_result(count,daylist,src_database,src_schema,src_table,target_database,target_schema,target_table,target_hash_unique_col_name,target_hash_non_unique_col_name,timestampfield,start_from_column_name,end_at_column_name):
        compilation_result = DataformCreateCompilationResultOperator(
            task_id=f"create_compilation_result_{count}",
            retries=0,
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            compilation_result={
                "code_compilation_config": {
                    "vars": {
                        "daylist": daylist,
                        "src_database": src_database,
                        "src_schema":src_schema,   
                        "src_table":src_table, 
                        "target_database":target_database,
                        "target_schema":target_schema,
                        "target_table":target_table, 
                        "target_hash_unique_col_name":target_hash_unique_col_name,
                        "target_hash_non_unique_col_name":target_hash_non_unique_col_name,  
                        "timestampfield": timestampfield,
                        "start_from_column_name": start_from_column_name,  
                        "end_at_column_name": end_at_column_name
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
                "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
                "invocation_config": {"included_tags":["scd2_backfill"]}
            },
        )

        return workflow_invocation

task_arr=[]
task_arr.append(create_compilation_result(0,'"' + str(concurrent_days[0]) + '"',src_database,src_schema,src_table,target_database,target_schema,target_table,target_hash_unique_col_name,target_hash_non_unique_col_name,timestampfield,start_from_column_name,end_at_column_name))

count = 0

for array in concurrent_days[1:]:
    task1 = create_workflow_invocation(count)
    task_arr[-1]>>task1
    task_arr.append(task1)

    count = count + 1

    daylist = '"' + str(array) + '"'
    task2 = create_compilation_result(count,daylist,src_database,src_schema,src_table,target_database,target_schema,target_table,target_hash_unique_col_name,target_hash_non_unique_col_name,timestampfield,start_from_column_name,end_at_column_name)
    task_arr[-1]>>task2
    task_arr.append(task2)

task_final = create_workflow_invocation(count)
task_arr[-1]>>task_final