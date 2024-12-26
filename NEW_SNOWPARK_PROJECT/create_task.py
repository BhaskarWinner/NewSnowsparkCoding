import sys
import os
from datetime import timedelta
from snowflake.core import Root
import snowflake.connector
from snowflake.core.task import Task, StoredProcedureCall
from snowflake.snowpark import Session

# Add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import procedures module
from app import procedures
from snowflake.core.task.dagv1 import DAG , DAGTask , DAGOperation , CreateMode , DAGTaskBranch

#conn = snowflake.connector.connect()
conn = snowflake.connector.connect(
    user=os.environ.get('SNOWFLAKE_USER'),
    password=os.environ.get('SNOWFLAKE_PASSWORD'),
    account=os.environ.get("SNOWFLAKE_ACCOUNT"),
    warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
    database=os.environ.get('SNOWFLAKE_DATABASE'),
    schema=os.environ.get('SNOWFLAKE_SCHEMA'),
    role=os.environ.get('SNOWFLAKE_ROLE'))


#print("connection established")
#print(conn)

root = Root(conn)
print(root)
# crete definiton for the task 

my_task = Task("my_task",StoredProcedureCall(procedures.hello_procedure,\
    stage_location="@dev_deployment"),warehouse="compute_wh",schedule=timedelta(hours=1))

tasks = root.databases["KOIOS_DEV"].schemas['KOIOS_RAW'].tasks
#tasks.create(my_task)

# create dag  
with DAG("my_dag",schedule=timedelta(days=1)) as dag:
    dag_task_1 = DAGTask("my_hello_task",StoredProcedureCall(procedures.hello_procedure,\
    stage_location="@dev_deployment"),warehouse="compute_wh")

    dag_task_2 = DAGTask("my_test_task",StoredProcedureCall(procedures.test_procedure,\
    stage_location="@dev_deployment"),warehouse="compute_wh")

    dag_task_3 =  DAGTask("my_test_task3",StoredProcedureCall(procedures.test_procedure_two,\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
    dag_task_4 =  DAGTask("my_test_task4",StoredProcedureCall(procedures.test_procedure,\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
    dag_task_1 >> dag_task_2 >> [dag_task_3,dag_task_4]
    schema = root.databases["KOIOS_DEV"].schemas["KOIOS_RAW"]
    dag_op = DAGOperation(schema)
    dag_op.deploy(dag,CreateMode.or_replace)

# create dag task branch
def task_brach_condition(session: Session) -> str:
  # write conditon
  return "my_test_task3"

with DAG("my_dag_task_branch",schedule=timedelta(days=1)) as dag:
    dag_task_1 = DAGTask("my_hello_task",StoredProcedureCall(procedures.hello_procedure,\
    stage_location="@dev_deployment"),warehouse="compute_wh")

    dag_task_2 = DAGTask("my_test_task",StoredProcedureCall(procedures.test_procedure,\
    stage_location="@dev_deployment"),warehouse="compute_wh")

    dag_task_3 =  DAGTask("my_test_task3",StoredProcedureCall(procedures.test_procedure_two,\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
    dag_task_4 =  DAGTask("my_test_task4",StoredProcedureCall(procedures.test_procedure,\
    stage_location="@dev_deployment"),warehouse="compute_wh")
  
    dag_task_branch = DAGTaskBranch("task_branch",task_brach_condition,warehouse="compute_wh")
  
    dag_task_1 >> dag_task_2 >> dag_task_branch
  
    dag_task_branch >> [dag_task_3,dag_task_4]
  
    schema = root.databases["KOIOS_DEV"].schemas["KOIOS_RAW"]
    dag_op = DAGOperation(schema)
    dag_op.deploy(dag,CreateMode.or_replace)