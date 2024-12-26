from snowflake.core import Root 
import snowflake.connector
from datetime import timedelta
from snowflake.core.task import Task , StoredProcedureCall
import procedures

conn = snowflake.connector.connect()
print("connection established")
print(conn)

root = Root(conn)
print(root)

my_task = Task("my_task",StoredProcedureCall(procedures.hello_procedure,\
    stage_location="@dev_deployment"),warehouse="compute_wh",schedule=timedelta(hours=1))

tasks = root.databases["KOIOS_QA"].schemas['PUBLIC'].tasks
tasks.create(my_task)