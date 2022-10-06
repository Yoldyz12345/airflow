# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import requests
from airflow.decorators import task, dag
from airflow import DAG
from datetime import datetime
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow import settings
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import logging
from airflow.utils.log.logging_mixin import LoggingMixin


default_args = {
    'table_name': 'ProfilerResultLatest',
    'user_id': 'marina34',
    'task_id': '1',
    'file_path': settings.DAGS_FOLDER+"/persons_v2.csv"
}

log = logging.getLogger('airflow.task')

@dag(
    schedule_interval=None,
    start_date=datetime(2022, 9, 28),
    tags=['marina', 'db'],
    default_args=default_args
)
def my_dag_db():
    # on Postgres

    create_table = PostgresOperator(
        task_id = "create_db",
        sql=r"""create table if not exists {} (
            _id int primary key,
            user_id text,
            task_id text,
            attr_name text,
            profiler_res text,
            version int
            );""".format(default_args['table_name']),
    )

    # create_table = SqliteOperator(
    #     task_id = "create_db",
    #     sql=r"""create table if not exists {} (
    #         _id int primary key,
    #         user_id text,
    #         task_id text,
    #         attr_name text,
    #         profiler_res text,
    #         version int
    #         );""".format(default_args['table_name']),
    # )


    @task(task_id="profile")
    def profile_operator():
        files = {
            'input_csv': open(default_args['file_path'])
        }
        #profiler_resp = requests.post('http://127.0.0.1:9093/profile', files=files)

        #LoggingMixin().log.info(profiler_resp)

        #if profiler_resp.status_code == 200:
        #    result = profiler_resp.json()
        result = { 'Profile':
                       {
            "user_id": ["'marina34'"],
            "task_id": "1",
            "attr_name": ["'Фамилия'"],
            "profiler_res": ["'last_name'"],
            "version": "1"}
        }
        for key, values in result['Profile'].items(): # indent right

                for value in values:
                    query = r"""insert into {} (user_id, task_id, 
                    attr_name, profiler_res, version)
                                    values (?, ?, ?, ?, ?);
                                    """.format(default_args['table_name'])
                    #sl_hook = SqliteHook(sqlite_conn_id='sqlite_default')
                    sl_hook = PostgresHook(postgres_conn_id='postgres_localhost')
                    sl_hook.run(query, parameters=(default_args['user_id'],
                                                   default_args['task_id'], value, key, 1))




    @task(task_id="get_json")
    def get_json_operator():


        query = r"""select * from {}""".format(default_args['table_name'])
        sl_hook = PostgresHook(postgres_conn_id='postgres_localhost')
        result = sl_hook.run(query)
        LoggingMixin().log.info(result)


    profile = profile_operator()
    json = get_json_operator()
    LoggingMixin().log.info(json)
    create_table >> profile >> json;
    create_table >> json;



my_dag_db = my_dag_db()


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
