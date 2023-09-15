import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.generic_transfer import GenericTransfer


# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

with DAG(
    dag_id="artist",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="0 0 7 * * *",
    catchup=False,
) as dag:
    artist = PostgresOperator(
        task_id="task_1",
        sql="""
            SELECT * FROM "Album";
          """,
        postgres_conn_id = "Postgres",
    )
    artist