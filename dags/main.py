from time import sleep
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="pipeline",
    description="minha etl",
    schedule="2 2 2 2 2",
    start_date=datetime(2025,11,2),
    catchup=False
)
def pipeline():
    @task
    def primeira_atividade():
        print("primeira atividade rodou com sucesso")
        sleep(2)

    @task
    def segunda_atividade():
        print("segunda atividade rodou com sucesso")
        sleep(2)

    @task
    def terceira_atividade():
        print("terceira atividade rodou com sucesso")
        sleep(2)

if __name__ == "__main__":
    pipeline()
