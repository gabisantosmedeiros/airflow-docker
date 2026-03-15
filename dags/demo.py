from datetime import datetime
import random

from airflow.sdk import DAG  # novo namespace no Airflow 3.x :contentReference[oaicite:2]{index=2}
from airflow.providers.standard.operators.bash import BashOperator  # caminho em 3.1 :contentReference[oaicite:3]{index=3}
from airflow.providers.standard.operators.python import BranchPythonOperator  # branch operator no provider padrão :contentReference[oaicite:4]{index=4}
from airflow.utils.trigger_rule import TriggerRule  # regras de disparo (all_done, one_failed, etc.) :contentReference[oaicite:5]{index=5}

def escolhe_ramo() -> str:
    return random.choice(["branch_a_start", "branch_b_start"])


with DAG(
    dag_id="Demo",
    description="DAG Demo",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # execução manual na UI
    catchup=False,
    tags=["demo", "ui", "complex"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Iniciando pipeline' && sleep 1"
    )

    prep_fast = BashOperator(
        task_id="prep_fast",
        bash_command="echo 'prep_fast rodando rápido' && sleep 1"
    )

    prep_slow = BashOperator(
        task_id="prep_slow",
        bash_command="echo 'prep_slow rodando (também 1s mas finja que é pesado)' && sleep 1"
    )
    start >> [prep_fast, prep_slow]
    decide_branch = BranchPythonOperator(
        task_id="decide_branch",
        python_callable=escolhe_ramo,
    )

    [prep_fast, prep_slow] >> decide_branch

    branch_a_start = BashOperator(
        task_id="branch_a_start",
        bash_command="echo '[A] começo do ramo A' && sleep 1",
    )

    branch_a_do_work = BashOperator(
        task_id="branch_a_do_work",
        bash_command="echo '[A] fazendo trabalho pesado A' && sleep 1",
    )

    branch_a_fail = BashOperator(
        task_id="branch_a_fail",
        bash_command="echo '[A] vou falhar de propósito' && sleep 1 && exit 1",
        retries=0,  # sem retry; falha direto
    )

    branch_a_start >> branch_a_do_work >> branch_a_fail

    branch_b_start = BashOperator(
        task_id="branch_b_start",
        bash_command="echo '[B] começo do ramo B' && sleep 1",
    )

    branch_b_do_work_1 = BashOperator(
        task_id="branch_b_do_work_1",
        bash_command="echo '[B] passo 1 ok' && sleep 1",
    )

    branch_b_do_work_2 = BashOperator(
        task_id="branch_b_do_work_2",
        bash_command="echo '[B] passo 2 ok, tudo certinho' && sleep 1",
    )

    branch_b_start >> branch_b_do_work_1 >> branch_b_do_work_2

    decide_branch >> [branch_a_start, branch_b_start]

    notify_any_failure = BashOperator(
        task_id="notify_any_failure",
        bash_command="echo '⚠️ Pelo menos um ramo falhou!' && sleep 1",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    [branch_a_fail, branch_b_do_work_2] >> notify_any_failure

    join_all_done = BashOperator(
        task_id="join_all_done",
        bash_command=(
            "echo '🧹 Juntando resultados / limpeza final parcial' && "
            "echo 'Mesmo que um ramo tenha falhado ou sido SKIPPED' && "
            "sleep 1"
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )
    [branch_a_fail, branch_b_do_work_2] >> join_all_done

    final_report = BashOperator(
        task_id="final_report",
        bash_command=(
            "echo '📊 Relatório final: pelo menos um caminho funcionou!' && "
            "echo 'Gerando resumo final...' && sleep 1"
        ),
        trigger_rule=TriggerRule.ALL_DONE_SETUP_SUCCESS,
    )
    join_all_done >> final_report

    the_end = BashOperator(
        task_id="the_end",
        bash_command="echo '✅ Pipeline finalizado (UI feliz)' && sleep 1",
    )

    final_report >> the_end
