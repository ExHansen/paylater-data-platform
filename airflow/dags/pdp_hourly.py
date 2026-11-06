"""Hourly warehouse build: freshness gate, dbt build (models + tests), test results to ops."""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from pdp.tasks import record_task_failure

DBT = (
    "/opt/pdp/dbt-venv/bin/dbt build --project-dir /opt/pdp/dbt --profiles-dir /opt/pdp/dbt "
    "--target-path /tmp/pdp-dbt/target --log-path /tmp/pdp-dbt/logs"
)
DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": record_task_failure,
}


@dag(
    dag_id="pdp_hourly",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["pdp", "dbt"],
)
def pdp_hourly():
    @task
    def freshness_gate() -> dict:
        from pdp.tasks import quality_frequent

        return quality_frequent()

    dbt_build = BashOperator(task_id="dbt_build", bash_command=DBT)

    @task(trigger_rule="all_done")
    def dbt_tests_to_ops() -> int:
        from pdp.tasks import dbt_results_to_ops

        return dbt_results_to_ops("/tmp/pdp-dbt/target/run_results.json")

    freshness_gate() >> dbt_build >> dbt_tests_to_ops()


pdp_hourly()
