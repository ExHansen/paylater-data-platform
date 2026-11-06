"""Daily data-quality checks: source vs warehouse reconciliation and volume anomalies."""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from pdp.tasks import record_task_failure

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": record_task_failure,
}


@dag(
    dag_id="pdp_daily_dq",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["pdp", "data-quality"],
)
def pdp_daily_dq():
    @task
    def daily_checks() -> dict:
        from pdp.tasks import quality_daily

        return quality_daily()

    daily_checks()


pdp_daily_dq()
