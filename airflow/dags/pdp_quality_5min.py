"""Cheap data-quality checks every 5 minutes: freshness, duplicates, order, schema, values."""

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
    dag_id="pdp_quality_5min",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["pdp", "data-quality"],
)
def pdp_quality_5min():
    @task
    def frequent_checks() -> dict:
        from pdp.tasks import quality_frequent

        return quality_frequent()

    frequent_checks()


pdp_quality_5min()
